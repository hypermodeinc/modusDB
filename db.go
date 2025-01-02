package modusdb

import (
	"context"
	"errors"
	"fmt"
	"path"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/dgraph-io/badger/v4"
	"github.com/dgraph-io/dgo/v240/protos/api"
	"github.com/dgraph-io/dgraph/v24/dql"
	"github.com/dgraph-io/dgraph/v24/edgraph"
	"github.com/dgraph-io/dgraph/v24/posting"
	"github.com/dgraph-io/dgraph/v24/protos/pb"
	"github.com/dgraph-io/dgraph/v24/query"
	"github.com/dgraph-io/dgraph/v24/schema"
	"github.com/dgraph-io/dgraph/v24/worker"
	"github.com/dgraph-io/dgraph/v24/x"
	"github.com/dgraph-io/ristretto/v2/z"
)

var (
	// This ensures that we only have one driver for modusDB in this process.
	singleton atomic.Bool

	ErrSingletonOnly        = errors.New("only one modusDB driver is supported")
	ErrEmptyDataDir         = errors.New("data directory is required")
	ErrClosedDB             = errors.New("modusDB driver is closed")
	ErrNonExistentNamespace = errors.New("namespace does not exist")
)

// Driver is the modusDB driver. For now, we only support one modusDB per process.
type Driver struct {
	mutex  sync.RWMutex
	isOpen atomic.Bool

	z   *zero
	db0 *DB
}

// NewDriver returns a driver for modusDB.
func NewDriver(conf Config) (*Driver, error) {
	// Ensure that we do not create another driver for modusDB in the same process
	if !singleton.CompareAndSwap(false, true) {
		return nil, ErrSingletonOnly
	}

	if err := conf.validate(); err != nil {
		return nil, err
	}

	// setup data directories
	worker.Config.PostingDir = path.Join(conf.dataDir, "p")
	worker.Config.WALDir = path.Join(conf.dataDir, "w")
	x.WorkerConfig.TmpDir = path.Join(conf.dataDir, "t")

	// TODO: optimize these and more options
	x.WorkerConfig.Badger = badger.DefaultOptions("").FromSuperFlag(worker.BadgerDefaults)
	x.Config.MaxRetries = 10
	x.Config.Limit = z.NewSuperFlag("max-pending-queries=100000")
	x.Config.LimitNormalizeNode = conf.limitNormalizeNode

	// initialize each package
	edgraph.Init()
	worker.State.InitStorage()
	worker.InitForLite(worker.State.Pstore)
	schema.Init(worker.State.Pstore)
	posting.Init(worker.State.Pstore, 0) // TODO: set cache size

	modusDriver := &Driver{}
	modusDriver.isOpen.Store(true)
	modusDriver.db0 = &DB{id: 0, d: modusDriver}
	if err := modusDriver.reset(); err != nil {
		return nil, fmt.Errorf("error resetting db: %w", err)
	}

	x.UpdateHealthStatus(true)
	return modusDriver, nil
}

func (d *Driver) LeaseUIDs(numUIDs uint64) (*pb.AssignedIds, error) {
	if !d.isOpen.Load() {
		return nil, ErrClosedDB
	}

	num := &pb.Num{Val: numUIDs, Type: pb.Num_UID}
	return d.z.nextUIDs(num)
}

func (d *Driver) CreateNamespace() (*DB, error) {
	if !d.isOpen.Load() {
		return nil, ErrClosedDB
	}

	startTs, err := d.z.nextTS()
	if err != nil {
		return nil, err
	}
	nsID, err := d.z.nextNS()
	if err != nil {
		return nil, err
	}

	d.mutex.Lock()
	defer d.mutex.Unlock()

	if err := worker.ApplyInitialSchema(nsID, startTs); err != nil {
		return nil, fmt.Errorf("error applying initial schema: %w", err)
	}
	for _, pred := range schema.State().Predicates() {
		worker.InitTablet(pred)
	}

	return &DB{id: nsID, d: d}, nil
}

func (d *Driver) GetNamespace(nsID uint64) (*DB, error) {
	if !d.isOpen.Load() {
		return nil, ErrClosedDB
	}

	d.mutex.RLock()
	defer d.mutex.RUnlock()

	if nsID == 0 {
		return d.db0, nil
	}

	if nsID > d.z.lastNS {
		return nil, ErrNonExistentNamespace
	}

	// TODO: when delete namespace is implemented, check if the namespace exists

	return &DB{id: nsID, d: d}, nil
}

// GetDefaultDB returns the default namespace.
func (d *Driver) GetDefaultDB() *DB {
	return d.db0
}

// DropAll drops all the data and schema in modusDB.
func (d *Driver) DropAll(ctx context.Context) error {
	if !d.isOpen.Load() {
		return ErrClosedDB
	}

	d.mutex.Lock()
	defer d.mutex.Unlock()

	p := &pb.Proposal{Mutations: &pb.Mutations{
		GroupId: 1,
		DropOp:  pb.Mutations_ALL,
	}}
	if err := worker.ApplyMutations(ctx, p); err != nil {
		return fmt.Errorf("error applying mutation: %w", err)
	}
	if err := d.reset(); err != nil {
		return fmt.Errorf("error resetting db: %w", err)
	}

	// TODO: insert drop record
	return nil
}

// Close closes the modusDB driver.
func (d *Driver) Close() {
	if !d.isOpen.Load() {
		return
	}

	d.mutex.Lock()
	defer d.mutex.Unlock()

	if !singleton.CompareAndSwap(true, false) {
		panic("modusDB driver was not properly opened")
	}

	d.isOpen.Store(false)
	x.UpdateHealthStatus(false)
	posting.Cleanup()
	worker.State.Dispose()
}

func (d *Driver) dropData(ctx context.Context, db *DB) error {
	if !d.isOpen.Load() {
		return ErrClosedDB
	}

	p := &pb.Proposal{Mutations: &pb.Mutations{
		GroupId:   1,
		DropOp:    pb.Mutations_DATA,
		DropValue: strconv.FormatUint(db.ID(), 10),
	}}

	if err := worker.ApplyMutations(ctx, p); err != nil {
		return fmt.Errorf("error applying mutation: %w", err)
	}

	// TODO: insert drop record
	// TODO: should we reset back the timestamp as well?
	return nil
}

func (d *Driver) alterSchema(ctx context.Context, db *DB, sch string) error {
	if !d.isOpen.Load() {
		return ErrClosedDB
	}

	ps, err := schema.ParseWithNamespace(sch, db.ID())
	if err != nil {
		return fmt.Errorf("error parsing schema: %w", err)
	}

	startTs, err := d.z.nextTS()
	if err != nil {
		return err
	}

	d.mutex.Lock()
	defer d.mutex.Unlock()

	for _, pred := range ps.Preds {
		worker.InitTablet(pred.Predicate)
	}

	p := &pb.Proposal{Mutations: &pb.Mutations{
		GroupId: 1,
		StartTs: startTs,
		Schema:  ps.Preds,
		Types:   ps.Types,
	}}
	if err := worker.ApplyMutations(ctx, p); err != nil {
		return fmt.Errorf("error applying mutation: %w", err)
	}
	return nil
}

func (d *Driver) query(ctx context.Context, db *DB, q string) (*api.Response, error) {
	if !d.isOpen.Load() {
		return nil, ErrClosedDB
	}

	d.mutex.RLock()
	defer d.mutex.RUnlock()

	ctx = x.AttachNamespace(ctx, db.ID())
	return (&edgraph.Server{}).QueryNoAuth(ctx, &api.Request{
		ReadOnly: true,
		Query:    q,
		StartTs:  d.z.readTs(),
	})
}

func (d *Driver) mutate(ctx context.Context, db *DB, ms []*api.Mutation) (
	map[string]uint64, error) {

	if !d.isOpen.Load() {
		return nil, ErrClosedDB
	}

	if len(ms) == 0 {
		return nil, nil
	}

	dms := make([]*dql.Mutation, 0, len(ms))
	for _, mu := range ms {
		dm, err := edgraph.ParseMutationObject(mu, false)
		if err != nil {
			return nil, fmt.Errorf("error parsing mutation: %w", err)
		}
		dms = append(dms, dm)
	}
	newUIDs, err := query.ExtractBlankUIDs(ctx, dms)
	if err != nil {
		return nil, err
	}
	if len(newUIDs) > 0 {
		num := &pb.Num{Val: uint64(len(newUIDs)), Type: pb.Num_UID}
		res, err := d.z.nextUIDs(num)
		if err != nil {
			return nil, err
		}

		curId := res.StartId
		for k := range newUIDs {
			x.AssertTruef(curId != 0 && curId <= res.EndId, "not enough uids generated")
			newUIDs[k] = curId
			curId++
		}
	}

	edges, err := query.ToDirectedEdges(dms, newUIDs)
	if err != nil {
		return nil, err
	}
	ctx = x.AttachNamespace(ctx, db.ID())

	startTs, err := d.z.nextTS()
	if err != nil {
		return nil, err
	}
	commitTs, err := d.z.nextTS()
	if err != nil {
		return nil, err
	}

	d.mutex.Lock()
	defer d.mutex.Unlock()

	m := &pb.Mutations{
		GroupId: 1,
		StartTs: startTs,
		Edges:   edges,
	}
	m.Edges, err = query.ExpandEdges(ctx, m)
	if err != nil {
		return nil, fmt.Errorf("error expanding edges: %w", err)
	}

	for _, edge := range m.Edges {
		worker.InitTablet(edge.Attr)
	}

	p := &pb.Proposal{Mutations: m, StartTs: startTs}
	if err := worker.ApplyMutations(ctx, p); err != nil {
		return nil, err
	}

	return newUIDs, worker.ApplyCommited(ctx, &pb.OracleDelta{
		Txns: []*pb.TxnStatus{{StartTs: startTs, CommitTs: commitTs}},
	})
}

func (d *Driver) reset() error {
	z, restart, err := newZero()
	if err != nil {
		return fmt.Errorf("error initializing zero: %w", err)
	}

	if !restart {
		if err := worker.ApplyInitialSchema(0, 1); err != nil {
			return fmt.Errorf("error applying initial schema: %w", err)
		}
	}

	if err := schema.LoadFromDb(context.Background()); err != nil {
		return fmt.Errorf("error loading schema: %w", err)
	}
	for _, pred := range schema.State().Predicates() {
		worker.InitTablet(pred)
	}

	d.z = z
	return nil
}
