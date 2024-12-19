package modusdb

import (
	"fmt"
	"sync"

	"github.com/dgraph-io/badger/v4"
	"github.com/dgraph-io/dgraph/v24/posting"
	"github.com/dgraph-io/dgraph/v24/protos/pb"
	"github.com/dgraph-io/dgraph/v24/worker"
	"github.com/dgraph-io/dgraph/v24/x"
	"google.golang.org/protobuf/proto"
)

const (
	// We store zero data at UID 1 and assign the UIDs
	// starting from 2 to the data added by the user.
	zeroStateUID = 1
	initialUID   = 2

	// We store zero data at timestamp 2 and assign the timestamps
	// starting from 3 to the data added by the user.
	zeroStateTs = 2
	initialTs   = 3

	// We lease UIDs and timestamps in batches of 10000.
	leaseUIDAtATime = 10000
	leaseTsAtATime  = 10000

	// zero data is stored at the following predicate
	zeroStateKey = "0-dgraph.modusdb.zero"
)

// zero takes care of all the responsibilities of zero node in Dgraph.
type zero struct {
	lock sync.Mutex

	minLeasedUID uint64
	maxLeasedUID uint64
	minLeasedTs  uint64
	maxLeasedTs  uint64
	lastNS       uint64
}

func newZero() (*zero, bool, error) {
	zs, err := readZeroState()
	if err != nil {
		return nil, false, err
	}
	restart := zs != nil

	z := &zero{}
	if zs == nil {
		z.minLeasedUID = initialUID
		z.maxLeasedUID = initialUID
		z.minLeasedTs = initialTs
		z.maxLeasedTs = initialTs
		z.lastNS = 0
	} else {
		z.minLeasedUID = zs.MaxUID
		z.maxLeasedUID = zs.MaxUID
		z.minLeasedTs = zs.MaxTxnTs
		z.maxLeasedTs = zs.MaxTxnTs
		z.lastNS = zs.MaxNsID
	}
	posting.Oracle().ProcessDelta(&pb.OracleDelta{MaxAssigned: z.minLeasedTs - 1})
	worker.SetMaxUID(z.minLeasedUID - 1)

	if err := z.leaseUIDs(); err != nil {
		return nil, false, err
	}
	if err := z.leaseTs(); err != nil {
		return nil, false, err
	}

	return z, restart, nil
}

func (z *zero) nextTS() (uint64, error) {
	z.lock.Lock()
	defer z.lock.Unlock()

	if z.minLeasedTs >= z.maxLeasedTs {
		if err := z.leaseTs(); err != nil {
			return 0, fmt.Errorf("error leasing timestamps: %w", err)
		}
	}

	ts := z.minLeasedTs
	z.minLeasedTs += 1
	posting.Oracle().ProcessDelta(&pb.OracleDelta{MaxAssigned: ts})
	return ts, nil
}

func (z *zero) readTs() uint64 {
	z.lock.Lock()
	defer z.lock.Unlock()

	return z.minLeasedTs - 1
}

func (z *zero) nextUID() (uint64, error) {
	uids, err := z.nextUIDs(&pb.Num{Val: 1, Type: pb.Num_UID})
	if err != nil {
		return 0, err
	}
	return uids.StartId, nil
}

func (z *zero) nextUIDs(num *pb.Num) (*pb.AssignedIds, error) {
	z.lock.Lock()
	defer z.lock.Unlock()

	var resp *pb.AssignedIds
	if num.Bump {
		if z.minLeasedUID >= num.Val {
			resp = &pb.AssignedIds{StartId: z.minLeasedUID, EndId: z.minLeasedUID}
			z.minLeasedUID += 1
		} else {
			resp = &pb.AssignedIds{StartId: z.minLeasedUID, EndId: num.Val}
			z.minLeasedUID = num.Val + 1
		}
	} else {
		resp = &pb.AssignedIds{StartId: z.minLeasedUID, EndId: z.minLeasedUID + num.Val - 1}
		z.minLeasedUID += num.Val
	}

	for z.minLeasedUID >= z.maxLeasedUID {
		if err := z.leaseUIDs(); err != nil {
			return nil, err
		}
	}

	worker.SetMaxUID(z.minLeasedUID - 1)
	return resp, nil
}

func (z *zero) nextNS() (uint64, error) {
	z.lock.Lock()
	defer z.lock.Unlock()

	z.lastNS++
	if err := z.writeZeroState(); err != nil {
		return 0, fmt.Errorf("error leasing namespace ID: %w", err)
	}
	return z.lastNS, nil
}

// leaseTS leases timestamps in batches of leaseTsAtATime.
func (z *zero) leaseTs() error {
	if z.minLeasedTs+leaseTsAtATime <= z.maxLeasedTs {
		return nil
	}

	z.maxLeasedTs += z.minLeasedTs + leaseTsAtATime
	if err := z.writeZeroState(); err != nil {
		return fmt.Errorf("error leasing UIDs: %w", err)
	}

	return nil
}

func (z *zero) leaseUIDs() error {
	if z.minLeasedUID+leaseUIDAtATime <= z.maxLeasedUID {
		return nil
	}

	z.maxLeasedUID += z.minLeasedUID + leaseUIDAtATime
	if err := z.writeZeroState(); err != nil {
		return fmt.Errorf("error leasing timestamps: %w", err)
	}

	return nil
}

func (z *zero) writeZeroState() error {
	zeroState := &pb.MembershipState{MaxUID: z.maxLeasedUID, MaxTxnTs: z.maxLeasedTs, MaxNsID: z.lastNS}
	data, err := proto.Marshal(zeroState)
	if err != nil {
		return fmt.Errorf("error marshalling zero state: %w", err)
	}

	txn := worker.State.Pstore.NewTransactionAt(zeroStateTs, true)
	defer txn.Discard()

	e := &badger.Entry{
		Key:      x.DataKey(zeroStateKey, zeroStateUID),
		Value:    data,
		UserMeta: posting.BitCompletePosting,
	}
	if err := txn.SetEntry(e); err != nil {
		return fmt.Errorf("error setting zero state: %w", err)
	}
	if err := txn.CommitAt(zeroStateTs, nil); err != nil {
		return fmt.Errorf("error committing zero state: %w", err)
	}

	return nil
}

func readZeroState() (*pb.MembershipState, error) {
	txn := worker.State.Pstore.NewTransactionAt(zeroStateTs, false)
	defer txn.Discard()

	item, err := txn.Get(x.DataKey(zeroStateKey, zeroStateUID))
	if err != nil {
		if err == badger.ErrKeyNotFound {
			return nil, nil
		}
		return nil, fmt.Errorf("error getting zero state: %v", err)
	}

	var zeroState pb.MembershipState
	err = item.Value(func(val []byte) error {
		return proto.Unmarshal(val, &zeroState)
	})
	if err != nil {
		return nil, fmt.Errorf("error un-marshalling zero state: %v", err)
	}

	return &zeroState, nil
}
