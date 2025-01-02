package modusdb

import (
	"context"

	"github.com/dgraph-io/dgo/v240/protos/api"
)

// DB is one of the namespaces in modusDB.
type DB struct {
	id uint64
	d  *Driver
}

func (db *DB) ID() uint64 {
	return db.id
}

// DropData drops all the data in the current namespace of modusDB instance.
func (db *DB) DropData(ctx context.Context) error {
	return db.d.dropData(ctx, db)
}

// AlterSchema alters the schema of the current namespace of modusDB instance.
func (db *DB) AlterSchema(ctx context.Context, sch string) error {
	return db.d.alterSchema(ctx, db, sch)
}

// Mutate performs mutation on the given namespace of modusDB instance.
func (db *DB) Mutate(ctx context.Context, ms []*api.Mutation) (map[string]uint64, error) {
	return db.d.mutate(ctx, db, ms)
}

// Query performs query or mutation or upsert on the given modusDB instance.
func (db *DB) Query(ctx context.Context, query string) (*api.Response, error) {
	return db.d.query(ctx, db, query)
}
