package modusdb_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hypermodeinc/modusdb"
)

type User struct {
	Gid     uint64 `json:"gid,omitempty"`
	Name    string `json:"name,omitempty"`
	Age     int    `json:"age,omitempty"`
	ClerkId string `json:"clerk_id,omitempty" db:"constraint=unique"`
}

func TestCreateApi(t *testing.T) {
	ctx := context.Background()
	db, err := modusdb.New(modusdb.NewDefaultConfig(t.TempDir()))
	require.NoError(t, err)
	defer db.Close()

	db1, err := db.CreateNamespace()
	require.NoError(t, err)

	require.NoError(t, db1.DropData(ctx))

	user := &User{
		Name: "B", 
		Age:  20,
		ClerkId: "123",
	}

	

	gid, _, err := modusdb.Create(db, user, modusdb.WithNamespace(db1.ID()))
	require.NoError(t, err)

	require.Equal(t, "B", user.Name)
	require.Equal(t, uint64(2), gid)
	require.Equal(t, uint64(2), user.Gid)

	query := `{
		me(func: has(User.name)) {
			uid
			User.name
			User.age
			User.clerk_id
		}
	}`
	resp, err := db1.Query(ctx, query)
	require.NoError(t, err)
	require.JSONEq(t, `{"me":[{"uid":"0x2","User.name":"B","User.age":20,"User.clerk_id":"123"}]}`,
		string(resp.GetJson()))

	// TODO schema{} should work
	schemaQuery := `schema(pred: [User.name, User.age, User.clerk_id]) 
	{
		type
		index
		tokenizer
	}`
	resp, err = db1.Query(ctx, schemaQuery)
	require.NoError(t, err)

	actualJSON := string(resp.GetJson())
	fmt.Println("Actual JSON:", actualJSON)

	require.JSONEq(t,
		`{"schema":
			[
				{"predicate":"User.age","type":"int"},
				{"predicate":"User.clerk_id","type":"string","index":true,"tokenizer":["exact"]},
				{"predicate":"User.name","type":"string"}
			]
		}`,
		string(resp.GetJson()))
}

func TestCreateApiWithNonStruct(t *testing.T) {
	ctx := context.Background()
	db, err := modusdb.New(modusdb.NewDefaultConfig(t.TempDir()))
	require.NoError(t, err)
	defer db.Close()

	db1, err := db.CreateNamespace()
	require.NoError(t, err)

	require.NoError(t, db1.DropData(ctx))

	user := &User{
		Name: "B",
		Age:  20,
	}

	_, _, err = modusdb.Create[*User](db, &user, modusdb.WithNamespace(db1.ID()))
	require.Error(t, err)
	require.Equal(t, "expected struct, got ptr", err.Error())
}

func TestGetApi(t *testing.T) {
	ctx := context.Background()
	db, err := modusdb.New(modusdb.NewDefaultConfig(t.TempDir()))
	require.NoError(t, err)
	defer db.Close()

	db1, err := db.CreateNamespace()
	require.NoError(t, err)

	require.NoError(t, db1.DropData(ctx))

	user := &User{
		Name: "B",
		Age:  20,
	}

	_, _, err = modusdb.Create(db, user, modusdb.WithNamespace(db1.ID()))
	require.NoError(t, err)

	userQuery, err := modusdb.Get[User](db, uint64(2), modusdb.WithNamespace(db1.ID()))

	require.NoError(t, err)
	require.Equal(t, uint64(2), userQuery.Gid)
	require.Equal(t, 20, userQuery.Age)
	require.Equal(t, "B", userQuery.Name)
}
