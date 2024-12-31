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

func TestFirstTimeUser(t *testing.T) {
	db, err := modusdb.New(modusdb.NewDefaultConfig(t.TempDir()))
	require.NoError(t, err)
	defer db.Close()

	gid, user, err := modusdb.Create(db, &User{
		Name:    "A",
		Age:     10,
		ClerkId: "123",
	})

	require.NoError(t, err)
	require.Equal(t, user.Gid, gid)
	require.Equal(t, "A", user.Name)
	require.Equal(t, 10, user.Age)
	require.Equal(t, "123", user.ClerkId)

	gid, queriedUser, err := modusdb.Get[User](db, gid)

	require.NoError(t, err)
	require.Equal(t, queriedUser.Gid, gid)
	require.Equal(t, 10, queriedUser.Age)
	require.Equal(t, "A", queriedUser.Name)
	require.Equal(t, "123", queriedUser.ClerkId)

	gid, queriedUser2, err := modusdb.Get[User](db, modusdb.ConstrainedField{
		Key:   "clerk_id",
		Value: "123",
	})

	require.NoError(t, err)
	require.Equal(t, queriedUser.Gid, gid)
	require.Equal(t, 10, queriedUser2.Age)
	require.Equal(t, "A", queriedUser2.Name)
	require.Equal(t, "123", queriedUser2.ClerkId)

	_, _, err = modusdb.Delete[User](db, gid)
	require.NoError(t, err)

	_, queriedUser3, err := modusdb.Get[User](db, gid)
	require.Error(t, err)
	require.Equal(t, "no object found", err.Error())
	require.Nil(t, queriedUser3)

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
		Name:    "B",
		Age:     20,
		ClerkId: "123",
	}

	gid, user, err := modusdb.Create(db, user, db1.ID())
	require.NoError(t, err)

	require.Equal(t, "B", user.Name)
	require.Equal(t, user.Gid, gid)

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

	_, _, err = modusdb.Create[*User](db, &user, db1.ID())
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
		Name:    "B",
		Age:     20,
		ClerkId: "123",
	}

	gid, _, err := modusdb.Create(db, user, db1.ID())
	require.NoError(t, err)

	gid, queriedUser, err := modusdb.Get[User](db, gid, db1.ID())

	require.NoError(t, err)
	require.Equal(t, queriedUser.Gid, gid)
	require.Equal(t, 20, queriedUser.Age)
	require.Equal(t, "B", queriedUser.Name)
	require.Equal(t, "123", queriedUser.ClerkId)
}

func TestGetApiWithConstrainedField(t *testing.T) {
	ctx := context.Background()
	db, err := modusdb.New(modusdb.NewDefaultConfig(t.TempDir()))
	require.NoError(t, err)
	defer db.Close()

	db1, err := db.CreateNamespace()
	require.NoError(t, err)

	require.NoError(t, db1.DropData(ctx))

	user := &User{
		Name:    "B",
		Age:     20,
		ClerkId: "123",
	}

	_, _, err = modusdb.Create(db, user, db1.ID())
	require.NoError(t, err)

	gid, queriedUser, err := modusdb.Get[User](db, modusdb.ConstrainedField{
		Key:   "clerk_id",
		Value: "123",
	}, db1.ID())

	require.NoError(t, err)
	require.Equal(t, queriedUser.Gid, gid)
	require.Equal(t, 20, queriedUser.Age)
	require.Equal(t, "B", queriedUser.Name)
	require.Equal(t, "123", queriedUser.ClerkId)
}

func TestDeleteApi(t *testing.T) {
	ctx := context.Background()
	db, err := modusdb.New(modusdb.NewDefaultConfig(t.TempDir()))
	require.NoError(t, err)
	defer db.Close()

	db1, err := db.CreateNamespace()
	require.NoError(t, err)

	require.NoError(t, db1.DropData(ctx))

	user := &User{
		Name:    "B",
		Age:     20,
		ClerkId: "123",
	}

	gid, _, err := modusdb.Create(db, user, db1.ID())
	require.NoError(t, err)

	_, _, err = modusdb.Delete[User](db, gid, db1.ID())
	require.NoError(t, err)

	_, queriedUser, err := modusdb.Get[User](db, gid, db1.ID())
	require.Error(t, err)
	require.Equal(t, "no object found", err.Error())
	require.Nil(t, queriedUser)

	_, queriedUser, err = modusdb.Get[User](db, modusdb.ConstrainedField{
		Key:   "clerk_id",
		Value: "123",
	}, db1.ID())
	require.Error(t, err)
	require.Equal(t, "no object found", err.Error())
	require.Nil(t, queriedUser)
}

func TestUpsertApi(t *testing.T) {
	ctx := context.Background()
	db, err := modusdb.New(modusdb.NewDefaultConfig(t.TempDir()))
	require.NoError(t, err)
	defer db.Close()

	db1, err := db.CreateNamespace()
	require.NoError(t, err)

	require.NoError(t, db1.DropData(ctx))

	user := &User{
		Name:    "B",
		Age:     20,
		ClerkId: "123",
	}

	gid, user, _, err := modusdb.Upsert(db, user, db1.ID())
	require.NoError(t, err)
	require.Equal(t, user.Gid, gid)

	user.Age = 21
	gid, _, _, err = modusdb.Upsert(db, user, db1.ID())
	require.NoError(t, err)
	require.Equal(t, user.Gid, gid)

	_, queriedUser, err := modusdb.Get[User](db, gid, db1.ID())
	require.NoError(t, err)
	require.Equal(t, user.Gid, queriedUser.Gid)
	require.Equal(t, 21, queriedUser.Age)
	require.Equal(t, "B", queriedUser.Name)
	require.Equal(t, "123", queriedUser.ClerkId)
}

type Project struct {
	Gid     uint64 `json:"gid,omitempty"`
	Name    string `json:"name,omitempty"`
	ClerkId string `json:"clerk_id,omitempty" db:"constraint=unique"`
	// Branches []Branch `json:"branches,omitempty" readFrom:"type=Branch,field=proj"`
}

type Branch struct {
	Gid     uint64  `json:"gid,omitempty"`
	Name    string  `json:"name,omitempty"`
	ClerkId string  `json:"clerk_id,omitempty" db:"constraint=unique"`
	Proj    Project `json:"proj,omitempty"`
}

func TestNestedObjectMutation(t *testing.T) {
	ctx := context.Background()
	db, err := modusdb.New(modusdb.NewDefaultConfig(t.TempDir()))
	require.NoError(t, err)
	defer db.Close()

	db1, err := db.CreateNamespace()
	require.NoError(t, err)

	require.NoError(t, db1.DropData(ctx))

	branch := &Branch{
		Name:    "B",
		ClerkId: "123",
		Proj: Project{
			Name:    "P",
			ClerkId: "456",
		},
	}

	gid, branch, err := modusdb.Create(db, branch, db1.ID())
	require.NoError(t, err)

	require.Equal(t, "B", branch.Name)
	require.Equal(t, branch.Gid, gid)
	require.NotEqual(t, uint64(0), branch.Proj.Gid)
	require.Equal(t, "P", branch.Proj.Name)

	query := `{
		me(func: has(Branch.name)) {
			uid
			Branch.name
			Branch.clerk_id
			Branch.proj {
				uid
				Project.name
				Project.clerk_id
			}
		}
	}`
	resp, err := db1.Query(ctx, query)
	require.NoError(t, err)
	require.JSONEq(t,
		`{"me":[{"uid":"0x2","Branch.name":"B","Branch.clerk_id":"123","Branch.proj": 
		{"uid":"0x3","Project.name":"P","Project.clerk_id":"456"}}]}`,
		string(resp.GetJson()))

	gid, queriedBranch, err := modusdb.Get[Branch](db, gid, db1.ID())
	require.NoError(t, err)
	require.Equal(t, queriedBranch.Gid, gid)
	require.Equal(t, "B", queriedBranch.Name)

}

func TestLinkingObjectsByConstrainedFields(t *testing.T) {
	ctx := context.Background()
	db, err := modusdb.New(modusdb.NewDefaultConfig(t.TempDir()))
	require.NoError(t, err)
	defer db.Close()

	db1, err := db.CreateNamespace()
	require.NoError(t, err)

	require.NoError(t, db1.DropData(ctx))

	projGid, project, err := modusdb.Create(db, &Project{
		Name:    "P",
		ClerkId: "456",
	}, db1.ID())
	require.NoError(t, err)

	require.Equal(t, "P", project.Name)
	require.Equal(t, project.Gid, projGid)

	branch := &Branch{
		Name:    "B",
		ClerkId: "123",
		Proj: Project{
			Name:    "P",
			ClerkId: "456",
		},
	}

	gid, branch, err := modusdb.Create(db, branch, db1.ID())
	require.NoError(t, err)

	require.Equal(t, "B", branch.Name)
	require.Equal(t, branch.Gid, gid)
	require.Equal(t, projGid, branch.Proj.Gid)
	require.Equal(t, "P", branch.Proj.Name)

	query := `{
		me(func: has(Branch.name)) {
			uid
			Branch.name
			Branch.clerk_id
			Branch.proj {
				uid
				Project.name
				Project.clerk_id
			}
		}
	}`
	resp, err := db1.Query(ctx, query)
	require.NoError(t, err)
	require.JSONEq(t,
		`{"me":[{"uid":"0x3","Branch.name":"B","Branch.clerk_id":"123","Branch.proj":
		{"uid":"0x2","Project.name":"P","Project.clerk_id":"456"}}]}`,
		string(resp.GetJson()))

	gid, queriedBranch, err := modusdb.Get[Branch](db, gid, db1.ID())
	require.NoError(t, err)
	require.Equal(t, queriedBranch.Gid, gid)
	require.Equal(t, "B", queriedBranch.Name)

}

func TestLinkingObjectsByGid(t *testing.T) {
	ctx := context.Background()
	db, err := modusdb.New(modusdb.NewDefaultConfig(t.TempDir()))
	require.NoError(t, err)
	defer db.Close()

	db1, err := db.CreateNamespace()
	require.NoError(t, err)

	require.NoError(t, db1.DropData(ctx))

	projGid, project, err := modusdb.Create(db, &Project{
		Name:    "P",
		ClerkId: "456",
	}, db1.ID())
	require.NoError(t, err)

	require.Equal(t, "P", project.Name)
	require.Equal(t, project.Gid, projGid)

	branch := &Branch{
		Name:    "B",
		ClerkId: "123",
		Proj: Project{
			Gid: projGid,
		},
	}

	gid, branch, err := modusdb.Create(db, branch, db1.ID())
	require.NoError(t, err)

	require.Equal(t, "B", branch.Name)
	require.Equal(t, branch.Gid, gid)
	require.Equal(t, projGid, branch.Proj.Gid)
	require.Equal(t, "P", branch.Proj.Name)

	query := `{
		me(func: has(Branch.name)) {
			uid
			Branch.name
			Branch.clerk_id
			Branch.proj {
				uid
				Project.name
				Project.clerk_id
			}
		}
	}`
	resp, err := db1.Query(ctx, query)
	require.NoError(t, err)
	require.JSONEq(t,
		`{"me":[{"uid":"0x3","Branch.name":"B","Branch.clerk_id":"123",
		"Branch.proj":{"uid":"0x2","Project.name":"P","Project.clerk_id":"456"}}]}`,
		string(resp.GetJson()))

	gid, queriedBranch, err := modusdb.Get[Branch](db, gid, db1.ID())
	require.NoError(t, err)
	require.Equal(t, queriedBranch.Gid, gid)
	require.Equal(t, "B", queriedBranch.Name)

}

type BadProject struct {
	Name    string `json:"name,omitempty"`
	ClerkId string `json:"clerk_id,omitempty"`
}

type BadBranch struct {
	Gid     uint64     `json:"gid,omitempty"`
	Name    string     `json:"name,omitempty"`
	ClerkId string     `json:"clerk_id,omitempty" db:"constraint=unique"`
	Proj    BadProject `json:"proj,omitempty"`
}

func TestNestedObjectMutationWithBadType(t *testing.T) {
	ctx := context.Background()
	db, err := modusdb.New(modusdb.NewDefaultConfig(t.TempDir()))
	require.NoError(t, err)
	defer db.Close()

	db1, err := db.CreateNamespace()
	require.NoError(t, err)

	require.NoError(t, db1.DropData(ctx))

	branch := &BadBranch{
		Name:    "B",
		ClerkId: "123",
		Proj: BadProject{
			Name:    "P",
			ClerkId: "456",
		},
	}

	_, _, err = modusdb.Create(db, branch, db1.ID())
	require.Error(t, err)
	require.Equal(t, fmt.Sprintf(modusdb.NoUniqueConstr, "BadProject"), err.Error())

	proj := &BadProject{
		Name:    "P",
		ClerkId: "456",
	}

	_, _, err = modusdb.Create(db, proj, db1.ID())
	require.Error(t, err)
	require.Equal(t, fmt.Sprintf(modusdb.NoUniqueConstr, "BadProject"), err.Error())

}

func TestRawAPIs(t *testing.T) {
	ctx := context.Background()
	db, err := modusdb.New(modusdb.NewDefaultConfig(t.TempDir()))
	require.NoError(t, err)
	defer db.Close()

	db1, err := db.CreateNamespace()
	require.NoError(t, err)

	require.NoError(t, db1.DropData(ctx))

	gid, err := modusdb.RawCreate(db, map[string]any{
		"name":     "B",
		"age":      20,
		"clerk_id": "123",
	}, map[string]string{
		"clerk_id": "unique",
	}, db1.ID())

	require.NoError(t, err)

	query := `{
		me(func: has(name)) {
			uid
			name
			age
			clerk_id
		}
	}`

	resp, err := db1.Query(ctx, query)
	require.NoError(t, err)
	require.JSONEq(t, `{"me":[{"uid":"0x2","name":"B","age":20,"clerk_id":"123"}]}`,
		string(resp.GetJson()))

	getGid, maps, err := modusdb.RawGet(db, gid, []string{"name", "age", "clerk_id"}, db1.ID())
	require.NoError(t, err)
	require.Equal(t, gid, getGid)
	require.Equal(t, "B", maps["name"])

	//TODO figure out why it comes back as a float64
	// schema and value are correctly set to int, so its a query side issue
	require.Equal(t, float64(20), maps["age"])
	require.Equal(t, "123", maps["clerk_id"])

	deleteGid, err := modusdb.RawDelete(db, modusdb.ConstrainedField{
		Key:   "clerk_id",
		Value: "123",
	}, db1.ID())
	require.NoError(t, err)
	require.Equal(t, gid, deleteGid)
}

func TestVectorIndexInsert(t *testing.T) {
	ctx := context.Background()
	db, err := modusdb.New(modusdb.NewDefaultConfig(t.TempDir()))
	require.NoError(t, err)
	defer db.Close()

	db1, err := db.CreateNamespace()
	require.NoError(t, err)

	require.NoError(t, db1.DropData(ctx))

	_, err = modusdb.RawCreate(db, map[string]any{
		"name":     "B",
		"age":      20,
		"clerk_id": "123",
		"vec":      []float64{1.0, 2.0, 3.0},
	}, map[string]string{
		"clerk_id": "unique",
		"vec":      "vector",
	}, db1.ID())

	require.NoError(t, err)

	query := `{
		me(func: has(name)) {
			uid
			name
			age
			clerk_id
			vec
		}
	}`
	resp, err := db1.Query(ctx, query)
	require.NoError(t, err)
	require.JSONEq(t, `{"me":[{"uid":"0x2","name":"B","age":20,"clerk_id":"123","vec":[1,2,3]}]}`,
		string(resp.GetJson()))

}

func TestVectorIndexSearchUntyped(t *testing.T) {
	ctx := context.Background()
	db, err := modusdb.New(modusdb.NewDefaultConfig(t.TempDir()))
	require.NoError(t, err)
	defer db.Close()

	db1, err := db.CreateNamespace()
	require.NoError(t, err)

	require.NoError(t, db1.DropData(ctx))

	vectors := [][]float64{
		{1.0, 2.0, 3.0},    // Sequential
		{4.0, 5.0, 6.0},    // Sequential continued
		{7.0, 8.0, 9.0},    // Sequential continued
		{0.1, 0.2, 0.3},    // Small decimals
		{1.5, 2.5, 3.5},    // Half steps
		{1.0, 2.0, 3.0},    // Duplicate
		{10.0, 20.0, 30.0}, // Tens
		{0.5, 1.0, 1.5},    // Half increments
		{2.2, 4.4, 6.6},    // Multiples of 2.2
		{3.3, 6.6, 9.9},    // Multiples of 3.3
	}

	for _, vec := range vectors {
		_, err = modusdb.RawCreate(db, map[string]any{
			"vec": vec,
		}, map[string]string{
			"vec": "vector",
		}, db1.ID())
		require.NoError(t, err)
	}

	const query = `
		{
			vector(func: similar_to(vec, 5, "[4.1,5.1,6.1]")) {
					vec
			}
		}`

	resp, err := db1.Query(ctx, query)
	require.NoError(t, err)
	require.JSONEq(t, `{
        "vector":[
            {"vec":[4,5,6]},
            {"vec":[7,8,9]},
            {"vec":[1.5,2.5,3.5]},
            {"vec":[10,20,30]},
            {"vec":[2.2,4.4,6.6]}
        ]
    }`, string(resp.GetJson()))
}

type Document struct {
	Gid     uint64    `json:"gid,omitempty"`
	Text    string    `json:"text,omitempty"`
	TextVec []float32 `json:"textVec,omitempty" db:"constraint=vector"`
}

func TestVectorIndexSearchTyped(t *testing.T) {
	ctx := context.Background()
	db, err := modusdb.New(modusdb.NewDefaultConfig(t.TempDir()))
	require.NoError(t, err)
	defer db.Close()

	db1, err := db.CreateNamespace()
	require.NoError(t, err)

	require.NoError(t, db1.DropData(ctx))

	documents := []Document{
		{Text: "apple", TextVec: []float32{1.0, 0.0, 0.0}},
		{Text: "banana", TextVec: []float32{0.0, 1.0, 0.0}},
		{Text: "carrot", TextVec: []float32{0.0, 0.0, 1.0}},
		{Text: "dog", TextVec: []float32{1.0, 1.0, 0.0}},
		{Text: "elephant", TextVec: []float32{0.0, 1.0, 1.0}},
		{Text: "fox", TextVec: []float32{1.0, 0.0, 1.0}},
		{Text: "gorilla", TextVec: []float32{1.0, 1.0, 1.0}},
	}

	for _, doc := range documents {
		_, _, err = modusdb.Create(db, &doc, db1.ID())
		require.NoError(t, err)
	}

	const query = `
		{
			documents(func: similar_to(Document.textVec, 5, "[0.1,0.1,0.1]")) {
					Document.text
			}
		}`

	resp, err := db1.Query(ctx, query)
	require.NoError(t, err)
	require.JSONEq(t, `{
		"documents":[
			{"Document.text":"apple"},
			{"Document.text":"dog"},
			{"Document.text":"elephant"},
			{"Document.text":"fox"},
			{"Document.text":"gorilla"}
		]
	}`, string(resp.GetJson()))
}
