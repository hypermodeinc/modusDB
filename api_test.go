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
	ClerkId string `json:"clerk_id,omitempty" db:"constraint=exact"`
}

func TestFirstTimeUser(t *testing.T) {
	db, err := modusdb.New(modusdb.NewDefaultConfig(t.TempDir()))
	require.NoError(t, err)
	defer db.Close()

	gid, user, err := modusdb.Create(db, User{
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
	require.Equal(t, queriedUser3, User{})

}

func TestCreateApi(t *testing.T) {
	ctx := context.Background()
	db, err := modusdb.New(modusdb.NewDefaultConfig(t.TempDir()))
	require.NoError(t, err)
	defer db.Close()

	db1, err := db.CreateNamespace()
	require.NoError(t, err)

	require.NoError(t, db1.DropData(ctx))

	user := User{
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

	user := User{
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

	user := User{
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

	user := User{
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

	user := User{
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
	require.Equal(t, queriedUser, User{})

	_, queriedUser, err = modusdb.Get[User](db, modusdb.ConstrainedField{
		Key:   "clerk_id",
		Value: "123",
	}, db1.ID())
	require.Error(t, err)
	require.Equal(t, "no object found", err.Error())
	require.Equal(t, queriedUser, User{})
}

func TestUpsertApi(t *testing.T) {
	ctx := context.Background()
	db, err := modusdb.New(modusdb.NewDefaultConfig(t.TempDir()))
	require.NoError(t, err)
	defer db.Close()

	db1, err := db.CreateNamespace()
	require.NoError(t, err)

	require.NoError(t, db1.DropData(ctx))

	user := User{
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

func TestQueryApi(t *testing.T) {
	ctx := context.Background()
	db, err := modusdb.New(modusdb.NewDefaultConfig(t.TempDir()))
	require.NoError(t, err)
	defer db.Close()

	db1, err := db.CreateNamespace()
	require.NoError(t, err)

	require.NoError(t, db1.DropData(ctx))

	users := []User{
		{Name: "A", Age: 10, ClerkId: "123"},
		{Name: "B", Age: 20, ClerkId: "123"},
		{Name: "C", Age: 30, ClerkId: "123"},
		{Name: "D", Age: 40, ClerkId: "123"},
		{Name: "E", Age: 50, ClerkId: "123"},
	}

	for _, user := range users {
		_, _, err = modusdb.Create(db, user, db1.ID())
		require.NoError(t, err)
	}

	gids, queriedUsers, err := modusdb.Query[User](db, modusdb.QueryParams{}, db1.ID())
	require.NoError(t, err)
	require.Len(t, queriedUsers, 5)
	require.Len(t, gids, 5)
	require.Equal(t, "A", queriedUsers[0].Name)
	require.Equal(t, "B", queriedUsers[1].Name)
	require.Equal(t, "C", queriedUsers[2].Name)
	require.Equal(t, "D", queriedUsers[3].Name)
	require.Equal(t, "E", queriedUsers[4].Name)

	gids, queriedUsers, err = modusdb.Query[User](db, modusdb.QueryParams{
		Filter: modusdb.Filter{
			Field: "age",
			String: modusdb.StringPredicate{
				// The reason its a string even for int is bc i cant tell if
				// user wants to compare with 0 the number or didn't provide a value
				// TODO: fix this
				GreaterOrEqual: fmt.Sprintf("%d", 20),
			},
		},
	}, db1.ID())

	require.NoError(t, err)
	require.Len(t, queriedUsers, 4)
	require.Len(t, gids, 4)
	require.Equal(t, "B", queriedUsers[0].Name)
	require.Equal(t, "C", queriedUsers[1].Name)
	require.Equal(t, "D", queriedUsers[2].Name)
	require.Equal(t, "E", queriedUsers[3].Name)
}

func TestQueryApiWithPaginiationAndSorting(t *testing.T) {
	ctx := context.Background()
	db, err := modusdb.New(modusdb.NewDefaultConfig(t.TempDir()))
	require.NoError(t, err)
	defer db.Close()

	db1, err := db.CreateNamespace()
	require.NoError(t, err)

	require.NoError(t, db1.DropData(ctx))

	users := []User{
		{Name: "A", Age: 10, ClerkId: "123"},
		{Name: "B", Age: 20, ClerkId: "123"},
		{Name: "C", Age: 30, ClerkId: "123"},
		{Name: "D", Age: 40, ClerkId: "123"},
		{Name: "E", Age: 50, ClerkId: "123"},
	}

	for _, user := range users {
		_, _, err = modusdb.Create(db, user, db1.ID())
		require.NoError(t, err)
	}

	gids, queriedUsers, err := modusdb.Query[User](db, modusdb.QueryParams{
		Filter: modusdb.Filter{
			Field: "age",
			String: modusdb.StringPredicate{
				GreaterOrEqual: fmt.Sprintf("%d", 20),
			},
		},
		Pagination: modusdb.Pagination{
			Limit:  3,
			Offset: 1,
		},
	}, db1.ID())

	require.NoError(t, err)
	require.Len(t, queriedUsers, 3)
	require.Len(t, gids, 3)
	require.Equal(t, "C", queriedUsers[0].Name)
	require.Equal(t, "D", queriedUsers[1].Name)
	require.Equal(t, "E", queriedUsers[2].Name)

	gids, queriedUsers, err = modusdb.Query[User](db, modusdb.QueryParams{
		Pagination: modusdb.Pagination{
			Limit:  3,
			Offset: 1,
		},
		Sorting: modusdb.Sorting{
			OrderAscField: "age",
		},
	}, db1.ID())

	require.NoError(t, err)
	require.Len(t, queriedUsers, 3)
	require.Len(t, gids, 3)
	require.Equal(t, "B", queriedUsers[0].Name)
	require.Equal(t, "C", queriedUsers[1].Name)
	require.Equal(t, "D", queriedUsers[2].Name)
}

type Project struct {
	Gid     uint64 `json:"gid,omitempty"`
	Name    string `json:"name,omitempty"`
	ClerkId string `json:"clerk_id,omitempty" db:"constraint=exact"`
	// Branches []Branch `json:"branches,omitempty" readFrom:"type=Branch,field=proj"`
}

type Branch struct {
	Gid     uint64  `json:"gid,omitempty"`
	Name    string  `json:"name,omitempty"`
	ClerkId string  `json:"clerk_id,omitempty" db:"constraint=exact"`
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

	branch := Branch{
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

	projGid, project, err := modusdb.Create(db, Project{
		Name:    "P",
		ClerkId: "456",
	}, db1.ID())
	require.NoError(t, err)

	require.Equal(t, "P", project.Name)
	require.Equal(t, project.Gid, projGid)

	branch := Branch{
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

	projGid, project, err := modusdb.Create(db, Project{
		Name:    "P",
		ClerkId: "456",
	}, db1.ID())
	require.NoError(t, err)

	require.Equal(t, "P", project.Name)
	require.Equal(t, project.Gid, projGid)

	branch := Branch{
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
	ClerkId string     `json:"clerk_id,omitempty" db:"constraint=exact"`
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

	branch := BadBranch{
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

	proj := BadProject{
		Name:    "P",
		ClerkId: "456",
	}

	_, _, err = modusdb.Create(db, proj, db1.ID())
	require.Error(t, err)
	require.Equal(t, fmt.Sprintf(modusdb.NoUniqueConstr, "BadProject"), err.Error())

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
		{Text: "apple", TextVec: []float32{0.1, 0.1, 0.0}},
		{Text: "banana", TextVec: []float32{0.0, 1.0, 0.0}},
		{Text: "carrot", TextVec: []float32{0.0, 0.0, 1.0}},
		{Text: "dog", TextVec: []float32{1.0, 1.0, 0.0}},
		{Text: "elephant", TextVec: []float32{0.0, 1.0, 1.0}},
		{Text: "fox", TextVec: []float32{1.0, 0.0, 1.0}},
		{Text: "gorilla", TextVec: []float32{1.0, 1.0, 1.0}},
	}

	for _, doc := range documents {
		_, _, err = modusdb.Create(db, doc, db1.ID())
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

	const query2 = `
		{
			documents(func: type("Document")) @filter(similar_to(Document.textVec, 5, "[0.1,0.1,0.1]")) {
					Document.text
			}
		}`

	resp, err = db1.Query(ctx, query2)
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

func TestVectorIndexSearchWithQuery(t *testing.T) {
	ctx := context.Background()
	db, err := modusdb.New(modusdb.NewDefaultConfig(t.TempDir()))
	require.NoError(t, err)
	defer db.Close()

	db1, err := db.CreateNamespace()
	require.NoError(t, err)

	require.NoError(t, db1.DropData(ctx))

	documents := []Document{
		{Text: "apple", TextVec: []float32{0.1, 0.1, 0.0}},
		{Text: "banana", TextVec: []float32{0.0, 1.0, 0.0}},
		{Text: "carrot", TextVec: []float32{0.0, 0.0, 1.0}},
		{Text: "dog", TextVec: []float32{1.0, 1.0, 0.0}},
		{Text: "elephant", TextVec: []float32{0.0, 1.0, 1.0}},
		{Text: "fox", TextVec: []float32{1.0, 0.0, 1.0}},
		{Text: "gorilla", TextVec: []float32{1.0, 1.0, 1.0}},
	}

	for _, doc := range documents {
		_, _, err = modusdb.Create(db, doc, db1.ID())
		require.NoError(t, err)
	}

	gids, docs, err := modusdb.Query[Document](db, modusdb.QueryParams{
		Filter: modusdb.Filter{

			Field: "textVec",
			Vector: modusdb.VectorPredicate{
				SimilarTo: []float32{0.1, 0.1, 0.1},
				TopK:      5,
			},
		},
	}, db1.ID())

	require.NoError(t, err)
	require.Len(t, docs, 5)
	require.Len(t, gids, 5)
	require.Equal(t, "apple", docs[0].Text)
	require.Equal(t, "dog", docs[1].Text)
	require.Equal(t, "elephant", docs[2].Text)
	require.Equal(t, "fox", docs[3].Text)
	require.Equal(t, "gorilla", docs[4].Text)
}
