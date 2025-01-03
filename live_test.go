/*
 * Copyright 2025 Hypermode Inc.
 * Licensed under the terms of the Apache License, Version 2.0
 * See the LICENSE file that accompanied this code for further details.
 *
 * SPDX-FileCopyrightText: 2025 Hypermode Inc. <hello@hypermode.com>
 * SPDX-License-Identifier: Apache-2.0
 */

package modusdb_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/cavaliergopher/grab/v3"
	"github.com/dgraph-io/dgraph/v24/dgraphapi"
	"github.com/dgraph-io/dgraph/v24/systest/1million/common"
	"github.com/stretchr/testify/require"

	"github.com/hypermodeinc/modusdb"
)

const (
	baseURL          = "https://github.com/dgraph-io/benchmarks/blob/master/data"
	oneMillionSchema = baseURL + "/1million.schema?raw=true"
	oneMillionRDF    = baseURL + "/1million.rdf.gz?raw=true"
	DbSchema         = `
			director.film : [uid] @reverse @count .
			name          : string @index(hash, term, trigram, fulltext) @lang .
		`
	SmallData = `
			<12534504120601169429> <name> "Marc Caro"@en .
			<2698880893682087932> <name> "Delicatessen"@en .
			<2698880893682087932> <name> "Delicatessen"@de .
			<2698880893682087932> <name> "Delicatessen"@it .
			<12534504120601169429> <director.film> <2698880893682087932> .
			<14514306440537019930> <director.film> <2698880893682087932> .
			<15617393957106514527> <name> "The City of Lost Children"@en .
			<15617393957106514527> <name> "Die Stadt der verlorenen Kinder"@de .
			<15617393957106514527> <name> "La città perduta"@it .
			<12534504120601169429> <director.film> <15617393957106514527> .
			<14514306440537019930> <director.film> <15617393957106514527> .
		`
)

func TestLiveLoaderSmall(t *testing.T) {

	db, err := modusdb.New(modusdb.NewDefaultConfig(t.TempDir()))
	require.NoError(t, err)
	defer db.Close()

	dataFolder := t.TempDir()
	schemaFile := filepath.Join(dataFolder, "data.schema")
	dataFile := filepath.Join(dataFolder, "data.rdf")
	require.NoError(t, os.WriteFile(schemaFile, []byte(DbSchema), 0600))
	require.NoError(t, os.WriteFile(dataFile, []byte(SmallData), 0600))
	require.NoError(t, db.Load(context.Background(), schemaFile, dataFile))

	const query = `{
		caro(func: allofterms(name@en, "Marc Caro")) {
			name@en
			director.film {
			  name@en
			}
		}
	  }`
	const expected = `{
			"caro": [
				{
					"name@en": "Marc Caro",
					"director.film": [
						{
							"name@en": "Delicatessen"
						},
						{
							"name@en": "The City of Lost Children"
						}
					]
				}
			]
		}`

	resp, err := db.Query(context.Background(), query)
	require.NoError(t, err)
	require.JSONEq(t, expected, string(resp.Json))
}

func TestLiveLoader1Million(t *testing.T) {
	db, err := modusdb.New(modusdb.NewDefaultConfig(t.TempDir()))
	require.NoError(t, err)
	defer db.Close()

	baseDir := t.TempDir()
	schResp, err := grab.Get(baseDir, oneMillionSchema)
	require.NoError(t, err)
	dataResp, err := grab.Get(baseDir, oneMillionRDF)
	require.NoError(t, err)

	require.NoError(t, db.DropAll(context.Background()))
	require.NoError(t, db.Load(context.Background(), schResp.Filename, dataResp.Filename))

	for _, tt := range common.OneMillionTCs {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
		resp, err := db.Query(ctx, tt.Query)
		cancel()

		if ctx.Err() == context.DeadlineExceeded {
			t.Fatal("aborting test due to query timeout")
		}
		require.NoError(t, err)
		require.NoError(t, dgraphapi.CompareJSON(tt.Resp, string(resp.Json)))
	}
}
