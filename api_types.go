/*
 * Copyright 2025 Hypermode Inc.
 * Licensed under the terms of the Apache License, Version 2.0
 * See the LICENSE file that accompanied this code for further details.
 *
 * SPDX-FileCopyrightText: 2025 Hypermode Inc. <hello@hypermode.com>
 * SPDX-License-Identifier: Apache-2.0
 */

package modusdb

import (
	"context"
	"encoding/binary"
	"fmt"
	"strings"
	"time"

	"github.com/dgraph-io/dgo/v240/protos/api"
	"github.com/dgraph-io/dgraph/v24/protos/pb"
	"github.com/dgraph-io/dgraph/v24/types"
	"github.com/dgraph-io/dgraph/v24/x"
	"github.com/hypermodeinc/modusdb/api/dql_query"
	"github.com/hypermodeinc/modusdb/api/utils"
	"github.com/twpayne/go-geom"
	"github.com/twpayne/go-geom/encoding/wkb"
)

type UniqueField interface {
	uint64 | ConstrainedField
}
type ConstrainedField struct {
	Key   string
	Value any
}

type QueryParams struct {
	Filter     *Filter
	Pagination *Pagination
	Sorting    *Sorting
}

type Filter struct {
	Field  string
	String StringPredicate
	Vector VectorPredicate
	And    *Filter
	Or     *Filter
	Not    *Filter
}

type Pagination struct {
	Limit  int64
	Offset int64
	After  string
}

type Sorting struct {
	OrderAscField  string
	OrderDescField string
	OrderDescFirst bool
}

type StringPredicate struct {
	Equals         string
	LessThan       string
	LessOrEqual    string
	GreaterThan    string
	GreaterOrEqual string
	AllOfTerms     []string
	AnyOfTerms     []string
	AllOfText      []string
	AnyOfText      []string
	RegExp         string
}

type VectorPredicate struct {
	SimilarTo []float32
	TopK      int64
}

type ModusDbOption func(*modusDbOptions)

type modusDbOptions struct {
	namespace uint64
}

func WithNamespace(namespace uint64) ModusDbOption {
	return func(o *modusDbOptions) {
		o.namespace = namespace
	}
}

func getDefaultNamespace(db *DB, ns ...uint64) (context.Context, *Namespace, error) {
	dbOpts := &modusDbOptions{
		namespace: db.defaultNamespace.ID(),
	}
	for _, ns := range ns {
		WithNamespace(ns)(dbOpts)
	}

	n, err := db.getNamespaceWithLock(dbOpts.namespace)
	if err != nil {
		return nil, nil, err
	}

	ctx := context.Background()
	ctx = x.AttachNamespace(ctx, n.ID())

	return ctx, n, nil
}

func valueToPosting_ValType(v any) (pb.Posting_ValType, error) {
	switch v.(type) {
	case string:
		return pb.Posting_STRING, nil
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32:
		return pb.Posting_INT, nil
	case uint64:
		return pb.Posting_UID, nil
	case bool:
		return pb.Posting_BOOL, nil
	case float32, float64:
		return pb.Posting_FLOAT, nil
	case []byte:
		return pb.Posting_BINARY, nil
	case time.Time:
		return pb.Posting_DATETIME, nil
	case geom.Point:
		return pb.Posting_GEO, nil
	case []float32, []float64:
		return pb.Posting_VFLOAT, nil
	default:
		return pb.Posting_DEFAULT, fmt.Errorf("unsupported type %T", v)
	}
}

func valueToApiVal(v any) (*api.Value, error) {
	switch val := v.(type) {
	case string:
		return &api.Value{Val: &api.Value_StrVal{StrVal: val}}, nil
	case int:
		return &api.Value{Val: &api.Value_IntVal{IntVal: int64(val)}}, nil
	case int8:
		return &api.Value{Val: &api.Value_IntVal{IntVal: int64(val)}}, nil
	case int16:
		return &api.Value{Val: &api.Value_IntVal{IntVal: int64(val)}}, nil
	case int32:
		return &api.Value{Val: &api.Value_IntVal{IntVal: int64(val)}}, nil
	case int64:
		return &api.Value{Val: &api.Value_IntVal{IntVal: val}}, nil
	case uint8:
		return &api.Value{Val: &api.Value_IntVal{IntVal: int64(val)}}, nil
	case uint16:
		return &api.Value{Val: &api.Value_IntVal{IntVal: int64(val)}}, nil
	case uint32:
		return &api.Value{Val: &api.Value_IntVal{IntVal: int64(val)}}, nil
	case uint64:
		return &api.Value{Val: &api.Value_UidVal{UidVal: val}}, nil
	case bool:
		return &api.Value{Val: &api.Value_BoolVal{BoolVal: val}}, nil
	case float32:
		return &api.Value{Val: &api.Value_DoubleVal{DoubleVal: float64(val)}}, nil
	case float64:
		return &api.Value{Val: &api.Value_DoubleVal{DoubleVal: val}}, nil
	case []float32:
		return &api.Value{Val: &api.Value_Vfloat32Val{
			Vfloat32Val: types.FloatArrayAsBytes(val)}}, nil
	case []float64:
		float32Slice := make([]float32, len(val))
		for i, v := range val {
			float32Slice[i] = float32(v)
		}
		return &api.Value{Val: &api.Value_Vfloat32Val{
			Vfloat32Val: types.FloatArrayAsBytes(float32Slice)}}, nil
	case []byte:
		return &api.Value{Val: &api.Value_BytesVal{BytesVal: val}}, nil
	case time.Time:
		bytes, err := val.MarshalBinary()
		if err != nil {
			return nil, err
		}
		return &api.Value{Val: &api.Value_DateVal{DateVal: bytes}}, nil
	case geom.Point:
		bytes, err := wkb.Marshal(&val, binary.LittleEndian)
		if err != nil {
			return nil, err
		}
		return &api.Value{Val: &api.Value_GeoVal{GeoVal: bytes}}, nil
	case uint:
		return &api.Value{Val: &api.Value_DefaultVal{DefaultVal: fmt.Sprint(v)}}, nil
	default:
		return nil, fmt.Errorf("unsupported type %T", v)
	}
}

func filterToQueryFunc(typeName string, f Filter) dql_query.QueryFunc {
	// Handle logical operators first
	if f.And != nil {
		return dql_query.And(filterToQueryFunc(typeName, *f.And))
	}
	if f.Or != nil {
		return dql_query.Or(filterToQueryFunc(typeName, *f.Or))
	}
	if f.Not != nil {
		return dql_query.Not(filterToQueryFunc(typeName, *f.Not))
	}

	// Handle field predicates
	if f.String.Equals != "" {
		return dql_query.BuildEqQuery(utils.GetPredicateName(typeName, f.Field), f.String.Equals)
	}
	if len(f.String.AllOfTerms) != 0 {
		return dql_query.BuildAllOfTermsQuery(utils.GetPredicateName(typeName, f.Field), strings.Join(f.String.AllOfTerms, " "))
	}
	if len(f.String.AnyOfTerms) != 0 {
		return dql_query.BuildAnyOfTermsQuery(utils.GetPredicateName(typeName, f.Field), strings.Join(f.String.AnyOfTerms, " "))
	}
	if len(f.String.AllOfText) != 0 {
		return dql_query.BuildAllOfTextQuery(utils.GetPredicateName(typeName, f.Field), strings.Join(f.String.AllOfText, " "))
	}
	if len(f.String.AnyOfText) != 0 {
		return dql_query.BuildAnyOfTextQuery(utils.GetPredicateName(typeName, f.Field), strings.Join(f.String.AnyOfText, " "))
	}
	if f.String.RegExp != "" {
		return dql_query.BuildRegExpQuery(utils.GetPredicateName(typeName, f.Field), f.String.RegExp)
	}
	if f.String.LessThan != "" {
		return dql_query.BuildLtQuery(utils.GetPredicateName(typeName, f.Field), f.String.LessThan)
	}
	if f.String.LessOrEqual != "" {
		return dql_query.BuildLeQuery(utils.GetPredicateName(typeName, f.Field), f.String.LessOrEqual)
	}
	if f.String.GreaterThan != "" {
		return dql_query.BuildGtQuery(utils.GetPredicateName(typeName, f.Field), f.String.GreaterThan)
	}
	if f.String.GreaterOrEqual != "" {
		return dql_query.BuildGeQuery(utils.GetPredicateName(typeName, f.Field), f.String.GreaterOrEqual)
	}
	if f.Vector.SimilarTo != nil {
		return dql_query.BuildSimilarToQuery(utils.GetPredicateName(typeName, f.Field), f.Vector.TopK, f.Vector.SimilarTo)
	}

	// Return empty query if no conditions match
	return func() string { return "" }
}

// Helper function to combine multiple filters
func filtersToQueryFunc(typeName string, filter Filter) dql_query.QueryFunc {
	return filterToQueryFunc(typeName, filter)
}

func paginationToQueryString(p Pagination) string {
	paginationStr := ""
	if p.Limit > 0 {
		paginationStr += ", " + fmt.Sprintf("first: %d", p.Limit)
	}
	if p.Offset > 0 {
		paginationStr += ", " + fmt.Sprintf("offset: %d", p.Offset)
	} else if p.After != "" {
		paginationStr += ", " + fmt.Sprintf("after: %s", p.After)
	}
	if paginationStr == "" {
		return ""
	}
	return paginationStr
}

func sortingToQueryString(typeName string, s Sorting) string {
	if s.OrderAscField == "" && s.OrderDescField == "" {
		return ""
	}

	var parts []string
	first, second := s.OrderDescField, s.OrderAscField
	firstOp, secondOp := "orderdesc", "orderasc"

	if !s.OrderDescFirst {
		first, second = s.OrderAscField, s.OrderDescField
		firstOp, secondOp = "orderasc", "orderdesc"
	}

	if first != "" {
		parts = append(parts, fmt.Sprintf("%s: %s", firstOp, utils.GetPredicateName(typeName, first)))
	}
	if second != "" {
		parts = append(parts, fmt.Sprintf("%s: %s", secondOp, utils.GetPredicateName(typeName, second)))
	}

	return ", " + strings.Join(parts, ", ")
}
