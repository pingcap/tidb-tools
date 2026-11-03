// Copyright 2024 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"testing"

	"github.com/pingcap/tidb-tools/pkg/dbutil"
	"github.com/pingcap/tidb-tools/sync_diff_inspector/chunk"
	"github.com/pingcap/tidb-tools/sync_diff_inspector/source"
	"github.com/pingcap/tidb-tools/sync_diff_inspector/source/common"
	"github.com/pingcap/tidb-tools/sync_diff_inspector/splitter"
	"github.com/pingcap/tidb-tools/sync_diff_inspector/utils"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/stretchr/testify/require"
)

type collationTestSource struct {
	source.Source
	table *common.TableDiff
	keys  []string
}

func (s *collationTestSource) GetTables() []*common.TableDiff { return []*common.TableDiff{s.table} }
func (s *collationTestSource) GetRowsIterator(context.Context, *splitter.RangeInfo) (source.RowDataIterator, error) {
	return &collationTestRows{keys: s.keys}, nil
}
func (s *collationTestSource) GenerateFixSQL(kind source.DMLType, upstream, downstream map[string]*dbutil.ColumnData, _ int) string {
	if kind == source.Delete {
		return utils.GenerateDeleteDML(downstream, s.table.Info, s.table.Schema)
	}
	return utils.GenerateReplaceDML(upstream, s.table.Info, s.table.Schema)
}

type collationTestRows struct{ keys []string }

func (r *collationTestRows) Next() (map[string]*dbutil.ColumnData, error) {
	if len(r.keys) == 0 {
		return nil, nil
	}
	key := r.keys[0]
	r.keys = r.keys[1:]
	return map[string]*dbutil.ColumnData{"k": {Data: []byte(key)}}, nil
}
func (r *collationTestRows) Close() {}

func TestCompareRowsPreservesSharedCaseInsensitiveKey(t *testing.T) {
	enabled := collate.NewCollationEnabled()
	collate.SetNewCollationEnabledForTest(true)
	defer collate.SetNewCollationEnabledForTest(enabled)
	info, err := dbutil.GetTableInfoBySQL("CREATE TABLE t (k VARCHAR(20) PRIMARY KEY) COLLATE=utf8mb4_general_ci", parser.New())
	require.NoError(t, err)
	table := &common.TableDiff{Schema: "test", Table: "t", Info: info}
	upstream := &collationTestSource{table: table, keys: []string{"B"}}
	downstream := &collationTestSource{table: table, keys: []string{"a", "B"}}
	df := &Diff{upstream: upstream, downstream: downstream, workSource: downstream, exportFixSQL: true}
	dml := new(ChunkDML)
	equal, err := df.compareRows(context.Background(), &splitter.RangeInfo{ChunkRange: &chunk.Range{Index: &chunk.ChunkID{}}}, dml)
	require.NoError(t, err)
	require.False(t, equal)
	require.Zero(t, dml.rowAdd)
	require.Equal(t, 1, dml.rowDelete)
	require.Equal(t, []string{"DELETE FROM `test`.`t` WHERE `k` = 'a' LIMIT 1;"}, dml.sqls)
}
