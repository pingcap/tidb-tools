// Copyright 2021 PingCAP, Inc.
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

package source

import (
	"context"
	"database/sql"
	"sort"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb-tools/pkg/dbutil"
	router "github.com/pingcap/tidb-tools/pkg/table-router"
	"github.com/pingcap/tidb-tools/sync_diff_inspector/source/common"
	"github.com/pingcap/tidb-tools/sync_diff_inspector/splitter"
	"go.uber.org/zap"
)

type TiDBSource struct {
	BasicSource
}

func (s *TiDBSource) GetTableAnalyzer() TableAnalyzer {
	return &TiDBTableAnalyzer{
		s.dbConn,
		s.sourceTableMap,
	}
}

type TiDBTableAnalyzer struct {
	dbConn         *sql.DB
	sourceTableMap map[string]*common.TableSource
}

func (a *TiDBTableAnalyzer) AnalyzeSplitter(ctx context.Context, table *common.TableDiff, startRange *splitter.RangeInfo) (splitter.ChunkIterator, error) {
	chunkSize := 0
	originSchema, originTable := getOriginTable(a.sourceTableMap, table)
	table.Schema = originSchema
	table.Table = originTable
	// if we decide to use bucket to split chunks
	// we always use bucksIter even we load from checkpoint is not bucketNode
	// TODO check whether we can use bucket for this table to split chunks.
	bucketIter, err := splitter.NewBucketIteratorWithCheckpoint(ctx, table, a.dbConn, chunkSize, startRange)
	if err == nil {
		return bucketIter, nil
	}
	log.Warn("build bucketIter failed", zap.Error(err))
	// fall back to random splitter

	// use random splitter if we cannot use bucket splitter, then we can simply choose target table to generate chunks.
	randIter, err := splitter.NewRandomIteratorWithCheckpoint(ctx, table, a.dbConn, chunkSize, startRange)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return randIter, nil
}

func NewTiDBSource(ctx context.Context, tableDiffs []*common.TableDiff, tableRouter *router.Table, dbConn *sql.DB) (Source, error) {
	sourceMap, err := getSourceTableMap(ctx, tableDiffs, tableRouter, dbConn)
	if err != nil {
		return nil, errors.Trace(err)
	}
	sort.Slice(tableDiffs, func(i, j int) bool {
		return dbutil.TableName(tableDiffs[i].Schema, tableDiffs[i].Table) < dbutil.TableName(tableDiffs[j].Schema, tableDiffs[j].Table)
	})
	ts := &TiDBSource{
		BasicSource{
			tableDiffs:     tableDiffs,
			sourceTableMap: sourceMap,
			dbConn:         dbConn,
		},
	}
	return ts, nil
}
