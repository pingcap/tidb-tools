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
	"github.com/pingcap/tidb-tools/pkg/dbutil"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb-tools/sync_diff_inspector/source/common"
	"github.com/pingcap/tidb-tools/sync_diff_inspector/splitter"
	"github.com/pingcap/tidb-tools/sync_diff_inspector/utils"
	"go.uber.org/zap"
)

type TiDBSource struct {
	BasicSource
}

func (s *TiDBSource) GetTableAnalyzer() TableAnalyzer {
	return &TiDBTableAnalyzer{s.dbConn}
}

type TiDBTableAnalyzer struct {
	dbConn *sql.DB
}

func (a *TiDBTableAnalyzer) AnalyzeSplitter(table *common.TableDiff, startRange *splitter.RangeInfo) (splitter.ChunkIterator, error) {
	chunkSize := 1000
	// if we decide to use bucket to split chunks
	// we always use bucksIter even we load from checkpoint is not bucketNode
	// TODO check whether we can use bucket for this table to split chunks.
	if true {
		bucketIter, err := splitter.NewBucketIteratorWithCheckpoint(table, a.dbConn, chunkSize, startRange)
		if err == nil {
			return bucketIter, nil
		}
		log.Warn("build bucketIter failed", zap.Error(err))
		// fall back to random splitter
	}

	// use random splitter if we cannot use bucket splitter, then we can simply choose target table to generate chunks.
	randIter, err := splitter.NewRandomIteratorWithCheckpoint(table, a.dbConn, chunkSize, startRange)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return randIter, nil
}

func (a *TiDBTableAnalyzer) AnalyzeChunkSize(table *common.TableDiff) (int64, error) {
	// TODO analyze chunk size with table
	return dbutil.GetRowCount(context.Background(), a.dbConn, table.Schema, table.Table, table.Range, nil)
}

func NewTiDBSource(ctx context.Context, tableDiffs []*common.TableDiff, dbConn *sql.DB) (Source, error) {
	ts := &TiDBSource{
		BasicSource{
			tableDiffs: tableDiffs,
			dbConn:     dbConn,
		},
	}
	for _, table := range tableDiffs {
		table.TableRowsQuery, table.TableOrderKeyCols = utils.GetTableRowsQueryFormat(table.Schema, table.Table, table.Info, table.Collation)
	}
	return ts, nil
}
