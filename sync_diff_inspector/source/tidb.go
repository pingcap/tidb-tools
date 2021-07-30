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

func (s *TiDBSource) GetTableIter() TableIter {
	return &TiDBTableIter{s.dbConn}
}

type TiDBTableIter struct {
	dbConn *sql.DB
}

func (t *TiDBTableIter) GetIterForTable(table *common.TableDiff, startRange *splitter.RangeInfo) (splitter.TableIterator, error) {
	chunkSize := 1000
	// if we decide to use bucket to split chunks
	// we always use bucksIter even we load from checkpoint is not bucketNode
	// TODO check whether we can use bucket for this table to split chunks.
	if true {
		bucketIter, err := splitter.NewBucketIteratorWithCheckpoint(table, t.dbConn, chunkSize, startRange)
		if err == nil {
			return bucketIter, nil
		}
		log.Warn("build bucketIter failed", zap.Error(err))
		// fall back to random splitter
	}

	// use random splitter if we cannot use bucket splitter, then we can simply choose target table to generate chunks.
	randIter, err := splitter.NewRandomIteratorWithCheckpoint(table, t.dbConn, chunkSize, startRange)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return randIter, nil
}

func NewTiDBSource(ctx context.Context, tableDiffs []*common.TableDiff, dbConn *sql.DB) (Source, error) {
	ts := &TiDBSource{
		BasicSource{
			tableDiffs: tableDiffs,
			tableRows:  make([]*TableRows, 0, len(tableDiffs)),
			dbConn:     dbConn,
		},
	}
	for _, table := range tableDiffs {
		tableRowsQuery, tableOrderKeyCols := utils.GetTableRowsQueryFormat(table.Schema, table.Table, table.Info, table.Collation)
		ts.tableRows = append(ts.tableRows, &TableRows{
			tableRowsQuery,
			tableOrderKeyCols,
		})

	}
	return ts, nil
}
