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
	"github.com/pingcap/tidb-tools/sync_diff_inspector/source/common"
	"github.com/pingcap/tidb-tools/sync_diff_inspector/splitter"
	"github.com/pingcap/tidb-tools/sync_diff_inspector/utils"
)

type MySQLSource struct {
	BasicSource
}

func (s *MySQLSource) GetTableAnalyzer() TableAnalyzer {
	return &MySQLTableAnalyzer{s.dbConn}
}

type MySQLTableAnalyzer struct {
	dbConn *sql.DB
}

func (a *MySQLTableAnalyzer) AnalyzeSplitter(ctx context.Context, table *common.TableDiff, startRange *splitter.RangeInfo) (splitter.ChunkIterator, error) {
	chunkSize := 1000
	// use random splitter if we cannot use bucket splitter, then we can simply choose target table to generate chunks.
	randIter, err := splitter.NewRandomIteratorWithCheckpoint(ctx, table, a.dbConn, chunkSize, startRange)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return randIter, nil
}

func NewMySQLSource(ctx context.Context, tableDiffs []*common.TableDiff, dbConn *sql.DB) (Source, error) {
	ts := &MySQLSource{
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

type MySQLSources struct {
}

func NewMySQLSources(ctx context.Context) (Source, error) {
	return nil, nil
}
