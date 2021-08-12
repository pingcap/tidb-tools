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
	router "github.com/pingcap/tidb-tools/pkg/table-router"
	"github.com/pingcap/tidb-tools/sync_diff_inspector/source/common"
	"github.com/pingcap/tidb-tools/sync_diff_inspector/splitter"
)

type MySQLSource struct {
	BasicSource
}

func (s *MySQLSource) GetTableAnalyzer() TableAnalyzer {
	return &MySQLTableAnalyzer{
		s.dbConn,
		s.sourceTableMap,
	}
}

type MySQLTableAnalyzer struct {
	dbConn         *sql.DB
	sourceTableMap map[string]*common.TableSource
}

func (a *MySQLTableAnalyzer) AnalyzeSplitter(ctx context.Context, table *common.TableDiff, startRange *splitter.RangeInfo) (splitter.ChunkIterator, error) {
	chunkSize := 1000
	originSchema, originTable := getOriginTable(a.sourceTableMap, table)
	table.Schema = originSchema
	table.Table = originTable
	// use random splitter if we cannot use bucket splitter, then we can simply choose target table to generate chunks.
	randIter, err := splitter.NewRandomIteratorWithCheckpoint(ctx, table, a.dbConn, chunkSize, startRange)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return randIter, nil
}

func NewMySQLSource(ctx context.Context, tableDiffs []*common.TableDiff, tableRouter *router.Table, dbConn *sql.DB) (Source, error) {
	sourceTableMap, err := getSourceTableMap(ctx, tableDiffs, tableRouter, dbConn)
	if err != nil {
		return nil, errors.Trace(err)
	}
	ts := &MySQLSource{
		BasicSource{
			sourceTableMap: sourceTableMap,
			tableDiffs:     tableDiffs,
			dbConn: dbConn,
		},
	}
	return ts, nil
}
