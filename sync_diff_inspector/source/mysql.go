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
	"fmt"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb-tools/pkg/dbutil"
	"github.com/pingcap/tidb-tools/sync_diff_inspector/source/common"
	"github.com/pingcap/tidb-tools/sync_diff_inspector/splitter"
	"github.com/pingcap/tidb-tools/sync_diff_inspector/utils"
	"go.uber.org/zap"
	"time"

	"github.com/pingcap/tidb-tools/sync_diff_inspector/config"
)

type MySQLSource struct {
	tableDiffs []*common.TableDiff
	tableRows  []*TableRows
	dbConn     *sql.DB
}

func (s *MySQLSource) Close() {
	s.dbConn.Close()
}

func (s *MySQLSource) GenerateChunksIterator(r *splitter.RangeInfo) (DBIterator, error) {
	// TODO build Iterator with config.
	dbIter := &TiDBChunksIterator{
		TableDiffs:     s.tableDiffs,
		nextTableIndex: 0,
		limit:          0,
		dbConn:         s.dbConn,
	}
	err := dbIter.nextTable(r)
	return dbIter, err
}

func (s *MySQLSource) GetCrc32(ctx context.Context, tableRange *splitter.RangeInfo, checksumInfoCh chan *ChecksumInfo) {
	beginTime := time.Now()
	table := s.tableDiffs[tableRange.GetTableIndex()]
	chunk := tableRange.GetChunk()
	checksum, err := dbutil.GetCRC32Checksum(ctx, s.dbConn, table.Schema, table.Table, table.Info, chunk.Where, utils.StringsToInterfaces(chunk.Args))
	cost := time.Since(beginTime)

	checksumInfoCh <- &ChecksumInfo{
		Checksum: checksum,
		Err:      err,
		Cost:     cost,
	}
}

func (s *MySQLSource) GetOrderKeyCols(tableIndex int) []*model.ColumnInfo {
	return s.tableRows[tableIndex].tableOrderKeyCols
}

func (s *MySQLSource) GenerateReplaceDML(data map[string]*dbutil.ColumnData, tableIndex int) string {
	return utils.GenerateReplaceDML(data, s.tableDiffs[tableIndex].Info, s.tableDiffs[tableIndex].Schema)
}

func (s *MySQLSource) GenerateDeleteDML(data map[string]*dbutil.ColumnData, tableIndex int) string {
	return utils.GenerateDeleteDML(data, s.tableDiffs[tableIndex].Info, s.tableDiffs[tableIndex].Schema)
}

func (s *) GetRowsIterator(ctx context.Context, tableRange *splitter.RangeInfo) (RowDataIterator, error) {
	chunk := tableRange.GetChunk()
	args := utils.StringsToInterfaces(chunk.Args)

	query := fmt.Sprintf(s.tableRows[tableRange.GetTableIndex()].tableRowsQuery, chunk.Where)

	log.Debug("select data", zap.String("sql", query), zap.Reflect("args", args))
	rows, err := s.dbConn.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &TiDBRowsIterator{
		rows,
	}, nil
}

func NewMySQLSource(ctx context.Context, tableDiffs []*common.TableDiff, dbConn *sql.DB) (Source, error) {
	ts := &TiDBSource{
		tableDiffs: tableDiffs,
		tableRows:  make([]*TableRows, 0, len(tableDiffs)),
		dbConn:     dbConn,
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

func NewMySQLSources(ctx context.Context, tableDiffs [][]config.TableInstance, sourceDBs map[string]*sql.DB) (Source, error) {
	return nil, nil
}

