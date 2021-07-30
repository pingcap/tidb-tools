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
	"github.com/pingcap/log"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb-tools/pkg/dbutil"
	"github.com/pingcap/tidb-tools/sync_diff_inspector/source/common"
	"github.com/pingcap/tidb-tools/sync_diff_inspector/splitter"
	"github.com/pingcap/tidb-tools/sync_diff_inspector/utils"
	"go.uber.org/zap"
	"time"
)

// BasicSource is the basic source for single MySQL/TiDB.
type BasicSource struct {
	tableDiffs []*common.TableDiff
	tableRows  []*TableRows
	dbConn     *sql.DB
}

func (s *BasicSource) GetDBIter(r *splitter.RangeInfo, analyzer TableIter) (DBIterator, error) {
	dbIter := &BasicChunksIterator{
		tableAnalyzer:  analyzer,
		TableDiffs:     s.tableDiffs,
		nextTableIndex: 0,
		limit:          0,
		dbConn:         s.dbConn,
	}
	err := dbIter.nextTable(r)
	return dbIter, err
}

func (s *BasicSource) Close() {
	s.dbConn.Close()
}

func (s *BasicSource) GetCrc32(ctx context.Context, tableRange *splitter.RangeInfo, checksumInfoCh chan *ChecksumInfo) {
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

func (s *BasicSource) GetOrderKeyCols(tableIndex int) []*model.ColumnInfo {
	return s.tableRows[tableIndex].tableOrderKeyCols
}

func (s *BasicSource) GenerateReplaceDML(data map[string]*dbutil.ColumnData, tableIndex int) string {
	return utils.GenerateReplaceDML(data, s.tableDiffs[tableIndex].Info, s.tableDiffs[tableIndex].Schema)
}

func (s *BasicSource) GenerateDeleteDML(data map[string]*dbutil.ColumnData, tableIndex int) string {
	return utils.GenerateDeleteDML(data, s.tableDiffs[tableIndex].Info, s.tableDiffs[tableIndex].Schema)
}

func (s *BasicSource) GetRowsIterator(ctx context.Context, tableRange *splitter.RangeInfo) (RowDataIterator, error) {
	chunk := tableRange.GetChunk()
	args := utils.StringsToInterfaces(chunk.Args)

	query := fmt.Sprintf(s.tableRows[tableRange.GetTableIndex()].tableRowsQuery, chunk.Where)

	log.Debug("select data", zap.String("sql", query), zap.Reflect("args", args))
	rows, err := s.dbConn.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &RowsIterator{
		rows,
	}, nil
}

func (s *BasicSource) GetDB() *sql.DB {
	return s.dbConn
}

// BasicChunksIterator is used for single mysql/tidb source.
type BasicChunksIterator struct {
	tableAnalyzer TableIter

	TableDiffs     []*common.TableDiff
	nextTableIndex int

	limit  int
	dbConn *sql.DB

	tableIter splitter.TableIterator
}

func (t *BasicChunksIterator) Next() (*splitter.RangeInfo, error) {
	// TODO: creates different tables chunks in parallel
	if t.tableIter == nil {
		return nil, nil
	}
	c, err := t.tableIter.Next()
	if err != nil {
		return nil, errors.Trace(err)
	}

	if c != nil {
		curIndex := t.getCurTableIndex()
		return &splitter.RangeInfo{
			ChunkRange: c,
			TableIndex: curIndex,
			IndexID:    t.getCurTableIndexID(),
		}, nil
	}
	err = t.nextTable(nil)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if t.tableIter == nil {
		return nil, nil
	}
	c, err = t.tableIter.Next()
	if err != nil {
		return nil, errors.Trace(err)
	}
	curIndex := t.getCurTableIndex()
	return &splitter.RangeInfo{
		ChunkRange: c,
		TableIndex: curIndex,
		IndexID:    t.getCurTableIndexID(),
	}, nil
}

func (t *BasicChunksIterator) Close() {
	t.tableIter.Close()
}

func (t *BasicChunksIterator) getCurTableIndex() int {
	return t.nextTableIndex - 1
}

func (t *BasicChunksIterator) getCurTableIndexID() int64 {
	if bt, ok := t.tableIter.(*splitter.BucketIterator); ok {
		return bt.GetIndexID()
	}
	return 0
}

// if error is nil and t.iter is not nil,
// then nextTable is done successfully.
func (t *BasicChunksIterator) nextTable(startRange *splitter.RangeInfo) error {
	if t.nextTableIndex >= len(t.TableDiffs) {
		t.tableIter = nil
		return nil
	}
	curTable := t.TableDiffs[t.nextTableIndex]
	t.nextTableIndex++

	// reads table index from checkpoint at the beginning
	if startRange != nil {
		curIndex := startRange.GetTableIndex()
		curTable = t.TableDiffs[curIndex]
		t.nextTableIndex = curIndex + 1
	}

	chunkIter, err := t.tableAnalyzer.GetIterForTable(curTable, startRange)
	if err != nil {
		return errors.Trace(err)
	}
	if t.tableIter != nil {
		t.tableIter.Close()
	}
	t.tableIter = chunkIter
	return nil
}

func (t *BasicChunksIterator) analyzeChunkSize(table *common.TableDiff) (int64, error) {
	return dbutil.GetRowCount(context.Background(), t.dbConn, table.Schema, table.Table, table.Range, nil)
	/*
		if err != nil {
			return 0, errors.Trace(err)
		}
	*/
	// TODO analyze table
}
