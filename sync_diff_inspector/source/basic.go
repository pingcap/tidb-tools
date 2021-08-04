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
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb-tools/pkg/dbutil"
	"github.com/pingcap/tidb-tools/sync_diff_inspector/source/common"
	"github.com/pingcap/tidb-tools/sync_diff_inspector/splitter"
	"github.com/pingcap/tidb-tools/sync_diff_inspector/utils"
	"go.uber.org/zap"
)

// BasicSource is the basic source for single MySQL/TiDB.
type BasicSource struct {
	tableDiffs []*common.TableDiff
	dbConn     *sql.DB
}

func (s *BasicSource) GetRangeIterator(r *splitter.RangeInfo, analyzer TableAnalyzer) (RangeIterator, error) {
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
func (s *BasicSource) GetCountAndCrc32(ctx context.Context, tableRange *splitter.RangeInfo, countCh chan int64, checksumInfoCh chan *ChecksumInfo) {
	beginTime := time.Now()
	table := s.tableDiffs[tableRange.GetTableIndex()]
	chunk := tableRange.GetChunk()
	count, checksum, err := utils.GetCountAndCRC32Checksum(ctx, s.dbConn, table.Schema, table.Table, table.Info, chunk.Where, utils.StringsToInterfaces(chunk.Args))
	cost := time.Since(beginTime)
	if countCh != nil {
		countCh <- count
	}
	checksumInfoCh <- &ChecksumInfo{
		Checksum: checksum,
		Err:      err,
		Cost:     cost,
	}
}

func (s *BasicSource) GetTable(i int) *common.TableDiff {
	return s.tableDiffs[i]
}

func (s *BasicSource) GetOrderKeyCols(tableIndex int) []*model.ColumnInfo {
	return s.tableDiffs[tableIndex].TableOrderKeyCols
}

func (s *BasicSource) GenerateFixSQL(t DMLType, data map[string]*dbutil.ColumnData, tableIndex int) string {
	if t == Replace {
		return utils.GenerateReplaceDML(data, s.tableDiffs[tableIndex].Info, s.tableDiffs[tableIndex].Schema)
	}
	if t == Delete {
		return utils.GenerateDeleteDML(data, s.tableDiffs[tableIndex].Info, s.tableDiffs[tableIndex].Schema)
	}
	log.Fatal("Don't support this type", zap.Any("dml type", t))
	return ""
}

func (s *BasicSource) GetRowsIterator(ctx context.Context, tableRange *splitter.RangeInfo) (RowDataIterator, error) {
	chunk := tableRange.GetChunk()
	args := utils.StringsToInterfaces(chunk.Args)

	query := fmt.Sprintf(s.tableDiffs[tableRange.GetTableIndex()].TableRowsQuery, chunk.Where)

	log.Debug("select data", zap.String("sql", query), zap.Reflect("args", args))
	rows, err := s.dbConn.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &BasicRowsIterator{
		rows,
	}, nil
}

func (s *BasicSource) GetDB() *sql.DB {
	return s.dbConn
}

// BasicChunksIterator is used for single mysql/tidb source.
type BasicChunksIterator struct {
	tableAnalyzer TableAnalyzer

	TableDiffs     []*common.TableDiff
	nextTableIndex int

	limit  int
	dbConn *sql.DB

	tableIter splitter.ChunkIterator
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

	chunkIter, err := t.tableAnalyzer.AnalyzeSplitter(curTable, startRange)
	if err != nil {
		return errors.Trace(err)
	}
	if t.tableIter != nil {
		t.tableIter.Close()
	}
	t.tableIter = chunkIter
	return nil
}
