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

// TiDBChunksIterator iterate chunks in tables sequence
type TiDBChunksIterator struct {
	TableDiffs     []*common.TableDiff
	nextTableIndex int

	limit int

	dbConn *sql.DB

	iter splitter.Iterator
}

func (t *TiDBChunksIterator) Next() (*splitter.RangeInfo, error) {
	// TODO: creates different tables chunks in parallel
	if t.iter == nil {
		return nil, nil
	}
	c, err := t.iter.Next()
	if err != nil {
		return nil, errors.Trace(err)
	}

	if c != nil {
		curIndex := t.getCurTableIndex()
		schema := t.TableDiffs[curIndex].Schema
		table := t.TableDiffs[curIndex].Table
		return &splitter.RangeInfo{
			ChunkRange: c,
			TableIndex: curIndex,
			Schema:     schema,
			Table:      table,
			IndexID:    t.getCurTableIndexID(),
		}, nil
	}
	err = t.nextTable(nil)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if t.iter == nil {
		return nil, nil
	}
	c, err = t.iter.Next()
	if err != nil {
		return nil, errors.Trace(err)
	}
	curIndex := t.getCurTableIndex()
	schema := t.TableDiffs[curIndex].Schema
	table := t.TableDiffs[curIndex].Table
	return &splitter.RangeInfo{
		ChunkRange: c,
		TableIndex: curIndex,
		Schema:     schema,
		Table:      table,
		IndexID:    t.getCurTableIndexID(),
	}, nil
}

func (t *TiDBChunksIterator) Close() {
	t.iter.Close()
}

func (t *TiDBChunksIterator) getCurTableIndex() int {
	return t.nextTableIndex - 1
}

func (t *TiDBChunksIterator) getCurTableIndexID() int64 {
	if bt, ok := t.iter.(*splitter.BucketIterator); ok {
		return bt.GetIndexID()
	}
	return 0
}

// if error is nil and t.iter is not nil,
// then nextTable is done successfully.
func (t *TiDBChunksIterator) nextTable(startRange *splitter.RangeInfo) error {
	if t.nextTableIndex >= len(t.TableDiffs) {
		t.iter = nil
		return nil
	}
	if startRange != nil {
		for i, tableDiff := range t.TableDiffs {
			if tableDiff.Schema == startRange.GetSchema() && tableDiff.Table == startRange.GetTable() {
				t.nextTableIndex = i + 1
			}
		}
	}
	curTable := t.TableDiffs[t.nextTableIndex]
	t.nextTableIndex++
	chunkIter, err := t.splitChunksForTable(curTable, startRange)
	if err != nil {
		return errors.Trace(err)
	}
	if t.iter != nil {
		t.iter.Close()
	}
	t.iter = chunkIter
	return nil
}

// useBucket returns the tableInstance that can use bucket info whether in source or target.
func (t *TiDBChunksIterator) useBucket(diff *common.TableDiff) bool {
	// TODO check whether we can use bucket for this table to split chunks.
	return true
}

func (t *TiDBChunksIterator) analyzeChunkSize(table *common.TableDiff) (int64, error) {
	return dbutil.GetRowCount(context.Background(), t.dbConn, table.Schema, table.Table, table.Range, nil)
	/*
		if err != nil {
			return 0, errors.Trace(err)
		}
	*/
	// TODO analyze table

}

func (t *TiDBChunksIterator) splitChunksForTable(tableDiff *common.TableDiff, startRange *splitter.RangeInfo) (splitter.Iterator, error) {
	// 1_000, 2_000, 4_000, 8_000, 16_000, 32_000, 64_000
	chunkSize := 1000

	// if we decide to use bucket to split chunks
	// we always use bucksIter even we load from checkpoint is not bucketNode
	if t.useBucket(tableDiff) {
		bucketIter, err := splitter.NewBucketIteratorWithCheckpoint(tableDiff, t.dbConn, chunkSize, startRange)
		if err == nil {
			return bucketIter, nil
		}
		log.Warn("build bucketIter failed", zap.Error(err))
		// fall back to random splitter
	}

	// use random splitter if we cannot use bucket splitter, then we can simply choose target table to generate chunks.
	randIter, err := splitter.NewRandomIteratorWithCheckpoint(tableDiff, t.dbConn, chunkSize, startRange)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return randIter, nil
}

type TableRows struct {
	tableRowsQuery    string
	tableOrderKeyCols []*model.ColumnInfo
}

type TiDBSource struct {
	tableDiffs []*common.TableDiff
	tableRows  []*TableRows
	dbConn     *sql.DB
}

func NewTiDBSource(ctx context.Context, tableDiffs []*common.TableDiff, dbConn *sql.DB) (Source, error) {
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

func (s *TiDBSource) Close() {
	s.dbConn.Close()
}

func (s *TiDBSource) GetTable(i int) *common.TableDiff {
	return s.tableDiffs[i]
}

func (s *TiDBSource) GetDB() *sql.DB {
	return s.dbConn
}

func (s *TiDBSource) GenerateChunksIterator(r *splitter.RangeInfo) (DBIterator, error) {
	dbIter := &TiDBChunksIterator{
		TableDiffs:     s.tableDiffs,
		nextTableIndex: 0,
		limit:          0,
		dbConn:         s.dbConn,
	}
	err := dbIter.nextTable(r)
	return dbIter, err
}

func (s *TiDBSource) GetCrc32(ctx context.Context, tableRange *splitter.RangeInfo, checksumInfoCh chan *ChecksumInfo) {
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

func (s *TiDBSource) GetOrderKeyCols(tableIndex int) []*model.ColumnInfo {
	return s.tableRows[tableIndex].tableOrderKeyCols
}

func (s *TiDBSource) GenerateReplaceDML(data map[string]*dbutil.ColumnData, tableIndex int) string {
	return utils.GenerateReplaceDML(data, s.tableDiffs[tableIndex].Info, s.tableDiffs[tableIndex].Schema)
}

func (s *TiDBSource) GenerateDeleteDML(data map[string]*dbutil.ColumnData, tableIndex int) string {
	return utils.GenerateDeleteDML(data, s.tableDiffs[tableIndex].Info, s.tableDiffs[tableIndex].Schema)
}

type TiDBRowsIterator struct {
	rows *sql.Rows
}

func (s *TiDBSource) GetRowsIterator(ctx context.Context, tableRange *splitter.RangeInfo) (RowDataIterator, error) {
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

func (s *TiDBRowsIterator) Close() {
	s.rows.Close()
}

func (s *TiDBRowsIterator) Next() (map[string]*dbutil.ColumnData, error) {
	if s.rows.Next() {
		return dbutil.ScanRow(s.rows)
	}
	return nil, nil
}

func (s *TiDBRowsIterator) GenerateFixSQL(t DMLType) (string, error) {
	return "", nil
}
