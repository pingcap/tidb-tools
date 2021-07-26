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
	"github.com/pingcap/tidb-tools/sync_diff_inspector/checkpoints"
	"github.com/pingcap/tidb-tools/sync_diff_inspector/config"
	"github.com/pingcap/tidb-tools/sync_diff_inspector/source/common"
	"github.com/pingcap/tidb-tools/sync_diff_inspector/splitter"
	"github.com/pingcap/tidb-tools/sync_diff_inspector/utils"
	"go.uber.org/zap"
)

// TiDBChunksIterator iterate chunks in tables sequence
type TiDBChunksIterator struct {
	TableDiffs     []*common.TableDiff
	nextTableIndex int

	chunkSize int
	limit     int

	dbConn *sql.DB

	iter splitter.Iterator
}

func (t *TiDBChunksIterator) Next() (*TableRange, error) {
	// TODO: creates different tables chunks in parallel
	if t.iter == nil {
		return nil, nil
	}
	chunks, err := t.iter.Next()

	if err != nil {
		return nil, errors.Trace(err)
	}

	if chunks != nil {
		return &TableRange{
			ChunkRange: chunks,
			TableIndex: t.getCurTableIndex(),
		}, nil
	}

	err = t.nextTable()
	if err != nil {
		return nil, errors.Trace(err)
	}
	if t.iter == nil {
		return nil, nil
	}
	chunks, err = t.iter.Next()
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &TableRange{
		ChunkRange: chunks,
		TableIndex: t.getCurTableIndex(),
	}, nil
}

func (t *TiDBChunksIterator) Close() {
	t.iter.Close()
}

func (t *TiDBChunksIterator) getCurTableIndex() int {
	return t.nextTableIndex - 1
}

// if error is nil and t.iter is not nil,
// then nextTable is done successfully.
func (t *TiDBChunksIterator) nextTable() error {
	if t.nextTableIndex >= len(t.TableDiffs) {
		t.iter = nil
		return nil
	}
	curTable := t.TableDiffs[t.nextTableIndex]
	t.nextTableIndex++
	chunkIter, err := t.splitChunksForTable(curTable)
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
func (s *TiDBChunksIterator) useBucket(diff *common.TableDiff) bool {
	// TODO check whether we can use bucket for this table to split chunks.
	return true
}

func (s *TiDBChunksIterator) splitChunksForTable(tableDiff *common.TableDiff) (splitter.Iterator, error) {
	chunkSize := 1000
	bucket := false
	var node checkpoints.Node
	if tableDiff.UseCheckpoint {
		// TODO error handling
		var err error
		node, err = checkpoints.LoadChunks()
		// TODO add warn log
		log.Warn("the checkpoint load failed, diable checkpoint")
		if err != nil {
			tableDiff.UseCheckpoint = false
		} else {
			switch node.(type) {
			case *checkpoints.BucketNode:
				bucket = true
			case *checkpoints.RandomNode:
				bucket = false
			}
		}
	}
	// TODO merge bucket function into useBucket()
	if (!tableDiff.UseCheckpoint && s.useBucket(tableDiff)) || bucket {
		bucketIter, err := splitter.NewBucketIterator(tableDiff, s.dbConn, chunkSize, node)
		if err != nil {
			return nil, errors.Trace(err)
		}

		return bucketIter, nil
		// TODO fall back to random splitter
	}
	// use random splitter if we cannot use bucket splitter, then we can simply choose target table to generate chunks.
	randIter, err := splitter.NewRandomIterator(tableDiff, s.dbConn, s.chunkSize, tableDiff.Range, tableDiff.Collation, node)

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

func NewTiDBSource(ctx context.Context, tableDiffs []*common.TableDiff, dbCfg *config.DBConfig) (Source, error) {
	// TODO build TiDB Source
	dbConn, err := common.CreateDB(ctx, &dbCfg.DBConfig, nil, 4)
	if err != nil {
		return nil, errors.Trace(err)
	}

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

func (s *TiDBSource) GenerateChunksIterator() (DBIterator, error) {
	// TODO build Iterator with config.
	dbIter := &TiDBChunksIterator{
		TableDiffs:     s.tableDiffs,
		nextTableIndex: 0,
		chunkSize:      0,
		limit:          0,
		dbConn:         s.dbConn,
	}
	err := dbIter.nextTable()
	return dbIter, err
}

func (s *TiDBSource) GetCrc32(ctx context.Context, tableChunk *TableRange, checksumInfoCh chan ChecksumInfo) {
	// TODO get crc32 with sql
	beginTime := time.Now()
	table := s.tableDiffs[tableChunk.TableIndex]
	checksum, err := dbutil.GetCRC32Checksum(ctx, s.dbConn, table.Schema, table.Table, table.Info, tableChunk.ChunkRange.Where, utils.StringsToInterfaces(tableChunk.ChunkRange.Args))
	cost := time.Since(beginTime)

	checksumInfoCh <- ChecksumInfo{
		Checksum: checksum,
		Err:      err,
		Cost:     cost,
	}
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

func (s *TiDBSource) GetRowsIterator(ctx context.Context, tableChunk *TableRange) (RowDataIterator, error) {
	// TODO get rowsdataIter with sql
	args := utils.StringsToInterfaces(tableChunk.ChunkRange.Args)

	query := fmt.Sprintf(s.tableRows[tableChunk.TableIndex].tableRowsQuery, tableChunk.ChunkRange.Where)

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
