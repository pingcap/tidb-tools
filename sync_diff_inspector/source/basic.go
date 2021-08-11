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
	router "github.com/pingcap/tidb-tools/pkg/table-router"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb-tools/pkg/dbutil"
	"github.com/pingcap/tidb-tools/sync_diff_inspector/source/common"
	"github.com/pingcap/tidb-tools/sync_diff_inspector/splitter"
	"github.com/pingcap/tidb-tools/sync_diff_inspector/utils"
	"go.uber.org/zap"
)

// BasicSource is the basic source for single MySQL/TiDB.
type BasicSource struct {
	tableDiffs     []*common.TableDiff
	sourceTableMap map[string]*common.SourceTable
}

func getSourceMap(ctx context.Context, tableDiffs []*common.TableDiff, tableRouter *router.Table, dbConn *sql.DB) (map[string]*common.SourceTable, error) {
	// we should get the real table name
	// and real table row query from source.
	uniqueMap := make(map[string]struct{})
	for _, tableDiff := range tableDiffs {
		uniqueMap[utils.UniqueID(tableDiff.Schema, tableDiff.Table)] = struct{}{}
	}

	sourceTableMap := make(map[string]*common.SourceTable)
	// instance -> db -> table
	allTablesMap := make(map[string]map[string]interface{})
	sourceSchemas, err := dbutil.GetSchemas(ctx, dbConn)
	if err != nil {
		return nil, errors.Annotatef(err, "get schemas from database")
	}

	for _, schema := range sourceSchemas {
		allTables, err := dbutil.GetTables(ctx, dbConn, schema)
		if err != nil {
			return nil, errors.Annotatef(err, "get tables from %s", schema)
		}
		allTablesMap[schema] = utils.SliceToMap(allTables)
	}

	for schema, allTables := range allTablesMap {
		for table := range allTables {
			targetSchema, targetTable := schema, table
			if tableRouter != nil {
				targetSchema, targetTable, err = tableRouter.Route(schema, table)
				if err != nil {
					return nil, errors.Errorf("get route result for %s.%s failed, error %v", schema, table, err)
				}
			}
			uniqueId := utils.UniqueID(targetSchema, targetTable)
			if _, ok := uniqueMap[uniqueId]; ok {
				sourceTableMap[uniqueId] = &common.SourceTable{
					OriginSchema: schema,
					OriginTable:  table,
					DBConn:       dbConn,
				}
			}
		}
	}

	return sourceTableMap, nil
}

func (s *BasicSource) GetRangeIterator(ctx context.Context, r *splitter.RangeInfo, analyzer TableAnalyzer) (RangeIterator, error) {
	dbIter := &BasicChunksIterator{
		currentID:      0,
		tableAnalyzer:  analyzer,
		TableDiffs:     s.tableDiffs,
		nextTableIndex: 0,
		limit:          0,
	}
	err := dbIter.nextTable(ctx, r)
	return dbIter, err
}

func (s *BasicSource) Close() {
	for _, st := range s.sourceTableMap {
		st.DBConn.Close()
	}
}

func (s *BasicSource) GetCountAndCrc32(ctx context.Context, tableRange *splitter.RangeInfo, checksumInfoCh chan *ChecksumInfo) {
	beginTime := time.Now()
	table := s.tableDiffs[tableRange.GetTableIndex()]
	chunk := tableRange.GetChunk()

	uniqueID := utils.UniqueID(table.Schema, table.Table)
	dbConn := s.sourceTableMap[uniqueID].DBConn
	originTable := s.sourceTableMap[uniqueID].OriginTable
	originSchema := s.sourceTableMap[uniqueID].OriginSchema

	count, checksum, err := utils.GetCountAndCRC32Checksum(ctx, dbConn, originSchema, originTable, table.Info, chunk.Where, utils.StringsToInterfaces(chunk.Args))
	cost := time.Since(beginTime)
	checksumInfoCh <- &ChecksumInfo{
		Checksum: checksum,
		Count:    count,
		Err:      err,
		Cost:     cost,
	}
}

func (s *BasicSource) GetTables() []*common.TableDiff {
	return s.tableDiffs
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

	table := s.tableDiffs[tableRange.GetTableIndex()]
	uniqueID := utils.UniqueID(table.Schema, table.Table)
	dbConn := s.sourceTableMap[uniqueID].DBConn
	originTable := s.sourceTableMap[uniqueID].OriginTable
	originSchema := s.sourceTableMap[uniqueID].OriginSchema
	rowsQuery, _ := utils.GetTableRowsQueryFormat(originSchema, originTable, table.Info, table.Collation)
	query := fmt.Sprintf(rowsQuery, chunk.Where)

	log.Debug("select data", zap.String("sql", query), zap.Reflect("args", args))
	rows, err := dbConn.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &BasicRowsIterator{
		rows,
	}, nil
}

func (s *BasicSource) GetDB() *sql.DB {
	for _, st := range s.sourceTableMap {
		return st.DBConn
	}
	return nil
}

// BasicChunksIterator is used for single mysql/tidb source.
type BasicChunksIterator struct {
	currentID     int
	tableAnalyzer TableAnalyzer

	TableDiffs     []*common.TableDiff
	nextTableIndex int

	limit  int
	dbConn *sql.DB

	tableIter splitter.ChunkIterator
}

func (t *BasicChunksIterator) Next(ctx context.Context) (*splitter.RangeInfo, error) {
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
		c.ID = t.currentID
		t.currentID++
		return &splitter.RangeInfo{
			ID:         c.ID,
			ChunkRange: c,
			TableIndex: curIndex,
			IndexID:    t.getCurTableIndexID(),
		}, nil
	}
	err = t.nextTable(ctx, nil)
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
	c.ID = t.currentID
	t.currentID++
	return &splitter.RangeInfo{
		ID:         c.ID,
		ChunkRange: c,
		TableIndex: curIndex,
		IndexID:    t.getCurTableIndexID(),
	}, nil
}

func (t *BasicChunksIterator) Close() {
	if t.tableIter != nil {
		t.tableIter.Close()
	}
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
func (t *BasicChunksIterator) nextTable(ctx context.Context, startRange *splitter.RangeInfo) error {
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

	chunkIter, err := t.tableAnalyzer.AnalyzeSplitter(ctx, curTable, startRange)
	if err != nil {
		return errors.Trace(err)
	}
	if t.tableIter != nil {
		t.tableIter.Close()
	}
	t.tableIter = chunkIter
	return nil
}
