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
	"container/heap"
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb-tools/pkg/filter"
	router "github.com/pingcap/tidb-tools/pkg/table-router"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb-tools/pkg/dbutil"
	"github.com/pingcap/tidb-tools/sync_diff_inspector/source/common"
	"github.com/pingcap/tidb-tools/sync_diff_inspector/splitter"
	"github.com/pingcap/tidb-tools/sync_diff_inspector/utils"
	"go.uber.org/zap"
)

type MySQLSources struct {
	tableDiffs []*common.TableDiff

	sourceTablesMap map[string][]*common.TableShardSource
}

func (s *MySQLSources) getMatchedSourcesForTable(table *common.TableDiff) []*common.TableShardSource {
	if s.sourceTablesMap == nil {
		log.Fatal("unreachable, source tables map shouldn't be nil.")
	}
	matchSources, ok := s.sourceTablesMap[utils.UniqueID(table.Schema, table.Table)]
	if !ok {
		log.Fatal("unreachable, no match source tables in mysql shard source.")
	}
	return matchSources
}

func (s *MySQLSources) GetTableAnalyzer() TableAnalyzer {
	log.Fatal("[UnReachable] we won't choose multi-mysql as work source to call GetTableAnalyzer")
	return nil
}

func (s *MySQLSources) GetRangeIterator(ctx context.Context, r *splitter.RangeInfo, analyzer TableAnalyzer) (RangeIterator, error) {
	log.Fatal("[UnReachable] we won't choose multi-mysql as work source to call GetRangeIterator")
	return nil, nil
}

func (s *MySQLSources) Close() {
	for _, t := range s.sourceTablesMap {
		for _, db := range t {
			db.DBConn.Close()
		}
	}
}

func (s *MySQLSources) GetCountAndCrc32(ctx context.Context, tableRange *splitter.RangeInfo, checksumInfoCh chan *ChecksumInfo) {
	beginTime := time.Now()
	table := s.tableDiffs[tableRange.GetTableIndex()]
	chunk := tableRange.GetChunk()

	matchSources := s.getMatchedSourcesForTable(table)
	infoCh := make(chan *ChecksumInfo, len(s.sourceTablesMap))

	for _, ms := range matchSources {
		go func(ms *common.TableShardSource) {
			count, checksum, err := utils.GetCountAndCRC32Checksum(ctx, ms.DBConn, ms.OriginSchema, ms.OriginTable, table.Info, chunk.Where, utils.StringsToInterfaces(chunk.Args))
			infoCh <- &ChecksumInfo{
				Checksum: checksum,
				Count:    count,
				Err:      err,
			}
		}(ms)
	}
	defer close(infoCh)

	var (
		err           error
		totalCount    int64
		totalChecksum int64
	)

	for i := 0; i < len(matchSources); i++ {
		info := <-infoCh
		// catch the first error
		if err == nil && info.Err != nil {
			err = info.Err
		}
		totalCount += info.Count
		totalChecksum ^= info.Checksum
	}

	cost := time.Since(beginTime)
	checksumInfoCh <- &ChecksumInfo{
		Checksum: totalChecksum,
		Count:    totalCount,
		Err:      err,
		Cost:     cost,
	}
}

func (s *MySQLSources) GetTables() []*common.TableDiff {
	return s.tableDiffs
}

func (s *MySQLSources) GenerateFixSQL(t DMLType, data map[string]*dbutil.ColumnData, tableIndex int) string {
	if t == Replace {
		return utils.GenerateReplaceDML(data, s.tableDiffs[tableIndex].Info, s.tableDiffs[tableIndex].Schema)
	}
	if t == Delete {
		return utils.GenerateDeleteDML(data, s.tableDiffs[tableIndex].Info, s.tableDiffs[tableIndex].Schema)
	}
	log.Fatal("Don't support this type", zap.Any("dml type", t))
	return ""
}

func (s *MySQLSources) GenerateFixSQLWithAnnotation(t DMLType, source, target map[string]*dbutil.ColumnData, tableIndex int) string {
	if t == Replace {
		return utils.GenerateReplaceDMLWithAnnotation(source, target, s.tableDiffs[tableIndex].Info, s.tableDiffs[tableIndex].Schema)
	}
	log.Fatal("Don't support this type", zap.Any("dml type", t))
	return ""
}

func (s *MySQLSources) GetRowsIterator(ctx context.Context, tableRange *splitter.RangeInfo) (RowDataIterator, error) {
	chunk := tableRange.GetChunk()
	args := utils.StringsToInterfaces(chunk.Args)

	sourceRows := make(map[int]*sql.Rows)

	table := s.tableDiffs[tableRange.GetTableIndex()]
	matchSources := s.getMatchedSourcesForTable(table)

	var rowsQuery string
	var orderKeyCols []*model.ColumnInfo
	for i, ms := range matchSources {
		rowsQuery, orderKeyCols = utils.GetTableRowsQueryFormat(ms.OriginSchema, ms.OriginTable, table.Info, table.Collation)
		query := fmt.Sprintf(rowsQuery, chunk.Where)
		rows, err := ms.DBConn.QueryContext(ctx, query, args...)
		if err != nil {
			return nil, errors.Trace(err)
		}
		sourceRows[i] = rows
	}

	sourceRowDatas := &common.RowDatas{
		Rows:         make([]common.RowData, 0, len(sourceRows)),
		OrderKeyCols: orderKeyCols,
	}
	heap.Init(sourceRowDatas)
	// first push one row from all the sources into heap
	for source, sourceRow := range sourceRows {
		rowData, err := getRowData(sourceRow)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if rowData != nil {
			heap.Push(sourceRowDatas, common.RowData{
				Data:   rowData,
				Source: source,
			})
		} else {
			if sourceRow.Err() != nil {
				return nil, sourceRow.Err()
			}
		}
	}

	return &MultiSourceRowsIterator{
		sourceRows:     sourceRows,
		sourceRowDatas: sourceRowDatas,
	}, nil
}

func (s *MySQLSources) GetDB() *sql.DB {
	// return any of them is ok
	for _, st := range s.sourceTablesMap {
		for _, db := range st {
			return db.DBConn
		}
	}
	return nil
}

type MultiSourceRowsIterator struct {
	sourceRows     map[int]*sql.Rows
	sourceRowDatas *common.RowDatas
}

func getRowData(rows *sql.Rows) (rowData map[string]*dbutil.ColumnData, err error) {
	for rows.Next() {
		rowData, err = dbutil.ScanRow(rows)
		return
	}
	return
}

func (ms *MultiSourceRowsIterator) Next() (map[string]*dbutil.ColumnData, error) {
	// Before running getSourceRow, heap save one row from all the sources,
	// otherwise this source has read to the end. Each row should be the smallest in each source.
	// Once there is one row popped, we need to immediately push one row, which is from the same source, into the heap.
	// all the sources had read to the end, no data to return
	if len(ms.sourceRowDatas.Rows) == 0 {
		return nil, nil
	}
	rowData := heap.Pop(ms.sourceRowDatas).(common.RowData)
	newRowData, err := getRowData(ms.sourceRows[rowData.Source])
	if err != nil {
		return nil, err
	}
	if newRowData != nil {
		heap.Push(ms.sourceRowDatas, common.RowData{
			Data:   newRowData,
			Source: rowData.Source,
		})
	} else {
		if ms.sourceRows[rowData.Source].Err() != nil {
			return nil, ms.sourceRows[rowData.Source].Err()
		}
	}
	return rowData.Data, nil
}

func (ms *MultiSourceRowsIterator) Close() {
	for _, s := range ms.sourceRows {
		s.Close()
	}
}

func NewMySQLSources(ctx context.Context, tableDiffs []*common.TableDiff, tableRouter *router.Table, dbs []*sql.DB) (Source, error) {
	sourceTablesMap := make(map[string][]*common.TableShardSource)
	// we should get the real table name
	// and real table row query from sourceDB.
	uniqueMap := make(map[string]struct{})
	for _, tableDiff := range tableDiffs {
		uniqueMap[utils.UniqueID(tableDiff.Schema, tableDiff.Table)] = struct{}{}
	}

	for i, sourceDB := range dbs {
		sourceSchemas, err := dbutil.GetSchemas(ctx, sourceDB)
		if err != nil {
			return nil, errors.Annotatef(err, "get schemas from %d source", i)
		}

		for _, schema := range sourceSchemas {
			// Skip system schema.
			if filter.IsSystemSchema(schema) {
				continue
			}
			allTables, err := dbutil.GetTables(ctx, sourceDB, schema)
			if err != nil {
				return nil, errors.Annotatef(err, "get tables from %d source %s", i, schema)
			}
			for _, table := range allTables {
				targetSchema, targetTable := schema, table
				if tableRouter != nil {
					targetSchema, targetTable, err = tableRouter.Route(schema, table)
					if err != nil {
						return nil, errors.Errorf("get route result for %d source %s.%s failed, error %v", i, schema, table, err)
					}
				}
				uniqueId := utils.UniqueID(targetSchema, targetTable)
				if _, ok := uniqueMap[uniqueId]; !ok {
					sourceTablesMap[uniqueId] = make([]*common.TableShardSource, 0)
				}
				if _, ok := uniqueMap[uniqueId]; ok {
					sourceTablesMap[uniqueId] = append(sourceTablesMap[uniqueId], &common.TableShardSource{
						TableSource: common.TableSource{
							OriginSchema: schema,
							OriginTable:  table,
						},
						DBConn: sourceDB,
					})
				}
			}
		}
	}

	mss := &MySQLSources{
		tableDiffs:      tableDiffs,
		sourceTablesMap: sourceTablesMap,
	}
	return mss, nil
}
