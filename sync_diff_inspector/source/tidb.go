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

	"github.com/coreos/go-semver/semver"
	tableFilter "github.com/pingcap/tidb-tools/pkg/table-filter"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb-tools/pkg/dbutil"
	"github.com/pingcap/tidb-tools/pkg/filter"
	"github.com/pingcap/tidb-tools/sync_diff_inspector/config"
	"github.com/pingcap/tidb-tools/sync_diff_inspector/source/common"
	"github.com/pingcap/tidb-tools/sync_diff_inspector/splitter"
	"github.com/pingcap/tidb-tools/sync_diff_inspector/utils"
	"github.com/pingcap/tidb/pkg/meta/model"
	"go.uber.org/zap"
)

type TiDBTableAnalyzer struct {
	dbConn            *sql.DB
	bucketSpliterPool *utils.WorkerPool
	sourceTableMap    map[string]*common.TableSource
}

func (a *TiDBTableAnalyzer) AnalyzeSplitter(ctx context.Context, table *common.TableDiff, startRange *splitter.RangeInfo) (splitter.ChunkIterator, error) {
	matchedSource := getMatchSource(a.sourceTableMap, table)
	// Shallow Copy
	originTable := *table
	originTable.Schema = matchedSource.OriginSchema
	originTable.Table = matchedSource.OriginTable
	progressID := dbutil.TableName(table.Schema, table.Table)
	// if we decide to use bucket to split chunks
	// we always use bucksIter even we load from checkpoint is not bucketNode
	// TODO check whether we can use bucket for this table to split chunks.
	// NOTICE: If checkpoint use random splitter, it will also fail the next time build bucket splitter.
	bucketIter, err := splitter.NewBucketIteratorWithCheckpoint(ctx, progressID, &originTable, a.dbConn, startRange, a.bucketSpliterPool)
	if err == nil {
		return bucketIter, nil
	}
	log.Info("failed to build bucket iterator, fall back to use random iterator", zap.Error(err))
	// fall back to random splitter

	// use random splitter if we cannot use bucket splitter, then we can simply choose target table to generate chunks.
	randIter, err := splitter.NewRandomIteratorWithCheckpoint(ctx, progressID, &originTable, a.dbConn, startRange)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return randIter, nil

}

type TiDBRowsIterator struct {
	rows *sql.Rows
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

type TiDBSource struct {
	tableDiffs     []*common.TableDiff
	sourceTableMap map[string]*common.TableSource
	snapshot       string
	sqlHint        string
	// bucketSpliterPool is the shared pool to produce chunks using bucket
	bucketSpliterPool *utils.WorkerPool
	dbConn            *sql.DB

	version *semver.Version
}

func (s *TiDBSource) GetTableAnalyzer() TableAnalyzer {
	return &TiDBTableAnalyzer{
		s.dbConn,
		s.bucketSpliterPool,
		s.sourceTableMap,
	}
}

func getMatchSource(sourceTableMap map[string]*common.TableSource, table *common.TableDiff) *common.TableSource {
	if len(sourceTableMap) == 0 {
		// no sourceTableMap, return the origin table name
		return &common.TableSource{
			OriginSchema: table.Schema,
			OriginTable:  table.Table,
		}
	}
	uniqueID := utils.UniqueID(table.Schema, table.Table)
	return sourceTableMap[uniqueID]
}

func (s *TiDBSource) GetRangeIterator(ctx context.Context, r *splitter.RangeInfo, analyzer TableAnalyzer, splitThreadCount int) (RangeIterator, error) {
	return NewChunksIterator(ctx, analyzer, s.tableDiffs, r, splitThreadCount)
}

func (s *TiDBSource) Close() {
	s.dbConn.Close()
}
func (s *TiDBSource) GetCountAndMd5(ctx context.Context, tableRange *splitter.RangeInfo) *ChecksumInfo {
	beginTime := time.Now()
	table := s.tableDiffs[tableRange.GetTableIndex()]
	chunk := tableRange.GetChunk()

	matchSource := getMatchSource(s.sourceTableMap, table)
	indexHint := ""
	if s.sqlHint == "auto" && len(chunk.IndexColumnNames) > 0 {
		// Since the index name is extracted from one data source,
		// while another data source may have an index with same columns and different index name, which can pass the struct check,
		// here we use the index column names to find the match index name again.
		//
		// For example:
		//     Upstream:   idx1(c1, c2)
		// 	   Downstream: idx2(c1, c2)
		// Here we will store [c1, c2] from idx1 in the chunk, and use them to get idx2 as index hint.
		if tableInfos, err := s.GetSourceStructInfo(ctx, tableRange.GetTableIndex()); err == nil {
			for _, index := range dbutil.FindAllIndex(tableInfos[0]) {
				if utils.IsIndexMatchingColumns(index, chunk.IndexColumnNames) {
					indexHint = fmt.Sprintf("/*+ USE_INDEX(`%s`.`%s`, `%s`) */",
						matchSource.OriginSchema,
						matchSource.OriginTable,
						index.Name.L,
					)
					break
				}
			}
		}
	}

	count, checksum, err := utils.GetCountAndMd5Checksum(
		ctx, s.dbConn, matchSource.OriginSchema, matchSource.OriginTable, table.Info,
		chunk.Where, indexHint, chunk.Args)

	cost := time.Since(beginTime)
	return &ChecksumInfo{
		Checksum: checksum,
		Count:    count,
		Err:      err,
		Cost:     cost,
	}
}

func (s *TiDBSource) GetCountForLackTable(ctx context.Context, tableRange *splitter.RangeInfo) int64 {
	table := s.tableDiffs[tableRange.GetTableIndex()]
	matchSource := getMatchSource(s.sourceTableMap, table)
	if matchSource != nil {
		count, _ := dbutil.GetRowCount(ctx, s.dbConn, matchSource.OriginSchema, matchSource.OriginTable, "", nil)
		return count
	}
	return 0
}

func (s *TiDBSource) GetTables() []*common.TableDiff {
	return s.tableDiffs
}

func (s *TiDBSource) GetSourceStructInfo(ctx context.Context, tableIndex int) ([]*model.TableInfo, error) {
	var err error
	tableInfos := make([]*model.TableInfo, 1)
	tableDiff := s.GetTables()[tableIndex]
	source := getMatchSource(s.sourceTableMap, tableDiff)
	tableInfos[0], err = dbutil.GetTableInfoWithVersion(ctx, s.GetDB(), source.OriginSchema, source.OriginTable, s.version)
	if err != nil {
		return nil, errors.Trace(err)
	}
	tableInfos[0], _ = utils.ResetColumns(tableInfos[0], tableDiff.IgnoreColumns)
	return tableInfos, nil
}

func (s *TiDBSource) GenerateFixSQL(t DMLType, upstreamData, downstreamData map[string]*dbutil.ColumnData, tableIndex int) string {
	if t == Insert {
		return utils.GenerateReplaceDML(upstreamData, s.tableDiffs[tableIndex].Info, s.tableDiffs[tableIndex].Schema)
	}
	if t == Delete {
		return utils.GenerateDeleteDML(downstreamData, s.tableDiffs[tableIndex].Info, s.tableDiffs[tableIndex].Schema)
	}
	if t == Replace {
		return utils.GenerateReplaceDMLWithAnnotation(upstreamData, downstreamData, s.tableDiffs[tableIndex].Info, s.tableDiffs[tableIndex].Schema)
	}
	log.Fatal("Don't support this type", zap.Any("dml type", t))
	return ""
}

func (s *TiDBSource) GetRowsIterator(ctx context.Context, tableRange *splitter.RangeInfo) (RowDataIterator, error) {
	chunk := tableRange.GetChunk()

	table := s.tableDiffs[tableRange.GetTableIndex()]
	matchedSource := getMatchSource(s.sourceTableMap, table)
	rowsQuery, _ := utils.GetTableRowsQueryFormat(matchedSource.OriginSchema, matchedSource.OriginTable, table.Info, table.Collation)
	query := fmt.Sprintf(rowsQuery, chunk.Where)

	log.Debug("select data", zap.String("sql", query), zap.Reflect("args", chunk.Args))
	rows, err := s.dbConn.QueryContext(ctx, query, chunk.Args...)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &TiDBRowsIterator{
		rows,
	}, nil
}

func (s *TiDBSource) GetDB() *sql.DB {
	return s.dbConn
}

func (s *TiDBSource) GetSnapshot() string {
	return s.snapshot
}

func NewTiDBSource(ctx context.Context, tableDiffs []*common.TableDiff, ds *config.DataSource, bucketSpliterPool *utils.WorkerPool, f tableFilter.Filter, skipNonExistingTable bool) (Source, error) {
	sourceTableMap := make(map[string]*common.TableSource)
	log.Info("find router for tidb source")
	// we should get the real table name
	// and real table row query from source.
	targetUniqueTableMap := make(map[string]struct{})
	for _, tableDiff := range tableDiffs {
		targetUniqueTableMap[utils.UniqueID(tableDiff.Schema, tableDiff.Table)] = struct{}{}
	}
	sourceTablesAfterRoute := make(map[string]struct{})

	// instance -> db -> table
	allTablesMap := make(map[string]map[string]interface{})
	sourceSchemas, err := dbutil.GetSchemas(ctx, ds.Conn)

	if err != nil {
		return nil, errors.Annotatef(err, "get schemas from database")
	}

	for _, schema := range sourceSchemas {
		if filter.IsSystemSchema(schema) {
			// ignore system schema
			continue
		}
		allTables, err := dbutil.GetTables(ctx, ds.Conn, schema)
		if err != nil {
			return nil, errors.Annotatef(err, "get tables from %s", schema)
		}
		allTablesMap[schema] = utils.SliceToMap(allTables)
	}

	for schema, allTables := range allTablesMap {
		for table := range allTables {
			targetSchema, targetTable := schema, table
			if ds.Router != nil {
				targetSchema, targetTable, err = ds.Router.Route(schema, table)
				if err != nil {
					return nil, errors.Errorf("get route result for %s.%s failed, error %v", schema, table, err)
				}
			}

			uniqueId := utils.UniqueID(targetSchema, targetTable)
			isMatched := f.MatchTable(targetSchema, targetTable)
			if isMatched {
				// if match the filter, we should respect it and check target has this table later.
				sourceTablesAfterRoute[uniqueId] = struct{}{}
			}
			if _, ok := targetUniqueTableMap[uniqueId]; ok || (isMatched && skipNonExistingTable) {
				if _, ok := sourceTableMap[uniqueId]; ok {
					log.Error("TiDB source don't support compare multiple source tables with one downstream table," +
						" if this happening when diff on same instance is fine. otherwise we are not guarantee this diff result is right")
				}
				sourceTableMap[uniqueId] = &common.TableSource{
					OriginSchema: schema,
					OriginTable:  table,
				}
			}
		}
	}

	tableDiffs, err = checkTableMatched(tableDiffs, targetUniqueTableMap, sourceTablesAfterRoute, skipNonExistingTable)
	if err != nil {
		return nil, errors.Annotatef(err, "please make sure the filter is correct.")
	}
	ts := &TiDBSource{
		tableDiffs:        tableDiffs,
		sourceTableMap:    sourceTableMap,
		snapshot:          ds.Snapshot,
		dbConn:            ds.Conn,
		bucketSpliterPool: bucketSpliterPool,
		version:           utils.TryToGetVersion(ctx, ds.Conn),
		sqlHint:           ds.SQLHintUseIndex,
	}
	return ts, nil
}
