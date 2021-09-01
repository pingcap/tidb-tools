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
	"github.com/pingcap/tidb-tools/pkg/filter"
	"github.com/pingcap/tidb-tools/sync_diff_inspector/config"
	"github.com/pingcap/tidb-tools/sync_diff_inspector/source/common"
	"github.com/pingcap/tidb-tools/sync_diff_inspector/splitter"
	"github.com/pingcap/tidb-tools/sync_diff_inspector/utils"
	"go.uber.org/zap"
)

type TiDBTableAnalyzer struct {
	dbConn         *sql.DB
	sourceTableMap map[string]*common.TableSource
}

func (a *TiDBTableAnalyzer) AnalyzeSplitter(ctx context.Context, progressID string, table *common.TableDiff, startRange *splitter.RangeInfo) (splitter.ChunkIterator, error) {
	matchedSource := getMatchSource(a.sourceTableMap, table)
	// Shallow Copy
	originTable := *table
	originTable.Schema = matchedSource.OriginSchema
	originTable.Table = matchedSource.OriginTable
	// if we decide to use bucket to split chunks
	// we always use bucksIter even we load from checkpoint is not bucketNode
	// TODO check whether we can use bucket for this table to split chunks.
	bucketIter, err := splitter.NewBucketIteratorWithCheckpoint(ctx, progressID, &originTable, a.dbConn, startRange)
	if err == nil {
		return bucketIter, nil
	}
	log.Warn("build bucketIter failed", zap.Error(err))
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
	dbConn         *sql.DB
}

func (s *TiDBSource) GetTableAnalyzer() TableAnalyzer {
	return &TiDBTableAnalyzer{
		s.dbConn,
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

func (s *TiDBSource) GetRangeIterator(ctx context.Context, r *splitter.RangeInfo, analyzer TableAnalyzer) (RangeIterator, error) {
	id := 0
	if r != nil {
		id = r.ChunkRange.ID
	}
	dbIter := &ChunksIterator{
		currentID:      id,
		tableAnalyzer:  analyzer,
		TableDiffs:     s.tableDiffs,
		nextTableIndex: 0,
		limit:          0,
	}
	err := dbIter.nextTable(ctx, r)
	return dbIter, err
}

func (s *TiDBSource) Close() {
	s.dbConn.Close()
}

func (s *TiDBSource) GetCountAndCrc32(ctx context.Context, tableRange *splitter.RangeInfo, checksumInfoCh chan *ChecksumInfo) {
	beginTime := time.Now()
	table := s.tableDiffs[tableRange.GetTableIndex()]
	chunk := tableRange.GetChunk()

	matchSource := getMatchSource(s.sourceTableMap, table)
	count, checksum, err := utils.GetCountAndCRC32Checksum(ctx, s.dbConn, matchSource.OriginSchema, matchSource.OriginTable, table.Info, chunk.Where, utils.StringsToInterfaces(chunk.Args))

	cost := time.Since(beginTime)
	checksumInfoCh <- &ChecksumInfo{
		Checksum: checksum,
		Count:    count,
		Err:      err,
		Cost:     cost,
	}
}

func (s *TiDBSource) GetTables() []*common.TableDiff {
	return s.tableDiffs
}

func (s *TiDBSource) GetSourceStructInfo(ctx context.Context, tableIndex int) ([]*model.TableInfo, error) {
	var err error
	tableInfos := make([]*model.TableInfo, 1)
	tableDiff := s.GetTables()[tableIndex]
	source := getMatchSource(s.sourceTableMap, tableDiff)
	tableInfos[0], err = dbutil.GetTableInfo(ctx, s.GetDB(), source.OriginSchema, source.OriginTable)
	if err != nil {
		return nil, errors.Trace(err)
	}
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
	args := utils.StringsToInterfaces(chunk.Args)

	table := s.tableDiffs[tableRange.GetTableIndex()]
	matchedSource := getMatchSource(s.sourceTableMap, table)
	rowsQuery, _ := utils.GetTableRowsQueryFormat(matchedSource.OriginSchema, matchedSource.OriginTable, table.Info, table.Collation)
	query := fmt.Sprintf(rowsQuery, chunk.Where)

	log.Debug("select data", zap.String("sql", query), zap.Reflect("args", args))
	rows, err := s.dbConn.QueryContext(ctx, query, args...)
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

func getSourceTableMap(ctx context.Context, tableDiffs []*common.TableDiff, ds *config.DataSource) (map[string]*common.TableSource, error) {
	sourceTableMap := make(map[string]*common.TableSource)
	if ds.Router != nil {
		log.Info("find router for tidb source")
		// we should get the real table name
		// and real table row query from source.
		uniqueMap := make(map[string]struct{})
		for _, tableDiff := range tableDiffs {
			uniqueMap[utils.UniqueID(tableDiff.Schema, tableDiff.Table)] = struct{}{}
		}

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
				targetSchema, targetTable, err := ds.Router.Route(schema, table)
				if err != nil {
					return nil, errors.Errorf("get route result for %s.%s failed, error %v", schema, table, err)
				}
				uniqueId := utils.UniqueID(targetSchema, targetTable)
				if _, ok := uniqueMap[uniqueId]; ok {
					if _, ok := sourceTableMap[uniqueId]; ok {
						log.Fatal("TiDB source don't merge multiple tables into one table")
					}
					sourceTableMap[uniqueId] = &common.TableSource{
						OriginSchema: schema,
						OriginTable:  table,
					}
				}
			}
		}
	}
	return sourceTableMap, nil
}

func NewTiDBSource(ctx context.Context, tableDiffs []*common.TableDiff, ds *config.DataSource) (Source, error) {
	sourceTableMap, err := getSourceTableMap(ctx, tableDiffs, ds)
	if err != nil {
		return nil, errors.Trace(err)
	}
	ts := &TiDBSource{
		tableDiffs:     tableDiffs,
		sourceTableMap: sourceTableMap,
		dbConn:         ds.Conn,
	}
	return ts, nil
}
