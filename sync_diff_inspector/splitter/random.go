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

package splitter

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb-tools/pkg/dbutil"
	"github.com/pingcap/tidb-tools/pkg/utils"
	"github.com/pingcap/tidb-tools/sync_diff_inspector/chunk"
	"github.com/pingcap/tidb-tools/sync_diff_inspector/source/common"
	"go.uber.org/zap"
)

type RandomIterator struct {
	table     *common.TableDiff
	chunkSize int
	chunks    []*chunk.Range
	nextChunk uint

	dbConn *sql.DB
}

func NewRandomIterator(table *common.TableDiff, dbConn *sql.DB, chunkSize int, limits string, collation string) (*RandomIterator, error) {
	// get the chunk count by data count and chunk size
	cnt, err := dbutil.GetRowCount(context.Background(), dbConn, table.Schema, table.Table, limits, nil)
	if err != nil {
		return nil, errors.Trace(err)
	}

	chunkCnt := (int(cnt) + chunkSize - 1) / chunkSize
	log.Info("split range by random", zap.Int64("row count", cnt), zap.Int("split chunk num", chunkCnt))

	var splitFieldArr []string
	if len(table.Fields) != 0 {
		splitFieldArr = strings.Split(table.Fields, ",")
	}

	for i := range splitFieldArr {
		splitFieldArr[i] = strings.TrimSpace(splitFieldArr[i])
	}

	fields, err := getSplitFields(table.Info, splitFieldArr)
	if err != nil {
		return nil, errors.Trace(err)
	}

	chunks, err := splitRangeByRandom(dbConn, chunk.NewChunkRange(), chunkCnt, table.Schema, table.Table, fields, table.Range, table.Collation)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &RandomIterator{
		table:     table,
		chunkSize: chunkSize,
		chunks:    chunks,
		nextChunk: 0,
		dbConn:    dbConn,
	}, nil

}

func (s *RandomIterator) Next() (*chunk.Range, error) {
	if uint(len(s.chunks)) <= s.nextChunk {
		return nil, nil
	}
	chunk := s.chunks[s.nextChunk]
	s.nextChunk = s.nextChunk + 1
	return chunk, nil
}

func (s *RandomIterator) Close() {

}

// getSplitFields returns fields to split chunks, order by pk, uk, index, columns.
func getSplitFields(table *model.TableInfo, splitFields []string) ([]*model.ColumnInfo, error) {
	cols := make([]*model.ColumnInfo, 0, len(table.Columns))
	colsMap := make(map[string]*model.ColumnInfo)

	splitCols := make([]*model.ColumnInfo, 0, 2)
	for _, splitField := range splitFields {
		col := dbutil.FindColumnByName(table.Columns, splitField)
		if col == nil {
			return nil, errors.NotFoundf("column %s in table %s", splitField, table.Name)

		}
		splitCols = append(splitCols, col)
	}

	if len(splitCols) != 0 {
		return splitCols, nil
	}

	for _, col := range table.Columns {
		colsMap[col.Name.O] = col
	}
	indices := dbutil.FindAllIndex(table)
	if len(indices) != 0 {
		for _, col := range indices[0].Columns {
			cols = append(cols, colsMap[col.Name.O])
		}
		return cols, nil
	}

	return []*model.ColumnInfo{table.Columns[0]}, nil
}

// splitRangeByRandom splits a chunk to multiple chunks by random
func splitRangeByRandom(db *sql.DB, chunk *chunk.Range, count int, schema string, table string, columns []*model.ColumnInfo, limits, collation string) (chunks []*chunk.Range, err error) {
	if count <= 1 {
		chunks = append(chunks, chunk)
		return chunks, nil
	}

	chunkLimits, args := chunk.ToString(collation)
	limitRange := fmt.Sprintf("(%s) AND %s", chunkLimits, limits)

	randomValues := make([][]string, len(columns))
	for i, column := range columns {
		randomValues[i], err = dbutil.GetRandomValues(context.Background(), db, schema, table, column.Name.O, count-1, limitRange, utils.StringsToInterfaces(args), collation)
		if err != nil {
			return nil, errors.Trace(err)
		}

		log.Debug("get split values by random", zap.Stringer("chunk", chunk), zap.String("column", column.Name.O), zap.Int("random values num", len(randomValues[i])))
	}

	// TODO build random chunks
	log.Debug("split range by random", zap.Stringer("origin chunk", chunk), zap.Int("split num", len(chunks)))

	return chunks, nil
}
