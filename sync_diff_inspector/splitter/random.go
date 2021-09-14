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
	"github.com/pingcap/tidb-tools/sync_diff_inspector/chunk"
	"github.com/pingcap/tidb-tools/sync_diff_inspector/progress"
	"github.com/pingcap/tidb-tools/sync_diff_inspector/source/common"
	"github.com/pingcap/tidb-tools/sync_diff_inspector/utils"
	"go.uber.org/zap"
)

type RandomIterator struct {
	table     *common.TableDiff
	chunkSize int64
	chunks    []*chunk.Range
	chunksCh  chan []*chunk.Range
	nextChunk uint
	errCh     chan error

	dbConn     *sql.DB
	progressID string
}

func NewRandomIterator(ctx context.Context, progressID string, table *common.TableDiff, dbConn *sql.DB) (*RandomIterator, error) {
	return NewRandomIteratorWithCheckpoint(ctx, progressID, table, dbConn, nil)
}

func NewRandomIteratorWithCheckpoint(ctx context.Context, progressID string, table *common.TableDiff, dbConn *sql.DB, startRange *RangeInfo) (*RandomIterator, error) {
	rs := &RandomIterator{
		table:      table,
		chunkSize:  table.ChunkSize,
		chunksCh:   make(chan []*chunk.Range, DefaultChannelBuffer),
		dbConn:     dbConn,
		progressID: progressID,
		errCh:      make(chan error, 1),
	}
	progress.StartTable(rs.progressID, 0, false)
	go rs.produceChunks(ctx, startRange)
	return rs, nil

}

func (s *RandomIterator) produceChunks(ctx context.Context, startRange *RangeInfo) {
	defer func() {
		progress.UpdateTotal(s.progressID, 0, true)
		close(s.chunksCh)
	}()
	// get the chunk count by data count and chunk size
	var splitFieldArr []string
	if len(s.table.Fields) != 0 {
		splitFieldArr = strings.Split(s.table.Fields, ",")
	}

	for i := range splitFieldArr {
		splitFieldArr[i] = strings.TrimSpace(splitFieldArr[i])
	}

	fields, err := GetSplitFields(s.table.Info, splitFieldArr)
	if err != nil {
		s.errCh <- err
		return
	}
	chunkRange := chunk.NewChunkRange()
	where := s.table.Range
	var iargs []interface{}
	if startRange != nil {
		c := startRange.GetChunk()
		if c.IsLastChunkForTable() {
			return
		}
		for _, bound := range c.Bounds {
			chunkRange.Update(bound.Column, bound.Upper, "", true, false)
		}

		conditions, args := chunkRange.ToString(s.table.Collation)
		if len(where) > 0 {
			where = fmt.Sprintf("((%s) AND %s)", conditions, where)
		} else {
			where = fmt.Sprintf("(%s)", conditions)
		}
		iargs = utils.StringsToInterfaces(args)
	}

	cnt, err := dbutil.GetRowCount(ctx, s.dbConn, s.table.Schema, s.table.Table, where, iargs)
	if err != nil {
		s.errCh <- err
		return
	}

	chunkSize := s.table.ChunkSize
	if chunkSize <= 0 {
		if len(s.table.Info.Indices) != 0 {
			chunkSize = utils.CalculateChunkSize(cnt)
		} else {
			// no index
			// will use table scan
			// so we use one chunk
			chunkSize = cnt
		}
	}
	log.Info("get chunk size for table", zap.Int64("chunk size", chunkSize),
		zap.String("db", s.table.Schema), zap.String("table", s.table.Table))

	chunkCnt := (cnt + chunkSize - 1) / chunkSize
	log.Info("split range by random", zap.Int64("row count", cnt), zap.Int64("split chunk num", chunkCnt))

	chunks, err := splitRangeByRandom(s.dbConn, chunkRange, int(chunkCnt), s.table.Schema, s.table.Table, fields, s.table.Range, s.table.Collation)
	if err != nil {
		s.errCh <- err
		return
	}
	chunk.InitChunks(chunks, chunk.Random, 0, s.table.Collation, s.table.Range, len(chunks))

	progress.UpdateTotal(s.progressID, len(chunks), false)
	s.chunksCh <- chunks
	return
}

func (s *RandomIterator) Next() (*chunk.Range, error) {
	var ok bool
	if uint(len(s.chunks)) <= s.nextChunk {
		select {
		case err := <-s.errCh:
			return nil, errors.Trace(err)
		case s.chunks, ok = <-s.chunksCh:
			if !ok && s.chunks == nil {
				log.Info("close chunks channel for table",
					zap.String("schema", s.table.Schema), zap.String("table", s.table.Table))
				return nil, nil
			}
		}
		s.nextChunk = 0
	}
	c := s.chunks[s.nextChunk]
	s.nextChunk = s.nextChunk + 1
	return c, nil
}

func (s *RandomIterator) Close() {

}

// GetSplitFields returns fields to split chunks, order by pk, uk, index, columns.
func GetSplitFields(table *model.TableInfo, splitFields []string) ([]*model.ColumnInfo, error) {
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

	for i := 0; i <= utils.MinLenInSlices(randomValues); i++ {
		newChunk := chunk.Copy()

		for j, column := range columns {
			if i == 0 {
				if len(randomValues[j]) == 0 {
					break
				}
				newChunk.Update(column.Name.O, "", randomValues[j][i], false, true)
			} else if i == len(randomValues[j]) {
				newChunk.Update(column.Name.O, randomValues[j][i-1], "", true, false)
			} else {
				newChunk.Update(column.Name.O, randomValues[j][i-1], randomValues[j][i], true, true)
			}
		}
		chunks = append(chunks, newChunk)
	}
	log.Debug("split range by random", zap.Stringer("origin chunk", chunk), zap.Int("split num", len(chunks)))
	return chunks, nil
}
