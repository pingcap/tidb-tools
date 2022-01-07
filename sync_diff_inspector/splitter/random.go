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
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb-tools/pkg/dbutil"
	"github.com/pingcap/tidb-tools/sync_diff_inspector/chunk"
	"github.com/pingcap/tidb-tools/sync_diff_inspector/progress"
	"github.com/pingcap/tidb-tools/sync_diff_inspector/source/common"
	"github.com/pingcap/tidb-tools/sync_diff_inspector/utils"
	"github.com/pingcap/tidb/parser/model"
	"go.uber.org/zap"
)

type RandomIterator struct {
	table     *common.TableDiff
	chunkSize int64
	chunks    []*chunk.Range
	nextChunk uint

	dbConn *sql.DB
}

func NewRandomIterator(ctx context.Context, progressID string, table *common.TableDiff, dbConn *sql.DB) (*RandomIterator, error) {
	return NewRandomIteratorWithCheckpoint(ctx, progressID, table, dbConn, nil)
}

func NewRandomIteratorWithCheckpoint(ctx context.Context, progressID string, table *common.TableDiff, dbConn *sql.DB, startRange *RangeInfo) (*RandomIterator, error) {
	// get the chunk count by data count and chunk size
	var splitFieldArr []string
	if len(table.Fields) != 0 {
		splitFieldArr = strings.Split(table.Fields, ",")
	}

	for i := range splitFieldArr {
		splitFieldArr[i] = strings.TrimSpace(splitFieldArr[i])
	}

	fields, err := GetSplitFields(table.Info, splitFieldArr)
	if err != nil {
		return nil, errors.Trace(err)
	}

	chunkRange := chunk.NewChunkRange()
	beginIndex := 0
	bucketChunkCnt := 0
	chunkCnt := 0
	var chunkSize int64 = 0
	if startRange != nil {
		c := startRange.GetChunk()
		if c.IsLastChunkForTable() {
			return &RandomIterator{
				table:     table,
				chunkSize: 0,
				chunks:    nil,
				nextChunk: 0,
				dbConn:    dbConn,
			}, nil
		}
		// The sequences in `chunk.Range.Bounds` should be equivalent.
		for _, bound := range c.Bounds {
			chunkRange.Update(bound.Column, bound.Upper, "", true, false)
		}

		// Recover the chunkIndex. Let it be next to the checkpoint node.
		beginIndex = c.Index.ChunkIndex + 1
		bucketChunkCnt = c.Index.ChunkCnt
		// For chunk splitted by random splitter, the checkpoint chunk records the tableCnt.
		chunkCnt = bucketChunkCnt - beginIndex
	} else {
		cnt, err := dbutil.GetRowCount(ctx, dbConn, table.Schema, table.Table, table.Range, nil)
		if err != nil {
			return nil, errors.Trace(err)
		}

		chunkSize = table.ChunkSize
		// We can use config file to fix chunkSize,
		// otherwise chunkSize is 0.
		if chunkSize <= 0 {
			if len(table.Info.Indices) != 0 {
				chunkSize = utils.CalculateChunkSize(cnt)
			} else {
				// no index
				// will use table scan
				// so we use one chunk
				// plus 1 to avoid chunkSize is 0
				// while chunkCnt = (2cnt)/(cnt+1) <= 1
				chunkSize = cnt + 1
			}
		}
		log.Info("get chunk size for table", zap.Int64("chunk size", chunkSize),
			zap.String("db", table.Schema), zap.String("table", table.Table))

		// When cnt is 0, chunkCnt should be also 0.
		// When cnt is in [1, chunkSize], chunkCnt should be 1.
		chunkCnt = int((cnt + chunkSize - 1) / chunkSize)
		log.Info("split range by random", zap.Int64("row count", cnt), zap.Int("split chunk num", chunkCnt))
		bucketChunkCnt = chunkCnt
	}

	chunks, err := splitRangeByRandom(dbConn, chunkRange, chunkCnt, table.Schema, table.Table, fields, table.Range, table.Collation)
	if err != nil {
		return nil, errors.Trace(err)
	}
	chunk.InitChunks(chunks, chunk.Random, 0, 0, beginIndex, table.Collation, table.Range, bucketChunkCnt)

	failpoint.Inject("ignore-last-n-chunk-in-bucket", func(v failpoint.Value) {
		log.Info("failpoint ignore-last-n-chunk-in-bucket injected (random splitter)", zap.Int("n", v.(int)))
		if len(chunks) <= 1+v.(int) {
			failpoint.Return(nil, nil)
		}
		chunks = chunks[:(len(chunks) - v.(int))]
	})

	progress.StartTable(progressID, len(chunks), true)
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
	c := s.chunks[s.nextChunk]
	s.nextChunk = s.nextChunk + 1
	failpoint.Inject("print-chunk-info", func() {
		lowerBounds := make([]string, len(c.Bounds))
		upperBounds := make([]string, len(c.Bounds))
		for i, bound := range c.Bounds {
			lowerBounds[i] = bound.Lower
			upperBounds[i] = bound.Upper
		}
		log.Info("failpoint print-chunk-info injected (random splitter)", zap.Strings("lowerBounds", lowerBounds), zap.Strings("upperBounds", upperBounds), zap.String("indexCode", c.Index.ToString()))
	})
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
// Notice: If the `count <= 1`, it will skip splitting and return `chunk` as a slice directly.
// TODO: This function will get random row for each cols individually.
//		For example, for a table' schema which is `create table tbl(a int, b int, primary key(a, b));`,
//		there are 3 rows(`[a: 2, b: 2]`, `[a: 3, b: 5]`, `[a: 4, b: 4]`) in the table.
//		and finally this function might generate `[a:2,b:2]` and `[a:3,b:4]` (from `a` get random value 2,4, `b` get random value 2,4) as split points, which means
//		chunk whose range is (`a:2,b:2`, `a:3,b:4`], so we get a empty chunk.
func splitRangeByRandom(db *sql.DB, chunk *chunk.Range, count int, schema string, table string, columns []*model.ColumnInfo, limits, collation string) (chunks []*chunk.Range, err error) {
	if count <= 1 {
		chunks = append(chunks, chunk)
		return chunks, nil
	}

	chunkLimits, args := chunk.ToString(collation)
	limitRange := fmt.Sprintf("(%s) AND (%s)", chunkLimits, limits)

	randomValues := make([][]string, len(columns))
	for i, column := range columns {
		randomValues[i], err = dbutil.GetRandomValues(context.Background(), db, schema, table, column.Name.O, count-1, limitRange, args, collation)
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
