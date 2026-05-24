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
	lightningcommon "github.com/pingcap/tidb/pkg/lightning/common"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/util/mathutil"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

type RandomIterator struct {
	table     *common.TableDiff
	chunkSize int64

	chunksCh chan *chunk.Range
	firstErr lightningcommon.OnceError
	eg       *errgroup.Group
	egCtx    context.Context
	cancel   context.CancelFunc

	dbConn     *sql.DB
	progressID string

	splitColumns  []*model.ColumnInfo
	splitRange    *chunk.Range
	totalChunkCnt int
	beginIndex    int

	coarseChunks      []*chunk.Range
	coarseSplitCounts []int
}

const randomSplitCoarseChunks = 256

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

	chunkRange := chunk.NewChunkRange(table.Info)

	// Below logic is modified from BucketIterator
	// It's used to find the index which can match the split fields in RandomIterator.
	fieldNames := utils.GetColumnNames(fields)
	var indices = dbutil.FindAllIndex(table.Info)
NEXTINDEX:
	for _, index := range indices {
		if index == nil {
			continue
		}
		if startRange != nil && startRange.IndexID != index.ID {
			continue
		}

		indexColumns := utils.GetColumnsFromIndex(index, table.Info)

		if len(indexColumns) < len(index.Columns) {
			// some column in index is ignored.
			continue
		}

		if !utils.IsIndexMatchingColumns(index, fieldNames) {
			continue
		}

		// skip the index that has expression column
		for _, col := range indexColumns {
			if col.Hidden {
				continue NEXTINDEX
			}
		}

		// Found the index, store column names
		chunkRange.IndexColumnNames = utils.GetColumnNames(indexColumns)
		break
	}

	beginIndex := 0
	bucketChunkCnt := 0
	chunkCnt := 0
	var chunkSize int64 = 0
	if startRange != nil {
		c := startRange.GetChunk()
		if c.IsLastChunkForTable() {
			iter := &RandomIterator{
				table:     table,
				chunkSize: 0,
				dbConn:    dbConn,
				chunksCh:  make(chan *chunk.Range),
			}
			close(iter.chunksCh)
			return iter, nil
		}
		// The sequences in `chunk.Range.Bounds` should be equivalent.
		for _, bound := range c.Bounds {
			chunkRange.Update(bound.Column, bound.Upper, "", true, false)
		}

		// Recover the chunkIndex. Let it be next to the checkpoint node.
		beginIndex = c.Index.ChunkIndex + 1
		bucketChunkCnt = c.Index.ChunkCnt
		chunkCnt = bucketChunkCnt - beginIndex
	} else {
		cnt, err := dbutil.GetRowCount(ctx, dbConn, table.Schema, table.Table, table.Range, nil)
		if err != nil {
			return nil, errors.Trace(err)
		}

		chunkSize = table.ChunkSize
		if chunkSize <= 0 {
			if len(table.Info.Indices) != 0 {
				chunkSize = utils.CalculateChunkSize(cnt)
			} else {
				// If table has no index, we use one chunk.
				chunkSize = cnt + 1
			}
		}

		log.Info("get chunk size for table", zap.Int64("chunk size", chunkSize),
			zap.String("db", table.Schema), zap.String("table", table.Table))

		chunkCnt = int((cnt + chunkSize - 1) / chunkSize)
		log.Info("split range by random",
			zap.Int64("row count", cnt),
			zap.Int("split chunk num", chunkCnt))
		bucketChunkCnt = chunkCnt
	}

	rctx, cancel := context.WithCancel(ctx)
	eg, egctx := errgroup.WithContext(rctx)

	iter := &RandomIterator{
		table:         table,
		chunkSize:     chunkSize,
		dbConn:        dbConn,
		egCtx:         egctx,
		cancel:        cancel,
		eg:            eg,
		progressID:    progressID,
		splitColumns:  fields,
		splitRange:    chunkRange,
		totalChunkCnt: bucketChunkCnt,
		beginIndex:    beginIndex,
		chunksCh:      make(chan *chunk.Range, DefaultChannelBuffer),
	}

	// First split the chunkRange to coarse chunks.
	if chunkCnt <= randomSplitCoarseChunks {
		iter.coarseChunks = []*chunk.Range{chunkRange}
		iter.coarseSplitCounts = []int{max(chunkCnt, 1)}
	} else {
		coarseChunks, err := splitRangeByRandom(
			rctx, dbConn, chunkRange, randomSplitCoarseChunks,
			table.Schema, table.Table, fields, table.Range, table.Collation)
		if err != nil {
			return nil, errors.Trace(err)
		}
		iter.coarseChunks = coarseChunks
		iter.coarseSplitCounts = mathutil.Divide2Batches(chunkCnt, len(coarseChunks))
	}

	// Then start to produce fine chunks from coarse chunks in background.
	iter.eg.Go(func() error {
		return iter.produceChunks()
	})

	progress.StartTable(progressID, 0, false)
	return iter, nil

}

func (s *RandomIterator) Next() (*chunk.Range, error) {
	select {
	case <-s.egCtx.Done():
		return nil, s.firstErr.Get()
	case c, ok := <-s.chunksCh:
		err := s.firstErr.Get()
		if !ok || err != nil {
			return nil, err
		}

		failpoint.Inject("print-chunk-info", func() {
			lowerBounds := make([]string, len(c.Bounds))
			upperBounds := make([]string, len(c.Bounds))
			for i, bound := range c.Bounds {
				lowerBounds[i] = bound.Lower
				upperBounds[i] = bound.Upper
			}
			log.Info("failpoint print-chunk-info injected (random splitter)",
				zap.Strings("lowerBounds", lowerBounds),
				zap.Strings("upperBounds", upperBounds),
				zap.String("indexCode", c.Index.ToString()))
		})
		return c, nil
	}
}

func (s *RandomIterator) Close() {
	if s.cancel != nil {
		s.cancel()
		_ = s.eg.Wait()
	}
}

// GetSplitFields returns fields to split chunks, order by pk, uk, index, columns.
func GetSplitFields(table *model.TableInfo, splitFields []string) ([]*model.ColumnInfo, error) {
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
	NEXTINDEX:
		for _, idx := range indices {
			cols := make([]*model.ColumnInfo, 0, len(table.Columns))
			for _, icol := range idx.Columns {
				col := colsMap[icol.Name.O]
				if col.Hidden {
					continue NEXTINDEX
				}
				cols = append(cols, col)
			}
			return cols, nil
		}
	}

	for _, col := range table.Columns {
		if !col.Hidden {
			return []*model.ColumnInfo{col}, nil
		}
	}
	return nil, errors.NotFoundf("not found column")
}

// splitRangeByRandom splits a chunk to multiple chunks by random
// Notice: If the `count <= 1`, it will skip splitting and return `chunk` as a slice directly.
func splitRangeByRandom(ctx context.Context, db *sql.DB, chunk *chunk.Range, count int, schema string, table string, columns []*model.ColumnInfo, limits, collation string) (chunks []*chunk.Range, err error) {
	if count <= 1 {
		chunks = append(chunks, chunk)
		return chunks, nil
	}

	chunkLimits, args := chunk.ToString(collation)
	limitRange := fmt.Sprintf("(%s) AND (%s)", chunkLimits, limits)

	randomValues, err := utils.GetRandomValues(ctx, db, schema, table, columns, count-1, limitRange, args, collation)
	if err != nil {
		return nil, errors.Trace(err)
	}
	log.Debug("get split values by random", zap.Stringer("chunk", chunk), zap.Int("random values num", len(randomValues)))
	for i := range len(randomValues) {
		newChunk := chunk.Copy()

		for j, column := range columns {
			if i == 0 {
				if len(randomValues) == 0 {
					// randomValues is empty, so chunks will append chunk itself.
					break
				}
				newChunk.Update(column.Name.O, "", randomValues[i][j], false, true)
			} else if i == len(randomValues) {
				newChunk.Update(column.Name.O, randomValues[i-1][j], "", true, false)
			} else {
				newChunk.Update(column.Name.O, randomValues[i-1][j], randomValues[i][j], true, true)
			}
		}
		chunks = append(chunks, newChunk)
	}
	log.Debug("split range by random",
		zap.Stringer("origin chunk", chunk),
		zap.Int("split num", len(chunks)))
	return chunks, nil
}

func (s *RandomIterator) produceChunks() error {
	defer close(s.chunksCh)
	for i := range s.coarseChunks {
		fineChunks, err := splitRangeByRandom(
			s.egCtx, s.dbConn, s.coarseChunks[i], s.coarseSplitCounts[i],
			s.table.Schema, s.table.Table,
			s.splitColumns, s.table.Range, s.table.Collation)
		if err != nil {
			s.firstErr.Set(err)
			return err
		}

		isLastCoarseChunk := i == len(s.coarseChunks)-1
		if isLastCoarseChunk {
			failpoint.Inject("ignore-last-n-chunk-in-bucket", func(v failpoint.Value) {
				log.Info("failpoint ignore-last-n-chunk-in-bucket injected (random splitter)", zap.Int("n", v.(int)))
				if len(fineChunks) <= 1+v.(int) {
					fineChunks = nil
					return
				}
				fineChunks = fineChunks[:(len(fineChunks) - v.(int))]
			})
		}

		chunk.InitChunks(fineChunks, chunk.Random, 0, 0, s.beginIndex, s.totalChunkCnt, s.table.Collation, s.table.Range)
		s.beginIndex += len(fineChunks)
		progress.UpdateTotal(s.progressID, len(fineChunks), isLastCoarseChunk)
		for _, fineChunk := range fineChunks {
			select {
			case <-s.egCtx.Done():
				return nil
			case s.chunksCh <- fineChunk:
			}
		}
	}
	return nil
}
