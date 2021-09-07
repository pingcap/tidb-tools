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

const DefaultChannelBuffer = 1024

type BucketIterator struct {
	buckets      []dbutil.Bucket
	table        *common.TableDiff
	indexColumns []*model.ColumnInfo

	chunkPool *utils.WorkerPool
	chunkSize int64
	chunks    []*chunk.Range
	nextChunk uint

	chunksCh   chan []*chunk.Range
	errCh      chan error
	indexID    int64
	progressID string

	dbConn *sql.DB
}

func NewBucketIterator(ctx context.Context, progressID string, table *common.TableDiff, dbConn *sql.DB) (*BucketIterator, error) {
	return NewBucketIteratorWithCheckpoint(ctx, progressID, table, dbConn, nil, 1)
}

func NewBucketIteratorWithCheckpoint(ctx context.Context, progressID string, table *common.TableDiff, dbConn *sql.DB, startRange *RangeInfo, checkThreadCount int) (*BucketIterator, error) {
	bs := &BucketIterator{
		table:     table,
		chunkSize: table.ChunkSize,
		chunksCh:  make(chan []*chunk.Range, DefaultChannelBuffer),
		errCh:     make(chan error, 1),
		dbConn:    dbConn,

		progressID: progressID,
	}

	bs.chunkPool = utils.NewWorkerPool(uint(checkThreadCount), "bucketIter")

	if err := bs.init(startRange); err != nil {
		return nil, errors.Trace(err)
	}

	progress.StartTable(bs.progressID, 0, false)
	go bs.produceChunks(ctx, startRange)

	return bs, nil
}

func (s *BucketIterator) GetIndexID() int64 {
	return s.indexID
}

func (s *BucketIterator) Next() (*chunk.Range, error) {
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
	c.Index.ChunkIndex = int(s.nextChunk)
	s.nextChunk = s.nextChunk + 1
	return c, nil
}

func (s *BucketIterator) init(startRange *RangeInfo) error {
	s.nextChunk = 0
	buckets, err := dbutil.GetBucketsInfo(context.Background(), s.dbConn, s.table.Schema, s.table.Table, s.table.Info)
	if err != nil {
		return errors.Trace(err)
	}
	indices, err := utils.GetBetterIndex(context.Background(), s.dbConn, s.table.Schema, s.table.Table, s.table.Info)
	if err != nil {
		return errors.Trace(err)
	}
	for _, index := range indices {
		if index == nil {
			continue
		}
		if startRange != nil && startRange.IndexID != index.ID {
			continue
		}
		bucket, ok := buckets[index.Name.O]
		if !ok {
			return errors.NotFoundf("index %s in buckets info", index.Name.O)
		}
		log.Debug("buckets for index", zap.String("index", index.Name.O), zap.Reflect("buckets", buckets))

		indexColumns := utils.GetColumnsFromIndex(index, s.table.Info)

		if len(indexColumns) < len(index.Columns) {
			// some column in index is ignored.
			continue
		}
		s.buckets = bucket
		s.indexColumns = indexColumns
		s.indexID = index.ID
		break
	}

	if s.buckets == nil || s.indexColumns == nil {
		return errors.NotFoundf("no index to split buckets")
	}

	cnt := s.buckets[len(s.buckets)-1].Count
	if s.chunkSize <= 0 {
		s.chunkSize = utils.CalculateChunkSize(cnt)
	}

	log.Info("get chunk size for table", zap.Int64("chunk size", s.chunkSize),
		zap.String("db", s.table.Schema), zap.String("table", s.table.Table))
	return nil
}

func (s *BucketIterator) Close() {
}

func (s *BucketIterator) splitChunkForBucket(bucketID int, count int64, chunkRange *chunk.Range) {
	// That count = 0 and then chunkCnt = 0 is OK.
	// `splitRangeByRandom` will skip when chunkCnt <= 1
	//            count                     chunkCnt
	// 0 ... 0.5x ... x ... 1.5x   ------->   1
	//       1.5x ... 2x ... 2.5x  ------->   2
	s.chunkPool.Apply(func() {
		halfChunkSize := s.chunkSize / 2
		chunkCnt := int((count + halfChunkSize) / s.chunkSize)
		chunks, err := splitRangeByRandom(s.dbConn, chunkRange, chunkCnt, s.table.Schema, s.table.Table, s.indexColumns, s.table.Range, s.table.Collation)
		if err != nil {
			s.errCh <- errors.Trace(err)
			return
		}
		chunk.InitChunks(chunks, chunk.Bucket, bucketID, s.table.Collation, s.table.Range, chunkCnt)
		progress.UpdateTotal(s.progressID, len(chunks), false)
		s.chunksCh <- chunks
	})
}

func (s *BucketIterator) produceChunks(ctx context.Context, startRange *RangeInfo) {
	defer func() {
		progress.UpdateTotal(s.progressID, 0, true)
		close(s.chunksCh)
	}()
	var (
		lowerValues, upperValues []string
		latestCount              int64
		err                      error
	)
	beginBucket := 0
	if startRange != nil {
		chunkRange := chunk.NewChunkRange()
		c := startRange.GetChunk()

		for _, bound := range c.Bounds {
			chunkRange.Update(bound.Column, bound.Upper, "", true, false)
		}
		if c.IsLastChunkForTable() {
			// the last checkpoint range is the last chunk so return
			return
		}

		beginBucket = c.BucketID + 1
		if c.BucketID < len(s.buckets) {
			nextUpperValues, err := dbutil.AnalyzeValuesFromBuckets(s.buckets[c.BucketID].UpperBound, s.indexColumns)
			if err != nil {
				s.errCh <- errors.Trace(err)
				return
			}
			for i, column := range s.indexColumns {
				chunkRange.Update(column.Name.O, "", nextUpperValues[i], false, true)
			}
			latestCount = s.buckets[c.BucketID].Count
			lowerValues, err = dbutil.AnalyzeValuesFromBuckets(s.buckets[beginBucket].LowerBound, s.indexColumns)
			if err != nil {
				s.errCh <- errors.Trace(err)
				return
			}
		}

		where, args := chunkRange.ToString(s.table.Collation)

		count, err := dbutil.GetRowCount(ctx, s.dbConn, s.table.Schema, s.table.Table, where, utils.StringsToInterfaces(args))
		if err != nil {
			s.errCh <- errors.Trace(err)
			return
		}
		if count > 0 {
			s.splitChunkForBucket(c.BucketID, count, chunkRange)
		}
		if len(lowerValues) == 0 {
			// The node next the checkpoint is the last node
			return
		}
	}
	for i := beginBucket; i < len(s.buckets); i++ {
		count := s.buckets[i].Count - latestCount
		if count < s.chunkSize {
			// merge more buckets into one chunk
			continue
		}

		upperValues, err = dbutil.AnalyzeValuesFromBuckets(s.buckets[i].UpperBound, s.indexColumns)
		if err != nil {
			s.errCh <- errors.Trace(err)
			return
		}

		chunkRange := chunk.NewChunkRange()
		for j, column := range s.indexColumns {
			var lowerValue, upperValue string
			if len(lowerValues) > 0 {
				lowerValue = lowerValues[j]
			}
			if len(upperValues) > 0 {
				upperValue = upperValues[j]
			}
			chunkRange.Update(column.Name.O, lowerValue, upperValue, len(lowerValues) > 0, len(upperValues) > 0)
		}

		s.splitChunkForBucket(i, count, chunkRange)
		latestCount = s.buckets[i].Count
		lowerValues = upperValues
	}

	// merge the rest keys into one chunk
	chunkRange := chunk.NewChunkRange()
	if len(lowerValues) > 0 {
		for j, column := range s.indexColumns {
			chunkRange.Update(column.Name.O, lowerValues[j], "", true, false)
		}
	}
	chunks := []*chunk.Range{chunkRange}
	chunk.InitChunks(chunks, chunk.Bucket, len(s.buckets), s.table.Collation, s.table.Range, 1)
	progress.UpdateTotal(s.progressID, len(chunks), false)
	s.chunksCh <- chunks
	s.chunkPool.WaitFinished()
}
