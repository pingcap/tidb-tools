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
	"github.com/pingcap/tidb-tools/sync_diff_inspector/checkpoints"
	"github.com/pingcap/tidb-tools/sync_diff_inspector/chunk"
	"github.com/pingcap/tidb-tools/sync_diff_inspector/source/common"
	"github.com/pingcap/tidb-tools/sync_diff_inspector/utils"
	"go.uber.org/zap"
)

type BucketIterator struct {
	table        *common.TableDiff
	indexColumns []*model.ColumnInfo
	buckets      []dbutil.Bucket
	chunkSize    int64
	chunks       []*chunk.Range
	nextChunk    uint
	chunksCh     chan []*chunk.Range
	errCh        chan error
	ctrlCh       chan bool

	dbConn *sql.DB
}

func NewBucketIterator(table *common.TableDiff, dbConn *sql.DB, chunkSize int) (*BucketIterator, error) {
	return NewBucketIteratorWithCheckpoint(table, dbConn, chunkSize, nil)
}

func NewBucketIteratorWithCheckpoint(table *common.TableDiff, dbConn *sql.DB, chunkSize int, node *checkpoints.BucketNode) (*BucketIterator, error) {

	bs := &BucketIterator{
		table:     table,
		chunkSize: int64(chunkSize),
		chunksCh:  make(chan []*chunk.Range, 1),
		errCh:     make(chan error, 1),
		ctrlCh:    make(chan bool, 2),
		dbConn:    dbConn,
	}
	go bs.createProducerWithCheckpoint(node)

	if err := bs.init(); err != nil {
		return nil, errors.Trace(err)
	}

	return bs, nil
}

func (s *BucketIterator) Next() (*chunk.Range, error) {
	// `len(s.chunks) == 0` is included in this
	if uint(len(s.chunks)) <= s.nextChunk {
		// TODO: add timeout
		// select {

		// }
		select {
		case err := <-s.errCh:
			return nil, err
		case s.chunks = <-s.chunksCh:
		}

		if s.chunks == nil {
			return nil, nil
		}
		s.nextChunk = 0
	}

	chunk := s.chunks[s.nextChunk]
	s.nextChunk = s.nextChunk + 1
	return chunk, nil
}

func (s *BucketIterator) init() error {
	s.nextChunk = 0
	buckets, err := dbutil.GetBucketsInfo(context.Background(), s.dbConn, s.table.Schema, s.table.Table, s.table.Info)
	if err != nil {
		return errors.Trace(err)
	}
	// TODO: 1. ignore some columns
	//		 2. how to choose index
	indices := dbutil.FindAllIndex(s.table.Info)
	for _, index := range indices {
		if index == nil {
			continue
		}
		bucket, ok := buckets[index.Name.O]
		if !ok {
			return errors.NotFoundf("index %s in buckets info", index.Name.O)
		}
		log.Debug("buckets for index", zap.String("index", index.Name.O), zap.Reflect("buckets", buckets))

		indexcolumns := utils.GetColumnsFromIndex(index, s.table.Info)

		if len(indexcolumns) == 0 {
			continue
		}
		s.buckets = bucket
		s.indexColumns = indexcolumns
		break
	}

	if s.buckets == nil || s.indexColumns == nil {
		return errors.NotFoundf("no index to split buckets")
	}

	s.ctrlCh <- false
	return nil
}

func (s *BucketIterator) Close() {
	s.ctrlCh <- true
}

func (s *BucketIterator) createProducerWithCheckpoint(node *checkpoints.BucketNode) {
	// close this goruntine gracefully.
	// init control
	ctrl := <-s.ctrlCh
	if ctrl {
		return
	}
	var (
		lowerValues, upperValues []string
		latestCount              int64
		err                      error
	)
	chunkSize := s.chunkSize
	table := s.table
	buckets := s.buckets
	indexColumns := s.indexColumns
	beginBucket := 0
	if node == nil {
		lowerValues = make([]string, len(indexColumns), len(indexColumns))
	} else {
		lowerValues = node.GetUpperBound()
		beginBucket = node.GetBucketID()
	}
	// TODO chunksize when checkpoint
	for i := beginBucket; i < len(buckets); i++ {
		count := buckets[i].Count - latestCount
		if count < s.chunkSize {
			// merge more buckets into one chunk
			continue
		}

		upperValues, err = dbutil.AnalyzeValuesFromBuckets(buckets[i].UpperBound, indexColumns)
		if err != nil {
			s.errCh <- errors.Trace(err)
			return
		}

		chunkRange := chunk.NewChunkRange()
		for j, column := range indexColumns {
			var lowerValue, upperValue string
			if len(lowerValues) > 0 {
				lowerValue = lowerValues[j]
			}
			if len(upperValues) > 0 {
				upperValue = upperValues[j]
			}
			chunkRange.Update(column.Name.O, lowerValue, upperValue, len(lowerValues) > 0, len(upperValues) > 0)
		}

		chunks := []*chunk.Range{}
		if count >= 2*chunkSize {
			splitChunks, err := splitRangeByRandom(s.dbConn, chunkRange, int(count/chunkSize), table.Schema, table.Table, indexColumns, table.Range, table.Collation, node)
			if err != nil {
				s.errCh <- errors.Trace(err)
				return
			}
			chunks = append(chunks, splitChunks...)
		} else {
			chunks = append(chunks, chunkRange)
		}

		latestCount = buckets[i].Count
		lowerValues = upperValues
		select {
		case _ = <-s.ctrlCh:
			return
		case s.chunksCh <- chunks:

		}

	}

	// merge the rest keys into one chunk
	if len(lowerValues) > 0 {
		chunkRange := chunk.NewChunkRange()
		for j, column := range indexColumns {
			chunkRange.Update(column.Name.O, lowerValues[j], "", true, false)
		}
		select {
		case _ = <-s.ctrlCh:
			return
		case s.chunksCh <- []*chunk.Range{chunkRange}:

		}
	}

	// finish split this table.
	for {
		select {
		case _ = <-s.ctrlCh:
			return
		case s.chunksCh <- nil:
		}
	}

}
