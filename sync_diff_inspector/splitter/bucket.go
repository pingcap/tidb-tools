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
	indexID      int64

	dbConn *sql.DB
}

func NewBucketIterator(table *common.TableDiff, dbConn *sql.DB, chunkSize int) (*BucketIterator, error) {
	return NewBucketIteratorWithCheckpoint(table, dbConn, chunkSize, nil)
}

func NewBucketIteratorWithCheckpoint(table *common.TableDiff, dbConn *sql.DB, chunkSize int, node checkpoints.Node) (*BucketIterator, error) {
	bs := &BucketIterator{
		table:     table,
		chunkSize: int64(chunkSize),
		chunksCh:  make(chan []*chunk.Range, 1024),
		errCh:     make(chan error, 1),
		dbConn:    dbConn,
	}

	bucketNode, ok := node.(*checkpoints.BucketNode)
	if !ok {
		bucketNode = nil
	}
	if err := bs.init(bucketNode); err != nil {
		return nil, errors.Trace(err)
	}
	go bs.produceChunkWithCheckpoint(bucketNode)

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
	chunk.IndexID = s.indexID
	s.nextChunk = s.nextChunk + 1
	return chunk, nil
}

func (s *BucketIterator) init(node *checkpoints.BucketNode) error {
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
		if node != nil && node.IndexID != index.ID {
			continue
		}
		bucket, ok := buckets[index.Name.O]
		if !ok {
			return errors.NotFoundf("index %s in buckets info", index.Name.O)
		}
		log.Debug("buckets for index", zap.String("index", index.Name.O), zap.Reflect("buckets", buckets))

		indexColumns := utils.GetColumnsFromIndex(index, s.table.Info)

		if len(indexColumns) == 0 {
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

	return nil
}

func (s *BucketIterator) Close() {
}

func (s *BucketIterator) produceChunkWithCheckpoint(node *checkpoints.BucketNode) {
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
	chunkID := 0
	if node == nil {
		lowerValues = make([]string, len(indexColumns), len(indexColumns))
	} else {
		bounds := node.GetUpperBound()
		columns := node.GetColumnName()
		lowerValues = make([]string, 0, len(indexColumns))
		for _, index := range indexColumns {
			for i := 0; i < len(bounds); i++ {
				if index.Name.O == columns[i] {
					lowerValues = append(lowerValues, bounds[i])
				}
			}
		}
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
			splitChunks, err := splitRangeByRandom(s.dbConn, chunkRange, int(count/chunkSize), table.Schema, table.Table, indexColumns, table.Range, table.Collation)
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
		chunkID = chunk.InitChunks(chunks, chunkID, table.Collation, table.Range)
		s.chunksCh <- chunks
	}

	// merge the rest keys into one chunk
	if len(lowerValues) > 0 {
		chunkRange := chunk.NewChunkRange()
		for j, column := range indexColumns {
			chunkRange.Update(column.Name.O, lowerValues[j], "", true, false)
		}
		chunks := []*chunk.Range{chunkRange}
		chunkID = chunk.InitChunks(chunks, chunkID, table.Collation, table.Range)
		s.chunksCh <- chunks
	}
}
