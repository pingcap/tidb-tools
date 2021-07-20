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
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb-tools/sync_diff_inspector/chunk"
	"github.com/pingcap/tidb-tools/sync_diff_inspector/config"
	"github.com/pingcap/tidb-tools/sync_diff_inspector/source/common"
	"github.com/pingcap/tidb-tools/sync_diff_inspector/splitter"
)

type TiDBChunksIterator struct {
	TableDiffs          []*common.TableDiff
	curTableIndex       int
	curTableHasSplitted bool

	chunkSize int
	limit     int

	TiDBSplitter *TiDBSplitter

	iter chunk.Iterator
}

func (t *TiDBChunksIterator) Next() (*chunk.Range, error) {
	if !t.curTableHasSplitted {
		curTable := t.TableDiffs[t.curTableIndex]

		chunkIter, err := t.TiDBSplitter.splitChunksForTable(curTable)
		if err != nil {
			return nil, errors.Trace(err)
		}
		t.iter = chunkIter
		t.curTableHasSplitted = true
	}
	chunk, err := t.iter.Next()
	if err != nil {
		return nil, errors.Trace(err)
	}
	if chunk == nil {
		// current table seek to end
		t.curTableIndex++
		t.curTableHasSplitted = false
	}
	return chunk, nil
}

type TiDBSplitter struct {
	chunkSize int
	collation string

	dbConn *sql.DB
}

// useBucket returns the tableInstance that can use bucket info whether in source or target.
func (s *TiDBSplitter) useBucket(diff *common.TableDiff) bool {
	// TODO check whether we can use bucket for this table to split chunks.
	return true
}

func (s *TiDBSplitter) splitChunksForTable(tableDiff *common.TableDiff) (chunk.Iterator, error) {
	if s.useBucket(tableDiff) {
		bucketSplitter, err := splitter.NewBucketSplitter(tableDiff, s.collation, s.dbConn)
		if err != nil {
			return nil, errors.Trace(err)
		}
		chunkIter, err := bucketSplitter.Split()
		if err != nil {
			// fall back to random splitter
		}
		return chunkIter, nil
	}
	// use random splitter if we cannot use bucket splitter, then we can simply choose target table to generate chunks.
	randSplitter := splitter.NewRandomSplitter(tableDiff, s.chunkSize, tableDiff.Range, tableDiff.Collation)
	return randSplitter.Split()
}

type TiDBSource struct {
	tableDiffs []*common.TableDiff
	chunkSize  int
	dbConn     *sql.DB
}

func NewTiDBSource(tableDiffs []*common.TableDiff, cfg *config.DBConfig) (Source, error) {
	// TODO build TiDB Source
	ctx := context.Background()
	dbConn, err := common.CreateDB(ctx, &cfg.DBConfig, nil, 4)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &TiDBSource{
		tableDiffs,
		// TODO adjust chunk-size for each table.
		1,
		dbConn,
	}, nil
}

func (s *TiDBSource) GenerateChunks() (chunk.Iterator, error) {
	// TODO build Iterator with config.
	return &TiDBChunksIterator{
		TableDiffs: s.tableDiffs,
		TiDBSplitter: &TiDBSplitter{
			chunkSize: s.chunkSize,
			dbConn:    s.dbConn,
		},
	}, nil
}

func (s *TiDBSource) GetCrc32(chunk *chunk.Range) (string, error) {
	// TODO get crc32 with sql
	return "", nil
}

func (s *TiDBSource) GetRows(chunk *chunk.Range) (RowDataIterator, error) {
	// TODO get rowsdataIter with sql
	return nil, nil
}
