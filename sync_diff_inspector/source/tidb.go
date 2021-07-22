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

// TiDBChunksIterator iterate chunks in tables sequence
type TiDBChunksIterator struct {
	TableDiffs          []*common.TableDiff
	curTableIndex       int
	curTableHasSplitted bool

	chunkSize int
	limit     int

	dbConn *sql.DB

	iter splitter.Iterator
}

func (t *TiDBChunksIterator) Next() (*chunk.Range, error) {
	if !t.curTableHasSplitted {
		curTable := t.TableDiffs[t.curTableIndex]

		chunkIter, err := t.splitChunksForTable(curTable)
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

// useBucket returns the tableInstance that can use bucket info whether in source or target.
func (s *TiDBChunksIterator) useBucket(diff *common.TableDiff) bool {
	// TODO check whether we can use bucket for this table to split chunks.
	return true
}

func (s *TiDBChunksIterator) splitChunksForTable(tableDiff *common.TableDiff) (splitter.Iterator, error) {
	if s.useBucket(tableDiff) {
		bucketIter, err := splitter.NewBucketIterator(tableDiff, s.dbConn)
		if err != nil {
			return nil, errors.Trace(err)
		}

		return bucketIter, nil
		// TODO fall back to random splitter
	}
	// use random splitter if we cannot use bucket splitter, then we can simply choose target table to generate chunks.
	randIter, err := splitter.NewRandomIterator(tableDiff, s.dbConn, s.chunkSize, tableDiff.Range, tableDiff.Collation)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return randIter, nil
}

type TiDBSource struct {
	tableDiffs []*common.TableDiff
	dbConn     *sql.DB
}

func NewTiDBSource(tableDiffs []*common.TableDiff, dbCfg *config.DBConfig) (Source, error) {
	// TODO build TiDB Source
	ctx := context.Background()
	dbConn, err := common.CreateDB(ctx, &dbCfg.DBConfig, nil, 4)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &TiDBSource{
		tableDiffs,
		dbConn,
	}, nil
}

func (s *TiDBSource) GenerateChunksIterator() (DBIterator, error) {
	// TODO build Iterator with config.
	return &TiDBChunksIterator{
		TableDiffs:          s.tableDiffs,
		curTableIndex:       0,
		curTableHasSplitted: false,
		chunkSize:           0,
		limit:               0,
		dbConn:              s.dbConn,
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
