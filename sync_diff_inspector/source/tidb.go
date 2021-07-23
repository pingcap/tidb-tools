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
	TableDiffs     []*common.TableDiff
	nextTableIndex int

	chunkSize int
	limit     int

	dbConn *sql.DB

	iter splitter.Iterator
}

func (t *TiDBChunksIterator) Next() (*chunk.Range, error) {
	if t.iter == nil {
		return nil, nil
	}
	chunk, err := t.iter.Next()
	if err != nil {
		return nil, errors.Trace(err)
	}
	return chunk, nil
}

func (t *TiDBChunksIterator) Close() {
	t.iter.Close()
}

func (t *TiDBChunksIterator) NextTable() (bool, error) {
	if t.nextTableIndex >= len(t.TableDiffs) {
		return false, nil
	}
	curTable := t.TableDiffs[t.nextTableIndex]
	t.nextTableIndex++
	chunkIter, err := t.splitChunksForTable(curTable)
	if err != nil {
		return false, errors.Trace(err)
	}
	if t.iter != nil {
		t.iter.Close()
	}
	t.iter = chunkIter
	return true, nil
}

// useBucket returns the tableInstance that can use bucket info whether in source or target.
func (s *TiDBChunksIterator) useBucket(diff *common.TableDiff) bool {
	// TODO check whether we can use bucket for this table to split chunks.
	return true
}

func (s *TiDBChunksIterator) splitChunksForTable(tableDiff *common.TableDiff) (splitter.Iterator, error) {
	chunkSize := 1000
	if s.useBucket(tableDiff) {
		bucketIter, err := splitter.NewBucketIterator(tableDiff, s.dbConn, chunkSize)
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
		TableDiffs:     s.tableDiffs,
		nextTableIndex: 0,
		chunkSize:      0,
		limit:          0,
		dbConn:         s.dbConn,
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
