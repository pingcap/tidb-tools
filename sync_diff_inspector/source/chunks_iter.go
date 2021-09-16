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

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb-tools/pkg/dbutil"
	"github.com/pingcap/tidb-tools/sync_diff_inspector/chunk"
	"github.com/pingcap/tidb-tools/sync_diff_inspector/source/common"
	"github.com/pingcap/tidb-tools/sync_diff_inspector/splitter"
)

// ChunksIterator is used for single mysql/tidb source.
type ChunksIterator struct {
	ID            *chunk.ChunkID
	tableAnalyzer TableAnalyzer

	TableDiffs     []*common.TableDiff
	nextTableIndex int
	chunksCh       chan *splitter.RangeInfo
	errCh          chan error
	limit          int

	cancel context.CancelFunc
}

func NewChunksIterator(ctx context.Context, analyzer TableAnalyzer, tableDiffs []*common.TableDiff, startRange *splitter.RangeInfo) (*ChunksIterator, error) {
	ctxx, cancel := context.WithCancel(ctx)
	iter := &ChunksIterator{
		tableAnalyzer: analyzer,
		TableDiffs:    tableDiffs,
		chunksCh:      make(chan *splitter.RangeInfo, 32),
		errCh:         make(chan error, 1),
		cancel:        cancel,
	}
	go iter.produceChunks(ctxx, startRange)
	return iter, nil
}

func (t *ChunksIterator) produceChunks(ctx context.Context, startRange *splitter.RangeInfo) {
	defer close(t.chunksCh)

	t.nextTableIndex = 0
	if startRange != nil {
		curIndex := startRange.GetTableIndex()
		curTable := t.TableDiffs[curIndex]
		t.nextTableIndex = curIndex + 1
		chunkIter, err := t.tableAnalyzer.AnalyzeSplitter(ctx, curTable, startRange)
		if err != nil {
			t.errCh <- errors.Trace(err)
			return
		}
		for {
			c, err := chunkIter.Next()
			if err != nil {
				t.errCh <- errors.Trace(err)
				return
			}
			if c == nil {
				break
			}
			c.Index.TableIndex = curIndex
			select {
			case <-ctx.Done():
				log.Info("Stop do produce chunks by context done")
				return
			case t.chunksCh <- &splitter.RangeInfo{
				ChunkRange: c,
				IndexID:    getCurTableIndexID(chunkIter),
				ProgressID: startRange.ProgressID,
			}:
			}
		}
	}

	for ; t.nextTableIndex < len(t.TableDiffs); t.nextTableIndex++ {
		table := t.TableDiffs[t.nextTableIndex]
		chunkIter, err := t.tableAnalyzer.AnalyzeSplitter(ctx, table, nil)
		if err != nil {
			t.errCh <- errors.Trace(err)
			return
		}
		for {
			c, err := chunkIter.Next()
			if err != nil {
				t.errCh <- errors.Trace(err)
				return
			}
			if c == nil {
				break
			}
			c.Index.TableIndex = t.nextTableIndex
			select {
			case <-ctx.Done():
				log.Info("Stop do produce chunks by context done")
				return
			case t.chunksCh <- &splitter.RangeInfo{
				ChunkRange: c,
				IndexID:    getCurTableIndexID(chunkIter),
				ProgressID: dbutil.TableName(table.Schema, table.Table),
			}:
			}
		}
	}
}

func (t *ChunksIterator) Next(ctx context.Context) (*splitter.RangeInfo, error) {
	select {
	case <-ctx.Done():
		return nil, nil
	case r, ok := <-t.chunksCh:
		if !ok && r == nil {
			return nil, nil
		}
		return r, nil
	case err := <-t.errCh:
		return nil, errors.Trace(err)
	}
}

func (t *ChunksIterator) Close() {
	t.cancel()
}

func (t *ChunksIterator) getCurTableIndex() int {
	return t.nextTableIndex - 1
}

func getCurTableIndexID(tableIter splitter.ChunkIterator) int64 {
	if bt, ok := tableIter.(*splitter.BucketIterator); ok {
		return bt.GetIndexID()
	}
	return 0
}
