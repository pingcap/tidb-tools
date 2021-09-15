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
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb-tools/pkg/dbutil"
	"github.com/pingcap/tidb-tools/sync_diff_inspector/chunk"
	"github.com/pingcap/tidb-tools/sync_diff_inspector/source/common"
	"github.com/pingcap/tidb-tools/sync_diff_inspector/splitter"
	"go.uber.org/zap"
)

// ChunksIterator is used for single mysql/tidb source.
type ChunksIterator struct {
	ID            *chunk.ChunkID
	tableAnalyzer TableAnalyzer

	TableDiffs     []*common.TableDiff
	nextTableIndex int
	chunkIterCh    chan *splitter.ChunkIterator
	errCh          chan error
	limit          int

	tableIter  splitter.ChunkIterator
	progressID string
}

func (t *ChunksIterator) produceChunkIter(ctx context.Context, startRange *splitter.RangeInfo) {
	var i int
	if startRange == nil {
		i = 1
	} else {
		i = startRange.GetTableIndex() + 1
	}
	for ; i < len(t.TableDiffs); i++ {
		table := t.TableDiffs[i]
		startTime := time.Now()
		chunkIter, err := t.tableAnalyzer.AnalyzeSplitter(ctx, table, nil)
		if err != nil {
			t.errCh <- err
			return
		}
		log.Debug("initialize table", zap.Duration("time cost", time.Since(startTime)))
		t.chunkIterCh <- &chunkIter
	}
}

func (t *ChunksIterator) Next(ctx context.Context) (*splitter.RangeInfo, error) {
	// TODO: creates different tables chunks in parallel
	if t.tableIter == nil {
		return nil, nil
	}
	c, err := t.tableIter.Next()
	if err != nil {
		return nil, errors.Trace(err)
	}
	if c != nil {
		curIndex := t.getCurTableIndex()
		c.Index.TableIndex = curIndex
		return &splitter.RangeInfo{
			ChunkRange: c,
			IndexID:    t.getCurTableIndexID(),
			ProgressID: t.progressID,
		}, nil
	}
	err = t.nextTable(ctx, nil)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if t.tableIter == nil {
		return nil, nil
	}
	c, err = t.tableIter.Next()
	if err != nil {
		return nil, errors.Trace(err)
	}
	curIndex := t.getCurTableIndex()
	c.Index.TableIndex = curIndex
	return &splitter.RangeInfo{
		ChunkRange: c,
		IndexID:    t.getCurTableIndexID(),
		ProgressID: t.progressID,
	}, nil
}

func (t *ChunksIterator) Close() {
	if t.tableIter != nil {
		t.tableIter.Close()
	}
}

func (t *ChunksIterator) getCurTableIndex() int {
	return t.nextTableIndex - 1
}

func (t *ChunksIterator) getCurTableIndexID() int64 {
	if bt, ok := t.tableIter.(*splitter.BucketIterator); ok {
		return bt.GetIndexID()
	}
	return 0
}

func (t *ChunksIterator) initTable(ctx context.Context, startRange *splitter.RangeInfo) error {
	if t.nextTableIndex >= len(t.TableDiffs) {
		t.tableIter = nil
		return nil
	}
	curTable := t.TableDiffs[t.nextTableIndex]
	t.nextTableIndex++
	t.progressID = dbutil.TableName(curTable.Schema, curTable.Table)

	// reads table index from checkpoint at the beginning
	if startRange != nil {
		curIndex := startRange.GetTableIndex()
		curTable = t.TableDiffs[curIndex]
		t.nextTableIndex = curIndex + 1
		t.progressID = startRange.ProgressID
	}
	chunkIter, err := t.tableAnalyzer.AnalyzeSplitter(ctx, curTable, startRange)
	if err != nil {
		return errors.Trace(err)
	}
	if t.tableIter != nil {
		t.tableIter.Close()
	}
	t.tableIter = chunkIter
	go t.produceChunkIter(ctx, startRange)
	return nil
}

// if error is nil and t.iter is not nil,
// then nextTable is done successfully.
func (t *ChunksIterator) nextTable(ctx context.Context, startRange *splitter.RangeInfo) error {
	if t.nextTableIndex >= len(t.TableDiffs) {
		t.tableIter = nil
		close(t.chunkIterCh)
		close(t.errCh)
		return nil
	}
	curTable := t.TableDiffs[t.nextTableIndex]
	t.nextTableIndex++
	t.progressID = dbutil.TableName(curTable.Schema, curTable.Table)
	if t.tableIter != nil {
		t.tableIter.Close()
	}
	for {
		select {
		case c, ok := <-t.chunkIterCh:
			if !ok {
				t.tableIter = nil
				return nil
			}
			t.tableIter = *c
			return nil
		case err := <-t.errCh:
			return errors.Trace(err)
		}
	}
}
