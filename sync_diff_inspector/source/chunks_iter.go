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
	"github.com/pingcap/tidb-tools/pkg/dbutil"
	"github.com/pingcap/tidb-tools/sync_diff_inspector/chunk"
	"github.com/pingcap/tidb-tools/sync_diff_inspector/source/common"
	"github.com/pingcap/tidb-tools/sync_diff_inspector/splitter"
)

// ChunksIterator is used for single mysql/tidb source.
type ChunksIterator struct {
	ID            *chunk.ChunkID
	currentID     int
	tableAnalyzer TableAnalyzer

	TableDiffs     []*common.TableDiff
	nextTableIndex int

	limit int

	tableIter  splitter.ChunkIterator
	progressID string
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
		t.currentID++
		c.ID = t.currentID
		c.Index.TableIndex = curIndex
		return &splitter.RangeInfo{
			ChunkRange: c,
			TableIndex: curIndex,
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
	t.currentID++
	c.ID = t.currentID
	c.Index.TableIndex = curIndex
	return &splitter.RangeInfo{
		ChunkRange: c,
		TableIndex: curIndex,
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

// if error is nil and t.iter is not nil,
// then nextTable is done successfully.
func (t *ChunksIterator) nextTable(ctx context.Context, startRange *splitter.RangeInfo) error {
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

	chunkIter, err := t.tableAnalyzer.AnalyzeSplitter(ctx, t.progressID, curTable, startRange)
	if err != nil {
		return errors.Trace(err)
	}
	if t.tableIter != nil {
		t.tableIter.Close()
	}
	t.tableIter = chunkIter
	return nil
}
