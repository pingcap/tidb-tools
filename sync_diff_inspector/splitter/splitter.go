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
	"fmt"

	"github.com/pingcap/tidb-tools/sync_diff_inspector/checkpoints"
	"github.com/pingcap/tidb-tools/sync_diff_inspector/chunk"
)

const (
	SplitThreshold = 1000
)

// ChunkIterator generate next chunk for only one table lazily.
type ChunkIterator interface {
	// Next seeks the next chunk, return nil if seeks to end.
	Next() (*chunk.Range, error)
	Close()
}

// RangeInfo represents the unit of a process chunk.
// It's the only entrance of checkpoint.
type RangeInfo struct {
	ChunkRange *chunk.Range `json:"chunk-range"`
	TableIndex int          `json:"table-index"`
	// for bucket checkpoint
	IndexID int64 `json:"index-id"`

	ProgressID string `json:"progress-id"`
}

func (r *RangeInfo) GetChunk() *chunk.Range {
	return r.ChunkRange
}

func (r *RangeInfo) Copy() *RangeInfo {
	return &RangeInfo{
		ChunkRange: r.ChunkRange.Clone(),
		TableIndex: r.TableIndex,
		IndexID:    r.IndexID,
	}
}

func (r *RangeInfo) Update(column, lower, upper string, updateLower, updateUpper bool, collation, limits string) {
	r.ChunkRange.Update(column, lower, upper, updateLower, updateUpper)
	conditions, args := r.ChunkRange.ToString(collation)
	r.ChunkRange.Where = fmt.Sprintf("((%s) AND %s)", conditions, limits)
	r.ChunkRange.Args = args
}

// GetTableIndex return the index of table diffs.
// IMPORTANT!!!
// TODO We need to keep the tables order during checkpoint.
// TODO So we should have to save the config info to checkpoint file too.
func (r *RangeInfo) GetTableIndex() int {
	return r.TableIndex
}

func (r *RangeInfo) ToNode() *checkpoints.Node {
	return &checkpoints.Node{
		ChunkRange: r.ChunkRange,
		TableIndex: r.TableIndex,
		BucketID:   r.ChunkRange.BucketID,
		IndexID:    r.IndexID,
	}
}

func FromNode(n *checkpoints.Node) *RangeInfo {
	return &RangeInfo{
		ChunkRange: n.ChunkRange,
		TableIndex: n.TableIndex,
		IndexID:    n.IndexID,
	}
}
