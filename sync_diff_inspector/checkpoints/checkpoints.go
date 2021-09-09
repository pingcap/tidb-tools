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

package checkpoints

import (
	"container/heap"
	"context"
	"encoding/json"
	"os"
	"sync"

	"github.com/pingcap/tidb-tools/sync_diff_inspector/config"
	"github.com/pingcap/tidb-tools/sync_diff_inspector/report"

	"github.com/pingcap/tidb-tools/sync_diff_inspector/chunk"
	"github.com/siddontang/go/ioutil2"

	//"github.com/golang/protobuf/proto"
	//"github.com/pingcap/errors"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

var (

	// SuccessState
	// for chunk: means this chunk's data is equal
	// for table: means this all chunk in this table is equal(except ignore chunk)
	SuccessState = "success"

	// FailedState
	// for chunk: means this chunk's data is not equal
	// for table: means some chunks' data is not equal or some chunk check failed in this table
	FailedState = "failed"

	// for chunk: means meet error when check, don't know the chunk's data is equal or not equal
	// for table: don't have this state
	errorState = "error"

	// for chunk: means this chunk is not in check
	// for table: this table is checking but not finished
	notCheckedState = "not_checked"

	// for chunk: means this chunk is checking
	// for table: don't have this state
	checkingState = "checking"

	// for chunk: this chunk is ignored. if sample is not 100%, will ignore some chunk
	// for table: don't have this state
	ignoreState = "ignore"
)

type Node struct {
	State string `json:"state"` // indicate the state ("success" or "failed") of the chunk

	ChunkRange *chunk.Range `json:"chunk-range"`
	TableIndex int          `json:"table-index"`
	// for bucket checkpoint
	BucketID int   `json:"bucket-id"`
	IndexID  int64 `json:"index-id"`
}

func (n *Node) GetID() *chunk.ChunkID { return n.ChunkRange.Index }

func (n *Node) GetState() string { return n.State }

// IsAdjacent represents whether the next node is adjacent node.
// it's the important logic for checkpoint update.
// we need keep this node save to checkpoint in global order.
func (n *Node) IsAdjacent(next *Node) bool {
	if n.ChunkRange.Index.TableIndex == next.ChunkRange.Index.TableIndex-1 {
		if n.ChunkRange.IsLastChunkForTable() && next.ChunkRange.IsFirstChunkForTable() {
			return true
		}
	}
	if n.ChunkRange.Index.TableIndex == next.ChunkRange.Index.TableIndex {
		// same table
		if n.ChunkRange.Index.BucketIndex == next.ChunkRange.Index.BucketIndex-1 {
			if n.ChunkRange.IsLastChunkForBucket() && next.ChunkRange.IsFirstChunkForBucket() {
				return true
			}
			return false
		}
		if n.ChunkRange.Index.BucketIndex == next.ChunkRange.Index.BucketIndex {
			return n.ChunkRange.Index.ChunkIndex == next.ChunkRange.Index.ChunkIndex-1
		}
		return false
	}
	return false
}

// IsLess represents whether the cur node is less than next node.
func (n *Node) IsLess(next *Node) bool {
	if n.ChunkRange.Index.TableIndex <= next.ChunkRange.Index.TableIndex-1 {
		return true
	}
	if n.ChunkRange.Index.TableIndex == next.ChunkRange.Index.TableIndex {
		if n.ChunkRange.Index.BucketIndex <= next.ChunkRange.Index.BucketIndex-1 {
			return true
		}
		if n.ChunkRange.Index.BucketIndex == next.ChunkRange.Index.BucketIndex {
			return n.ChunkRange.Index.ChunkIndex < next.ChunkRange.Index.ChunkIndex
		}
		return false
	}
	return false
}

// Heap maintain a Min Heap, which can be accessed by multiple threads and protected by mutex.
type Heap struct {
	Nodes            []*Node
	CurrentSavedNode *Node       // CurrentSavedID save the lastest save chunk, initially was 0, updated by saveChunk method
	mu               *sync.Mutex // protect critical section
}

type Checkpoint struct {
	hp       *Heap
	ChunkMap map[uint]map[uint]uint // ChunkMap records the maps of TableIndex => BucketIndex => ChunkIndex
}

type SavedState struct {
	Chunk  *Node          `json:"chunk-info"`
	Report *report.Report `json:"report-info"`
}

// SetCurrentSavedID the method is unsynchronized, be cautious
func (cp *Checkpoint) SetCurrentSavedID(n *Node) {
	cp.hp.CurrentSavedNode = n
}

func (cp *Checkpoint) GetCurrentSavedID() *Node {
	cp.hp.mu.Lock()
	defer cp.hp.mu.Unlock()
	return cp.hp.CurrentSavedNode
}

func (cp *Checkpoint) Insert(node *Node) {
	cp.hp.mu.Lock()
	heap.Push(cp.hp, node)
	cp.hp.mu.Unlock()
}

// Len - get the length of the heap
func (hp Heap) Len() int { return len(hp.Nodes) }

// Less - determine which is more priority than another
func (hp Heap) Less(i, j int) bool {
	return hp.Nodes[i].IsLess(hp.Nodes[j])
}

// Swap - implementation of swap for the heap interface
func (hp Heap) Swap(i, j int) {
	hp.Nodes[i], hp.Nodes[j] = hp.Nodes[j], hp.Nodes[i]
}

// Push - implementation of push for the heap interface
func (hp *Heap) Push(x interface{}) {
	hp.Nodes = append(hp.Nodes, x.(*Node))
}

// Pop - implementation of pop for heap interface
func (hp *Heap) Pop() interface{} {
	if len(hp.Nodes) == 0 {
		return nil
	}
	old := hp.Nodes
	n := len(old)
	item := old[n-1]
	hp.Nodes = old[0 : n-1]
	return item
}

func (cp *Checkpoint) Init() {
	hp := new(Heap)
	hp.mu = &sync.Mutex{}
	hp.Nodes = make([]*Node, 0)
	hp.CurrentSavedNode = nil
	heap.Init(hp)
	cp.hp = hp
}

func (cp *Checkpoint) GetChunkSnapshot() *Node {
	cp.hp.mu.Lock()
	defer cp.hp.mu.Unlock()
	var cur, next *Node
	for cp.hp.Len() != 0 {
		if cp.hp.CurrentSavedNode != nil {
		}
		if cp.hp.CurrentSavedNode == nil || cp.hp.CurrentSavedNode.IsAdjacent(cp.hp.Nodes[0]) {
			cur = heap.Pop(cp.hp).(*Node)
			cp.hp.CurrentSavedNode = cur
			if cp.hp.Len() == 0 {
				break
			}
			next = cp.hp.Nodes[0]
			if !cur.IsAdjacent(next) {
				// wait for next 10s to check
				break
			}
		} else {
			// wait for next 10s to check
			break
		}
	}
	return cur
}

// SaveChunk saves the chunk to file.
func (cp *Checkpoint) SaveChunk(ctx context.Context, fileName string, cur *Node, reportInfo *report.Report) (*chunk.ChunkID, error) {
	if cur != nil {

		savedState := &SavedState{
			Chunk:  cur,
			Report: reportInfo,
		}
		checkpointData, err := json.Marshal(savedState)
		if err != nil {
			log.Warn("fail to save the chunk to the file", zap.Any("chunk index", cur.GetID()), zap.Error(err))
			return nil, errors.Trace(err)
		}

		if err = ioutil2.WriteFileAtomic(fileName, checkpointData, config.LocalFilePerm); err != nil {
			return nil, err
		}
		log.Info("save checkpoint",
			zap.Any("chunk", cur),
			zap.String("state", cur.GetState()))
		return cur.GetID(), nil
	}
	return nil, nil
}

// LoadChunk loads chunk info from file `chunk`
func (cp *Checkpoint) LoadChunk(fileName string) (*Node, *report.Report, error) {
	bytes, err := os.ReadFile(fileName)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	n := &SavedState{}
	err = json.Unmarshal(bytes, n)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	return n.Chunk, n.Report, nil
}

type CheckConfig struct {
	Table     string `json:"tables"`
	Fields    string `json:"fields"`
	Range     string `json:"range"`
	Snapshot  string `json:"snapshot"`
	Collation string `json:"collation"`
}
