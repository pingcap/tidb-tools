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

	"github.com/pingcap/tidb-tools/sync_diff_inspector/chunk"
	"github.com/siddontang/go/ioutil2"

	//"github.com/golang/protobuf/proto"
	//"github.com/pingcap/errors"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

const LocalFilePerm os.FileMode = 0o644

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

func (n *Node) GetID() int { return n.ChunkRange.ID }

func (n *Node) GetState() string { return n.State }

// Heap maintain a Min Heap, which can be accessed by multiple threads and protected by mutex.
type Heap struct {
	Nodes          []*Node
	CurrentSavedID int         // CurrentSavedID save the lastest save chunk id, initially was 0, updated by saveChunk method
	mu             *sync.Mutex // protect critical section
}

type Checkpoint struct {
	hp *Heap
}

// SetCurrentSavedID the method is unsynchronized, be cautious
func (cp *Checkpoint) SetCurrentSavedID(id int) {
	cp.hp.CurrentSavedID = id
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
	return hp.Nodes[i].GetID() < hp.Nodes[j].GetID()
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
	hp.CurrentSavedID = -1
	heap.Init(hp)
	cp.hp = hp
}

// SaveChunk saves the chunk to file.
func (cp *Checkpoint) SaveChunk(ctx context.Context, fileName string) (int, error) {
	cp.hp.mu.Lock()
	var cur, next *Node
	for {
		nextId := cp.hp.CurrentSavedID + 1
		if cp.hp.Len() == 0 {
			break
		}
		if nextId == cp.hp.Nodes[0].GetID() {
			cur = heap.Pop(cp.hp).(*Node)
			cp.hp.CurrentSavedID = cur.GetID()
			if cp.hp.Len() == 0 {
				break
			}
			next = cp.hp.Nodes[0]
			if cur.GetID()+1 != next.GetID() {
				break
			}
		} else {
			break
		}
	}
	cp.hp.mu.Unlock()
	if cur != nil {
		log.Info("save checkpoint",
			zap.Int("id", cur.GetID()),
			zap.Reflect("chunk", cur),
			zap.String("state", cur.GetState()))
		tableIndex := cur.TableIndex
		checkpointData, err := json.Marshal(cur)
		if err != nil {
			log.Warn("fail to save the chunk to the file", zap.Int("id", cur.GetID()))
			return 0, errors.Trace(err)
		}

		if err = ioutil2.WriteFileAtomic(fileName, checkpointData, LocalFilePerm); err != nil {
			return 0, err
		}
		return tableIndex, nil
	}
	return -1, nil
}

// LoadChunk loads chunk info from file `chunk`
func (cp *Checkpoint) LoadChunk(fileName string) (*Node, error) {
	bytes, err := os.ReadFile(fileName)
	if err != nil {
		return nil, errors.Trace(err)
	}
	n := &Node{}
	err = json.Unmarshal(bytes, n)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return n, nil
}

type CheckConfig struct {
	Table     string `json:"tables"`
	Fields    string `json:"fields"`
	Range     string `json:"range"`
	Snapshot  string `json:"snapshot"`
	Collation string `json:"collation"`
}
