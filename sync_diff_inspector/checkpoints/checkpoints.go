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
	"fmt"
	"os"
	"strconv"
	"sync"

	//"github.com/golang/protobuf/proto"
	//"github.com/pingcap/errors"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb-tools/pkg/dbutil"
	"github.com/pingcap/tidb-tools/sync_diff_inspector/chunk"
	"go.uber.org/zap"
)

const localFilePerm os.FileMode = 0o644

type Inner struct {
	Type       chunk.ChunkType `json:"type"`
	ID         int             `json:"chunk-id"`
	Schema     string          `json:"schema"`
	Table      string          `json:"table"`
	UpperBound []string        `json:"upper-bound"` // the upper bound should be like "(a, b, c)"
	ColumnName []string        `json:"column-names"`
	ChunkState string          `json:"chunk-state"` // indicate the state ("success" or "failed") of the chunk
}
type BucketNode struct {
	Inner
	BucketID int   `json:"bucket-id"`
	IndexID  int64 `json:"index-id"`
}

type RandomNode struct {
	Inner
}

//func (n *BucketNode) MarshalJSON() ([]byte, error) {
//	str := fmt.Sprintf(`{"type":%d, "chunk-id":%d,"schema":"%s","table":"%s","upper-bound":"%s","chunck-state":"%s","bucket-id":%d}`, n.GetType(), n.GetID(), n.GetSchema(), n.GetTable(), n.GetUpperBound(), n.GetChunkState(), n.GetBucketID())
//	fmt.Printf("%s\n", str)
//	return []byte(str), nil
//}

//func (n *BucketNode) UnmarshalJSON(data []byte) error {
//	err := json.Unmarshal(data, &n.ID)
//	if err != nil {
//		return errors.Trace(err)
//	}
//	err = json.Unmarshal(data, &n.Schema)
//	if err != nil {
//		return errors.Trace(err)
//	}
//	err = json.Unmarshal(data, &n.Table)
//	if err != nil {
//		return errors.Trace(err)
//	}
//	err = json.Unmarshal(data, &n.UpperBound)
//	if err != nil {
//		return errors.Trace(err)
//	}
//	err = json.Unmarshal(data, &n.ChunkState)
//	if err != nil {
//		return errors.Trace(err)
//	}
//	err = json.Unmarshal(data, &n.BucketID)
//	if err != nil {
//		return errors.Trace(err)
//	}
//	return nil
//}

func (n *BucketNode) GetBucketID() int {
	return n.BucketID
}

//func (n RandomNode) MarshalJSON() ([]byte, error) {
//	// TODO: random value type is [][]string, this methoad will be updated when implement LoadChunk method
//	str := fmt.Sprintf(`{"type":%d, "chunk-id":%d, "schema":"%s", "table":"%s", "upper-bound":"%s","chunck-state":"%s"}`, n.Type, n.ID, n.Schema, n.Table, n.UpperBound, n.ChunkState)
//	return []byte(str), nil
//}

type Node interface {
	GetID() int
	GetSchema() string
	GetTable() string
	GetUpperBound() []string
	GetType() chunk.ChunkType
	GetChunkState() string
}

func (n *Inner) GetID() int { return n.ID }

func (n *Inner) GetSchema() string { return n.Schema }

func (n *Inner) GetTable() string { return n.Table }

func (n *Inner) GetUpperBound() []string { return n.UpperBound }

func (n *Inner) GetType() chunk.ChunkType { return n.Type }

func (n *Inner) GetChunkState() string { return n.ChunkState }

func (n *Inner) GetColumnName() []string { return n.ColumnName }

// Heap maintain a Min Heap, which can be accessed by multiple threads and protected by mutex.
type Heap struct {
	Nodes          []Node
	CurrentSavedID int         // CurrentSavedID save the lastest save chunk id, initially was 0, updated by saveChunk method
	mu             *sync.Mutex // protect critical section
}
type Checkpointer struct {
	hp *Heap
	// TODO close the channel
	NodeChan chan Node
}

// the method is unsynchronized, be cautious
func (cp *Checkpointer) SetCurrentSavedID(id int) {
	cp.hp.CurrentSavedID = id
}

func (cp *Checkpointer) Insert(node Node) {
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
	hp.Nodes = append(hp.Nodes, x.(Node))
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

var (

	// checkpointFile represents the checkpoints' file name which used for save and loads chunks
	checkpointFile = "sync_diff_checkpoints.pb"

	// for chunk: means this chunk's data is equal
	// for table: means this all chunk in this table is equal(except ignore chunk)
	successState = "success"

	// for chunk: means this chunk's data is not equal
	// for table: means some chunks' data is not equal or some chunk check failed in this table
	failedState = "failed"

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

func WriteFile(path string, data []byte) error {
	return os.WriteFile(path, data, localFilePerm)
}

func (cp *Checkpointer) Init() {
	hp := new(Heap)
	hp.mu = &sync.Mutex{}
	hp.Nodes = make([]Node, 0)
	hp.CurrentSavedID = 0
	heap.Init(hp)
	cp.hp = hp
	cp.NodeChan = make(chan Node, 1024)
}

// saveChunk saves the chunk to file.
func (cp *Checkpointer) SaveChunk(ctx context.Context) (int, error) {
	// TODO save Chunk to file
	cp.hp.mu.Lock()
	var cur, next Node
	for {
		next_id := cp.hp.CurrentSavedID + 1
		if cp.hp.Len() == 0 {
			break
		}
		if next_id == cp.hp.Nodes[0].GetID() {
			cur = heap.Pop(cp.hp).(Node)
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
		var CheckpointData []byte
		var err error
		switch cur := cur.(type) {
		case *BucketNode, *RandomNode:
			CheckpointData, err = json.Marshal(cur)
			if err != nil {
				return 0, errors.Trace(err)
			}
		default:
			panic("error")
		}
		if err := WriteFile(checkpointFile, CheckpointData); err != nil {
			return 0, err
		}
		log.Info("load checkpoint",
			zap.Int("id", cur.GetID()),
			zap.String("table", dbutil.TableName(cur.GetSchema(), cur.GetTable())),
			zap.Reflect("type", cur.GetType()),
			zap.String("state", cur.GetChunkState()))
		return cur.GetID(), nil
	}
	return 0, nil

}

// loadChunks loads chunk info from file `chunk`
func (cp *Checkpointer) LoadChunks() (Node, error) {
	//chunks := make([]*chunk.Range, 0, 100)
	bytes, err := os.ReadFile(checkpointFile)
	if err != nil {
		return nil, errors.Trace(err)
	}
	bytes_copy := make([]byte, len(bytes))
	copy(bytes_copy, bytes)
	//str := string(bytes)
	// TODO find a better way
	m := make(map[string]interface{})
	err = json.Unmarshal(bytes_copy, &m)
	//t, err := strconv.Atoi(str[strings.Index(str, `"type"`)+len(`"type"`)+1 : strings.Index(str, `"type"`)+len(`"type"`)+2])
	if err != nil {
		return nil, errors.Trace(err)
	}
	t, err := strconv.Atoi(fmt.Sprint(m["type"]))
	if err != nil {
		return nil, errors.Trace(err)
	}

	switch t {
	case int(chunk.Bucket):
		node := &BucketNode{}
		err := json.Unmarshal(bytes, &node)
		if err != nil {
			fmt.Printf("%s\n", err.Error())
			return nil, errors.Trace(err)
		}
		return node, nil
	case int(chunk.Random):
		node := &RandomNode{}
		err := json.Unmarshal(bytes, &node)
		if err != nil {
			return nil, errors.Trace(err)
		}
		return node, nil
	default:
		panic("LoadChunk error")
	}
	// TODO load chunks from files
}
