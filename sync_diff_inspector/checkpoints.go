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

package main

import (
	"container/heap"
	"context"
	"os"
	"sync"

	//"github.com/golang/protobuf/proto"
	//"github.com/pingcap/errors"
	"github.com/pingcap/tidb-tools/sync_diff_inspector/chunk"
)

type ChunkType int

const localFilePerm os.FileMode = 0o644
const (
	Bucket = iota + 1
	Random
	Others
)

// 断点续传： fix sql 要等待 checkpoint 同步？
type Node struct {
	ID int
	// Instance ID ???
	Schema     string
	Table      string
	UpperBound string
	Type       ChunkType
	BucketID   int
	ChunkState string
	// random split chunk 看起来没有必要记录，直接记录对应的 Table，从对应 Table 继续执行
	// or 记录整个 random value 从对应 chunk 处开始执行
	// 断点续传后，使用二分，根据 UpperBound 找到 RandomValue 对应位置，生成 chunks 重新开始校验.
	RandomValue [][]string
}

// 维护一个小根堆，如果插入的值刚好是小根堆的堆顶值+1，那么更新堆顶元素，反之插入到堆中
// 定期进行 checkpoint 操作, 在 checkpoint 时，不断移除连续的堆顶元素，直到出现一个间断的堆顶元素，将该元素写入 json
type Heap struct {
	Nodes []*Node
	mu    sync.Mutex
}
type Checkpointer struct {
	hp *Heap
}

func (cp *Checkpointer) Insert(node *Node) {
	cp.hp.mu.Lock()
	heap.Push(cp.hp, node)
	cp.hp.mu.Unlock()
}

// Len - get the length of the heap
func (hp Heap) Len() int { return len(hp.Nodes) }

// Less - determine which is more priority than another
func (hp Heap) Less(i, j int) bool {
	return hp.Nodes[i].ID < hp.Nodes[j].ID
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
	hp.mu = sync.Mutex{}
	hp.Nodes = make([]*Node, 0)
	heap.Init(hp)
	cp.hp = hp
}

// saveChunk saves the chunk to file.
func (cp *Checkpointer) SaveChunk(ctx context.Context) (int, error) {
	// TODO save Chunk to file

	var cur, next *Node
	cp.hp.mu.Lock()
	for {
		if cp.hp.Len() == 0 {
			break
		}
		cur = heap.Pop(cp.hp).(*Node)
		if cp.hp.Len() == 0 {
			break
		}
		next = cp.hp.Nodes[0]
		if cur.ID+1 != next.ID {
			break
		}
	}
	cp.hp.mu.Unlock()
	if cur != nil {
		//	CheckpointData, err := proto.Marshal(cur)
		//	if err != err {
		//		return errors.Trace(err)
		//	}
		//	WriteFile(checkpointFile, CheckpointData)
	}
	return cur.ID, nil
}

// loadChunks loads chunk info from file `chunk`
func LoadChunks(ctx context.Context, instanceID, schema, table string) ([]*chunk.Range, error) {
	chunks := make([]*chunk.Range, 0, 100)
	// TODO load chunks from files
	return chunks, nil
}
