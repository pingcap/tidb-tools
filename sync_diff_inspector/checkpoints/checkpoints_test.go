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
	"context"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb-tools/sync_diff_inspector/chunk"
)

func TestClient(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testCheckpointSuit{})

type testCheckpointSuit struct{}

func (cp *testCheckpointSuit) TestSaveChunk(c *C) {
	checker := new(Checkpoint)
	checker.Init()
	ctx := context.Background()
	cur := checker.GetChunkSnapshot()
	id, err := checker.SaveChunk(ctx, "TestSaveChunk", cur, nil)
	c.Assert(err, IsNil)
	c.Assert(id, Equals, 0)
	wg := &sync.WaitGroup{}
	rounds := 100
	for i := 1; i < rounds; i++ {
		wg.Add(1)
		go func(i int) {
			node := &Node{
				ChunkRange: &chunk.Range{
					ID: i,
					Bounds: []*chunk.Bound{
						{
							HasLower: i != 1,
							Lower:    strconv.Itoa(i + 1000),
							Upper:    strconv.Itoa(i + 1000 + 1),
							HasUpper: i != rounds,
						},
					},
				},

				BucketID: i,
				State:    SuccessState,
			}
			if rand.Intn(4) == 0 {
				time.Sleep(time.Duration(rand.Intn(3)) * time.Second)
			}
			checker.Insert(node)
			wg.Done()
		}(i)
	}
	wg.Wait()
	defer os.Remove("TestSaveChunk")

	cur = checker.GetChunkSnapshot()
	id, err = checker.SaveChunk(ctx, "TestSaveChunk", cur, nil)
	c.Assert(err, IsNil)
	c.Assert(id, Equals, 99)
}

func (cp *testCheckpointSuit) TestLoadChunk(c *C) {
	checker := new(Checkpoint)
	checker.Init()
	ctx := context.Background()
	rounds := 100
	wg := &sync.WaitGroup{}
	for i := 1; i < rounds; i++ {
		wg.Add(1)
		go func(i int) {
			node := &Node{
				ChunkRange: &chunk.Range{
					ID: i,
					Bounds: []*chunk.Bound{
						{
							HasLower: i != 1,
							Lower:    strconv.Itoa(i + 1000),
							Upper:    strconv.Itoa(i + 1000 + 1),
							HasUpper: i != rounds,
						},
					},
				},
			}
			checker.Insert(node)
			wg.Done()
		}(i)
	}
	wg.Wait()
	defer os.Remove("TestLoadChunk")
	cur := checker.GetChunkSnapshot()
	id, err := checker.SaveChunk(ctx, "TestLoadChunk", cur, nil)
	c.Assert(err, IsNil)
	node, _, err := checker.LoadChunk("TestLoadChunk")
	c.Assert(err, IsNil)
	c.Assert(node.GetID(), Equals, id)
}
