package main

import (
	"container/heap"
	"context"
	"sync"
	"time"

	. "github.com/pingcap/check"
)

var _ = Suite(&testCheckpointSuit{})

type testCheckpointSuit struct{}

func (cp *testCheckpointSuit) TestSaveChunk(c *C) {
	hp := new(Heap)
	hp.mu = sync.Mutex{}
	hp.Nodes = make([]*Node, 0)
	heap.Init(hp)
	ctx := context.Background()
	wg := &sync.WaitGroup{}
	wg.Add(99)
	for i := 0; i < 100; i++ {
		go func(i_ int) {
			node := &Node{
				ID: i_,
			}
			if i_ == 10 {
				time.Sleep(5 * time.Second)
			}
			hp.mu.Lock()
			heap.Push(hp, node)
			hp.mu.Unlock()
			if i_ != 10 {
				wg.Done()
			}
		}(i)
	}
	wg.Wait()
	id, _ := SaveChunk(ctx, hp)
	c.Assert(id, Equals, 9)
	time.Sleep(5 * time.Second)
	id, _ = SaveChunk(ctx, hp)
	c.Assert(id, Equals, 99)
}
