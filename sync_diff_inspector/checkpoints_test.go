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
	checker := new(Checkpointer)
	checker.Init()
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
			checker.hp.mu.Lock()
			heap.Push(checker.hp, node)
			checker.hp.mu.Unlock()
			if i_ != 10 {
				wg.Done()
			}
		}(i)
	}
	wg.Wait()
	id, _ := checker.SaveChunk(ctx)
	c.Assert(id, Equals, 9)
	time.Sleep(5 * time.Second)
	id, _ = checker.SaveChunk(ctx)
	c.Assert(id, Equals, 99)
}
