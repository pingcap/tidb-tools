package main

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	. "github.com/pingcap/check"
)

func TestClient(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testCheckpointSuit{})

type testCheckpointSuit struct{}

func (cp *testCheckpointSuit) TestSaveChunk(c *C) {
	checker := new(Checkpointer)
	checker.Init()
	ctx := context.Background()
	id, _ := checker.SaveChunk(ctx)
	c.Assert(id, Equals, 0)
	wg := &sync.WaitGroup{}
	wg.Add(9999)
	for i := 1; i < 10000; i++ {
		go func(i_ int) {
			node := &BucketNode{
				Node:     Node{ID: i_},
				BucketID: i_,
			}
			if rand.Intn(4) == 0 {
				time.Sleep(time.Duration(rand.Intn(3)) * time.Second)
			}
			fmt.Printf("Insert %d\n", i_)
			checker.Insert(node)
			wg.Done()
		}(i)
	}
	wg.Wait()
	id, _ = checker.SaveChunk(ctx)
	c.Assert(id, Equals, 9999)
}
