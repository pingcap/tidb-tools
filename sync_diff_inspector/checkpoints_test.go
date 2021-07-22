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
	wg := &sync.WaitGroup{}
	wg.Add(9999)
	for i := 1; i < 10000; i++ {
		go func(i_ int) {
			node := &Node{
				ID: i_,
			}
			if i_ < 1000 {
				time.Sleep(1 * time.Second)
			}
			if rand.Intn(4) == 0 {
				time.Sleep(2 * time.Second)
			}
			fmt.Printf("Insert %d\n", i_)
			checker.Insert(node)
			wg.Done()
		}(i)
	}
	wg.Wait()
	id, _ := checker.SaveChunk(ctx)
	c.Assert(id, Equals, 9999)
}

//func (cp *testCheckpointSuit) TestSaveChunk2(c *C) {
//	checker := new(Checkpointer)
//	checker.Init()
//	ctx := context.Background()
//	node4 := &Node{
//		ID: 4,
//	}
//	checker.Insert(node4)
//	go func() {
//		time.Sleep(5 * time.Second)
//		node1 := &Node{ID: 1}
//		node2 := &Node{ID: 2}
//		checker.Insert(node1)
//		checker.Insert(node2)
//	}()
//	id, _ := checker.SaveChunk(ctx)
//	c.Assert(id, Equals, 0)
//	time.Sleep(6 * time.Second)
//	id, _ = checker.SaveChunk(ctx)
//	c.Assert(id, Equals, 2)

//}
