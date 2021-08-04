package checkpoints

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
	checker := new(Checkpoint)
	checker.Init()
	ctx := context.Background()
	id, err := checker.SaveChunk(ctx, "TestSaveChunk")
	c.Assert(err, IsNil)
	c.Assert(id, Equals, 0)
	wg := &sync.WaitGroup{}
	rounds := 10000
	for i := 1; i < rounds; i++ {
		wg.Add(1)
		go func(i_ int) {
			node := &Node{
				BucketID: i_,
				State:    SuccessState,
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
	id, err = checker.SaveChunk(ctx, "TestSaveChunk")
	c.Assert(err, IsNil)
	c.Assert(id, Equals, 9999)
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
			node := &Node{}
			checker.Insert(node)
			wg.Done()
		}(i)
	}
	wg.Wait()
	id, err := checker.SaveChunk(ctx, "TestLoadChunk")
	c.Assert(err, IsNil)
	node, err := checker.LoadChunk("TestLoadChunk")
	c.Assert(err, IsNil)
	c.Assert(node.GetID(), Equals, id)
}
