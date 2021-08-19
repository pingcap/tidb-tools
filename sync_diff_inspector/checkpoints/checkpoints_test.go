package checkpoints

import (
	"context"
	"fmt"
	"math/rand"
	"os"
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
	id, err := checker.SaveChunk(ctx, "TestSaveChunk")
	c.Assert(err, IsNil)
	c.Assert(id, Equals, 0)
	wg := &sync.WaitGroup{}
	rounds := 100
	for i := 1; i < rounds; i++ {
		wg.Add(1)
		go func(i_ int) {
			node := &Node{
				ChunkRange: &chunk.Range{
					ID: i_,
				},
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
	defer os.Remove("TestSaveChunk")
	id, err = checker.SaveChunk(ctx, "TestSaveChunk")
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
				},
			}
			checker.Insert(node)
			wg.Done()
		}(i)
	}
	wg.Wait()
	defer os.Remove("TestLoadChunk")
	id, err := checker.SaveChunk(ctx, "TestLoadChunk")
	c.Assert(err, IsNil)
	node, err := checker.LoadChunk("TestLoadChunk")
	c.Assert(err, IsNil)
	c.Assert(node.GetID(), Equals, id)
}
