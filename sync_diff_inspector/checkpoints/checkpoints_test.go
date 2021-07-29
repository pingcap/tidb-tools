package checkpoints

import (
	"context"
	"fmt"
	"github.com/pingcap/tidb-tools/sync_diff_inspector/chunk"
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
	id, _ := checker.SaveChunk(ctx)
	c.Assert(id, Equals, 0)
	wg := &sync.WaitGroup{}
	for i := 1; i < 10000; i++ {
		wg.Add(1)
		go func(i_ int) {
			node := &Node{
				ID:     i_,
				Schema: "test",
				Table:  "test",
				Chunk: &chunk.Range{
					BucketID: int64(i_),
				},
				ChunkState: "success",
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

func (cp *testCheckpointSuit) TestLoadChunk(c *C) {
	checker := new(Checkpoint)
	checker.Init()
	node, _ := checker.LoadChunk()
	c.Assert(node.GetChunk().BucketID, Equals, 9999)
}
