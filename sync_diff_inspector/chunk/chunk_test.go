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

package chunk

import (
	"testing"

	. "github.com/pingcap/check"
)

func TestClient(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testChunkSuite{})

type testChunkSuite struct{}

func (cp *testChunkSuite) TestChunkUpdate(c *C) {
	chunk := &Range{
		Bounds: []*Bound{
			{
				Column:   "a",
				Lower:    "1",
				Upper:    "2",
				HasLower: true,
				HasUpper: true,
			}, {
				Column:   "b",
				Lower:    "3",
				Upper:    "4",
				HasLower: true,
				HasUpper: true,
			},
		},
	}

	testCases := []struct {
		boundArgs  []string
		expectStr  string
		expectArgs []string
	}{
		{
			[]string{"a", "5", "6"},
			"((`a` > ?) OR (`a` = ? AND `b` > ?)) AND ((`a` < ?) OR (`a` = ? AND `b` <= ?))",
			[]string{"5", "5", "3", "6", "6", "4"},
		}, {
			[]string{"b", "5", "6"},
			"((`a` > ?) OR (`a` = ? AND `b` > ?)) AND ((`a` < ?) OR (`a` = ? AND `b` <= ?))",
			[]string{"1", "1", "5", "2", "2", "6"},
		}, {
			[]string{"c", "7", "8"},
			"((`a` > ?) OR (`a` = ? AND `b` > ?) OR (`a` = ? AND `b` = ? AND `c` > ?)) AND ((`a` < ?) OR (`a` = ? AND `b` < ?) OR (`a` = ? AND `b` = ? AND `c` <= ?))",
			[]string{"1", "1", "3", "1", "3", "7", "2", "2", "4", "2", "4", "8"},
		},
	}

	for _, cs := range testCases {
		newChunk := chunk.CopyAndUpdate(cs.boundArgs[0], cs.boundArgs[1], cs.boundArgs[2], true, true)
		conditions, args := newChunk.ToString("")
		c.Assert(conditions, Equals, cs.expectStr)
		c.Assert(args, DeepEquals, cs.expectArgs)
	}

	// the origin chunk is not changed
	conditions, args := chunk.ToString("")
	c.Assert(conditions, Equals, "((`a` > ?) OR (`a` = ? AND `b` > ?)) AND ((`a` < ?) OR (`a` = ? AND `b` <= ?))")
	expectArgs := []string{"1", "1", "3", "2", "2", "4"}
	c.Assert(args, DeepEquals, expectArgs)

	// test chunk update build by offset
	columnOffset := map[string]int{
		"a": 1,
		"b": 0,
	}
	chunkRange := NewChunkRangeOffset(columnOffset)
	chunkRange.Update("a", "1", "2", true, true)
	chunkRange.Update("b", "3", "4", true, true)
	c.Assert(chunkRange.ToMeta(), Equals, "range in sequence: (3,1) < (b,a) <= (4,2)")
}

func (cp *testChunkSuite) TestChunkToString(c *C) {
	// lower & upper
	chunk := &Range{
		Bounds: []*Bound{
			{
				Column:   "a",
				Lower:    "1",
				Upper:    "2",
				HasLower: true,
				HasUpper: true,
			}, {
				Column:   "b",
				Lower:    "3",
				Upper:    "4",
				HasLower: true,
				HasUpper: true,
			}, {
				Column:   "c",
				Lower:    "5",
				Upper:    "6",
				HasLower: true,
				HasUpper: true,
			},
		},
	}

	conditions, args := chunk.ToString("")
	c.Assert(conditions, Equals, "((`a` > ?) OR (`a` = ? AND `b` > ?) OR (`a` = ? AND `b` = ? AND `c` > ?)) AND ((`a` < ?) OR (`a` = ? AND `b` < ?) OR (`a` = ? AND `b` = ? AND `c` <= ?))")
	expectArgs := []string{"1", "1", "3", "1", "3", "5", "2", "2", "4", "2", "4", "6"}
	for i, arg := range args {
		c.Assert(arg, Equals, expectArgs[i])
	}

	conditions, args = chunk.ToString("latin1")
	c.Assert(conditions, Equals, "((`a` COLLATE 'latin1' > ?) OR (`a` = ? AND `b` COLLATE 'latin1' > ?) OR (`a` = ? AND `b` = ? AND `c` COLLATE 'latin1' > ?)) AND ((`a` COLLATE 'latin1' < ?) OR (`a` = ? AND `b` COLLATE 'latin1' < ?) OR (`a` = ? AND `b` = ? AND `c` COLLATE 'latin1' <= ?))")
	expectArgs = []string{"1", "1", "3", "1", "3", "5", "2", "2", "4", "2", "4", "6"}
	for i, arg := range args {
		c.Assert(arg, Equals, expectArgs[i])
	}

	c.Assert(chunk.String(), DeepEquals, "{\"id\":0,\"type\":0,\"bounds\":[{\"column\":\"a\",\"lower\":\"1\",\"upper\":\"2\",\"has-lower\":true,\"has-upper\":true},{\"column\":\"b\",\"lower\":\"3\",\"upper\":\"4\",\"has-lower\":true,\"has-upper\":true},{\"column\":\"c\",\"lower\":\"5\",\"upper\":\"6\",\"has-lower\":true,\"has-upper\":true}],\"where\":\"\",\"args\":null,\"bucket-id\":0}")
	c.Assert(chunk.ToMeta(), DeepEquals, "range in sequence: (1,3,5) < (a,b,c) <= (2,4,6)")

	// upper
	chunk = &Range{
		Bounds: []*Bound{
			{
				Column:   "a",
				Lower:    "1",
				Upper:    "2",
				HasLower: false,
				HasUpper: true,
			}, {
				Column:   "b",
				Lower:    "3",
				Upper:    "4",
				HasLower: false,
				HasUpper: true,
			}, {
				Column:   "c",
				Lower:    "5",
				Upper:    "6",
				HasLower: false,
				HasUpper: true,
			},
		},
	}

	conditions, args = chunk.ToString("latin1")
	c.Assert(conditions, Equals, "(`a` COLLATE 'latin1' < ?) OR (`a` = ? AND `b` COLLATE 'latin1' < ?) OR (`a` = ? AND `b` = ? AND `c` COLLATE 'latin1' <= ?)")
	expectArgs = []string{"2", "2", "4", "2", "4", "6"}
	for i, arg := range args {
		c.Assert(arg, Equals, expectArgs[i])
	}

	c.Assert(chunk.String(), DeepEquals, "{\"id\":0,\"type\":0,\"bounds\":[{\"column\":\"a\",\"lower\":\"1\",\"upper\":\"2\",\"has-lower\":false,\"has-upper\":true},{\"column\":\"b\",\"lower\":\"3\",\"upper\":\"4\",\"has-lower\":false,\"has-upper\":true},{\"column\":\"c\",\"lower\":\"5\",\"upper\":\"6\",\"has-lower\":false,\"has-upper\":true}],\"where\":\"\",\"args\":null,\"bucket-id\":0}")
	c.Assert(chunk.ToMeta(), DeepEquals, "range in sequence: (a,b,c) <= (2,4,6)")

	// lower
	chunk = &Range{
		Bounds: []*Bound{
			{
				Column:   "a",
				Lower:    "1",
				Upper:    "2",
				HasLower: true,
				HasUpper: false,
			}, {
				Column:   "b",
				Lower:    "3",
				Upper:    "4",
				HasLower: true,
				HasUpper: false,
			}, {
				Column:   "c",
				Lower:    "5",
				Upper:    "6",
				HasLower: true,
				HasUpper: false,
			},
		},
	}

	conditions, args = chunk.ToString("")
	c.Assert(conditions, Equals, "(`a` > ?) OR (`a` = ? AND `b` > ?) OR (`a` = ? AND `b` = ? AND `c` > ?)")
	expectArgs = []string{"1", "1", "3", "1", "3", "5"}
	for i, arg := range args {
		c.Assert(arg, Equals, expectArgs[i])
	}

	conditions, args = chunk.ToString("latin1")
	c.Assert(conditions, Equals, "(`a` COLLATE 'latin1' > ?) OR (`a` = ? AND `b` COLLATE 'latin1' > ?) OR (`a` = ? AND `b` = ? AND `c` COLLATE 'latin1' > ?)")
	expectArgs = []string{"1", "1", "3", "1", "3", "5"}
	for i, arg := range args {
		c.Assert(arg, Equals, expectArgs[i])
	}

	c.Assert(chunk.String(), DeepEquals, "{\"id\":0,\"type\":0,\"bounds\":[{\"column\":\"a\",\"lower\":\"1\",\"upper\":\"2\",\"has-lower\":true,\"has-upper\":false},{\"column\":\"b\",\"lower\":\"3\",\"upper\":\"4\",\"has-lower\":true,\"has-upper\":false},{\"column\":\"c\",\"lower\":\"5\",\"upper\":\"6\",\"has-lower\":true,\"has-upper\":false}],\"where\":\"\",\"args\":null,\"bucket-id\":0}")
	c.Assert(chunk.ToMeta(), DeepEquals, "range in sequence: (1,3,5) < (a,b,c)")

	// none
	chunk = &Range{
		Bounds: []*Bound{
			{
				Column:   "a",
				Lower:    "1",
				Upper:    "2",
				HasLower: false,
				HasUpper: false,
			}, {
				Column:   "b",
				Lower:    "3",
				Upper:    "4",
				HasLower: false,
				HasUpper: false,
			}, {
				Column:   "c",
				Lower:    "5",
				Upper:    "6",
				HasLower: false,
				HasUpper: false,
			},
		},
	}
	conditions, args = chunk.ToString("")
	c.Assert(conditions, Equals, "TRUE")
	expectArgs = []string{}
	for i, arg := range args {
		c.Assert(arg, Equals, expectArgs[i])
	}

	c.Assert(chunk.String(), DeepEquals, "{\"id\":0,\"type\":0,\"bounds\":[{\"column\":\"a\",\"lower\":\"1\",\"upper\":\"2\",\"has-lower\":false,\"has-upper\":false},{\"column\":\"b\",\"lower\":\"3\",\"upper\":\"4\",\"has-lower\":false,\"has-upper\":false},{\"column\":\"c\",\"lower\":\"5\",\"upper\":\"6\",\"has-lower\":false,\"has-upper\":false}],\"where\":\"\",\"args\":null,\"bucket-id\":0}")
	c.Assert(chunk.ToMeta(), DeepEquals, "range in sequence: Full")
}

func (*testChunkSuite) TestChunkInit(c *C) {
	chunks := []*Range{
		{
			Bounds: []*Bound{
				{
					Column:   "a",
					Lower:    "1",
					Upper:    "2",
					HasLower: true,
					HasUpper: true,
				}, {
					Column:   "b",
					Lower:    "3",
					Upper:    "4",
					HasLower: true,
					HasUpper: true,
				}, {
					Column:   "c",
					Lower:    "5",
					Upper:    "6",
					HasLower: true,
					HasUpper: true,
				},
			},
		}, {
			Bounds: []*Bound{
				{
					Column:   "a",
					Lower:    "2",
					Upper:    "3",
					HasLower: true,
					HasUpper: true,
				}, {
					Column:   "b",
					Lower:    "4",
					Upper:    "5",
					HasLower: true,
					HasUpper: true,
				}, {
					Column:   "c",
					Lower:    "6",
					Upper:    "7",
					HasLower: true,
					HasUpper: true,
				},
			},
		},
	}

	InitChunks(chunks, Others, 1, "[123]", "[sdfds fsd fd gd]")
	c.Assert(chunks[0].Where, Equals, "((((`a` COLLATE '[123]' > ?) OR (`a` = ? AND `b` COLLATE '[123]' > ?) OR (`a` = ? AND `b` = ? AND `c` COLLATE '[123]' > ?)) AND ((`a` COLLATE '[123]' < ?) OR (`a` = ? AND `b` COLLATE '[123]' < ?) OR (`a` = ? AND `b` = ? AND `c` COLLATE '[123]' <= ?))) AND [sdfds fsd fd gd])")
	c.Assert(chunks[0].Args, DeepEquals, []string{"1", "1", "3", "1", "3", "5", "2", "2", "4", "2", "4", "6"})
	c.Assert(chunks[0].BucketID, Equals, 1)
	c.Assert(chunks[0].Type, Equals, Others)
	InitChunk(chunks[1], Others, 2, "[456]", "[dsfsdf]")
	c.Assert(chunks[1].Where, Equals, "((((`a` COLLATE '[456]' > ?) OR (`a` = ? AND `b` COLLATE '[456]' > ?) OR (`a` = ? AND `b` = ? AND `c` COLLATE '[456]' > ?)) AND ((`a` COLLATE '[456]' < ?) OR (`a` = ? AND `b` COLLATE '[456]' < ?) OR (`a` = ? AND `b` = ? AND `c` COLLATE '[456]' <= ?))) AND [dsfsdf])")
	c.Assert(chunks[1].Args, DeepEquals, []string{"2", "2", "4", "2", "4", "6", "3", "3", "5", "3", "5", "7"})
	c.Assert(chunks[1].BucketID, Equals, 2)
	c.Assert(chunks[1].Type, Equals, Others)
}

func (*testChunkSuite) TestChunkCopyAndUpdate(c *C) {
	chunk := NewChunkRange()
	chunk.Update("a", "1", "2", true, true)
	chunk.Update("a", "2", "3", true, true)
	chunk.Update("a", "324", "5435", false, false)
	chunk.Update("b", "4", "5", true, false)
	chunk.Update("b", "8", "9", false, true)
	chunk.Update("c", "6", "7", false, true)
	chunk.Update("c", "10", "11", true, false)

	conditions, args := chunk.ToString("")
	c.Assert(conditions, Equals, "((`a` > ?) OR (`a` = ? AND `b` > ?) OR (`a` = ? AND `b` = ? AND `c` > ?)) AND ((`a` < ?) OR (`a` = ? AND `b` < ?) OR (`a` = ? AND `b` = ? AND `c` <= ?))")
	c.Assert(args, DeepEquals, []string{"2", "2", "4", "2", "4", "10", "3", "3", "9", "3", "9", "7"})

	chunk2 := chunk.CopyAndUpdate("a", "4", "6", true, true)
	conditions, args = chunk2.ToString("")
	c.Assert(conditions, Equals, "((`a` > ?) OR (`a` = ? AND `b` > ?) OR (`a` = ? AND `b` = ? AND `c` > ?)) AND ((`a` < ?) OR (`a` = ? AND `b` < ?) OR (`a` = ? AND `b` = ? AND `c` <= ?))")
	c.Assert(args, DeepEquals, []string{"4", "4", "4", "4", "4", "10", "6", "6", "9", "6", "9", "7"})
	_, args = chunk.ToString("")
	// `Copy` use the same []string
	c.Assert(args, DeepEquals, []string{"2", "2", "4", "2", "4", "10", "3", "3", "9", "3", "9", "7"})

	InitChunk(chunk, Others, 2, "[324]", "[543]")
	chunk3 := chunk.Clone()
	chunk3.Update("a", "2", "3", true, true)
	c.Assert(chunk3.Where, Equals, "((((`a` COLLATE '[324]' > ?) OR (`a` = ? AND `b` COLLATE '[324]' > ?) OR (`a` = ? AND `b` = ? AND `c` COLLATE '[324]' > ?)) AND ((`a` COLLATE '[324]' < ?) OR (`a` = ? AND `b` COLLATE '[324]' < ?) OR (`a` = ? AND `b` = ? AND `c` COLLATE '[324]' <= ?))) AND [543])")
	c.Log(chunk3.Args)
	c.Assert(chunk3.Args, DeepEquals, []string{"2", "2", "4", "2", "4", "10", "3", "3", "9", "3", "9", "7"})
	c.Assert(chunk3.BucketID, Equals, 2)
	c.Assert(chunk3.Type, Equals, Others)
}
