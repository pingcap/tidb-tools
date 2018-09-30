// Copyright 2018 PingCAP, Inc.
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

package diff

import (
	. "github.com/pingcap/check"
)

var _ = Suite(&testChunkSuite{})

type testChunkSuite struct{}

type chunkTestCase struct {
	chunk        *chunkRange
	chunkCnt     int64
	expectChunks []*chunkRange
}

func (*testChunkSuite) TestSplitRange(c *C) {
	testCases := []*chunkTestCase{
		{
			&chunkRange{
				begin:        int64(1),
				end:          int64(1000),
				containBegin: true,
				containEnd:   true,
			},
			1,
			[]*chunkRange{
				{
					begin:        int64(1),
					end:          int64(1000),
					containBegin: true,
					containEnd:   true,
				},
				{
					begin:        struct{}{},
					end:          int64(1),
					containBegin: false,
					containEnd:   false,
					noBegin:      true,
				},
				{
					begin:        int64(1000),
					end:          struct{}{},
					containBegin: false,
					containEnd:   false,
					noEnd:        true,
				},
			},
		}, {
			&chunkRange{
				begin:        int64(1),
				end:          int64(1000),
				containBegin: true,
				containEnd:   false,
			},
			2,
			[]*chunkRange{
				{
					begin:        int64(1),
					end:          int64(501),
					containBegin: true,
					containEnd:   false,
				},
				{
					begin:        int64(501),
					end:          int64(1000),
					containBegin: true,
					containEnd:   false,
				},
				{
					begin:        struct{}{},
					end:          int64(1),
					containBegin: false,
					containEnd:   false,
					noBegin:      true,
				},
				{
					begin:        int64(1000),
					end:          struct{}{},
					containBegin: false,
					containEnd:   false,
					noEnd:        true,
				},
			},
		}, {
			&chunkRange{
				begin:        float64(1.1),
				end:          float64(1000.1),
				containBegin: false,
				containEnd:   false,
			},
			2,
			[]*chunkRange{
				{
					begin:        float64(1.1),
					end:          float64(501.1),
					containBegin: false,
					containEnd:   false,
				},
				{
					begin:        float64(501.1),
					end:          float64(1000.1),
					containBegin: true,
					containEnd:   false,
				},
				{
					begin:        struct{}{},
					end:          float64(1.1),
					containBegin: false,
					containEnd:   false,
					noBegin:      true,
				},
				{
					begin:        float64(1000.1),
					end:          struct{}{},
					containBegin: false,
					containEnd:   false,
					noEnd:        true,
				},
			},
		},
	}

	for _, testCase := range testCases {
		chunks, err := splitRange(nil, testCase.chunk, testCase.chunkCnt, "", "", nil, "", "")
		c.Assert(err, IsNil)

		for i, chunk := range chunks {
			c.Assert(chunk.begin, Equals, testCase.expectChunks[i].begin)
			c.Assert(chunk.end, Equals, testCase.expectChunks[i].end)
			c.Assert(chunk.containBegin, Equals, testCase.expectChunks[i].containBegin)
			c.Assert(chunk.containEnd, Equals, testCase.expectChunks[i].containEnd)
		}
	}
}
