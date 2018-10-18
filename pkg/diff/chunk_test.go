/*************************************************************************
 *
 * PingCAP CONFIDENTIAL
 * __________________
 *
 *  [2015] - [2018] PingCAP Incorporated
 *  All Rights Reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of PingCAP Incorporated and its suppliers,
 * if any.  The intellectual and technical concepts contained
 * herein are proprietary to PingCAP Incorporated
 * and its suppliers and may be covered by P.R.China and Foreign Patents,
 * patents in process, and are protected by trade secret or copyright law.
 * Dissemination of this information or reproduction of this material
 * is strictly forbidden unless prior written permission is obtained
 * from PingCAP Incorporated.
 */

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
