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

package progress

import (
	"bytes"
	"testing"

	. "github.com/pingcap/check"
)

func TestClient(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testProgressSuite{})

type testProgressSuite struct{}

func (s *testProgressSuite) TestProgress(c *C) {
	p := NewTableProgressPrinter(4)
	p.RegisterTable("1", true, true)
	p.StartTable("1", 50, true)
	p.RegisterTable("2", true, false)
	p.StartTable("2", 1, true)
	p.Inc("2")
	p.RegisterTable("3", false, false)
	p.StartTable("3", 1, false)
	p.Inc("3")
	p.UpdateTotal("3", 1, true)
	p.Inc("3")
	p.RegisterTable("4", false, false)
	p.StartTable("4", 1, true)
	p.FailTable("4")
	p.Inc("3")
	p.Close()
	buffer := new(bytes.Buffer)
	p.SetOutput(buffer)
	p.PrintSummary()
	c.Assert(
		buffer.String(),
		DeepEquals,
		"\x1b[1A\x1b[J\nSummary:\n\nThe structure of `1` is not equal.\nThe structure of `2` is not equal.\n"+
			"\nThe rest of the tables are all equal.\nThe patch file has been generated to './output_dir/patch.sql'\n"+
			"You can view the comparison details through './output_dir/sync_diff_inspector.log'\n\n",
	)
}
