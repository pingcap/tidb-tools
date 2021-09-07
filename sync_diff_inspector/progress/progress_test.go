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
	"errors"
	"testing"
	"time"

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
	p.StartTable("2", 2, true)
	p.Inc("2")
	p.RegisterTable("3", false, false)
	p.StartTable("3", 1, false)
	p.Inc("2")
	p.Inc("3")
	p.UpdateTotal("3", 1, true)
	p.Inc("3")
	p.StartTable("4", 1, true)
	p.FailTable("4")
	p.Inc("3")
	p.Inc("4")
	time.Sleep(500 * time.Millisecond)
	p.Close()
	buffer := new(bytes.Buffer)
	p.SetOutput(buffer)
	p.PrintSummary()
	c.Assert(
		buffer.String(),
		DeepEquals,
		"\x1b[1A\x1b[J\nSummary:\n\nThe structure of `1` is not equal.\nThe structure of `2` is not equal.\nThe data of `4` is not equal.\n"+
			"\nThe rest of the tables are all equal.\nThe patch file has been generated to './output_dir/patch.sql'\n"+
			"You can view the comparison details through './output_dir/sync_diff_inspector.log'\n\n",
	)
}

func (s *testProgressSuite) TestTableError(c *C) {
	p := NewTableProgressPrinter(4)
	p.RegisterTable("1", true, true)
	p.StartTable("1", 50, true)
	p.RegisterTable("2", true, true)
	p.StartTable("2", 1, true)
	p.Inc("2")
	buffer := new(bytes.Buffer)
	p.SetOutput(buffer)
	p.Error(errors.New("[aaa]"))
	time.Sleep(500 * time.Millisecond)
	c.Assert(
		buffer.String(),
		DeepEquals,
		"\x1b[0A\x1b[JComparing the table structure of `1` ... failure\n"+
			"_____________________________________________________________________________\n"+
			"Progress [===============>---------------------------------------------] 25% 0/0\n"+
			"\x1b[2A\x1b[JComparing the table structure of `2` ... failure\n"+
			"_____________________________________________________________________________\n"+
			"Progress [==============================>------------------------------] 50% 0/0\n"+
			"\x1b[1A\x1b[J\nError in comparison process:\n[aaa]\n\n"+
			"You can view the comparison details through './output_dir/sync_diff_inspector.log'\n",
	)
}

func (s *testProgressSuite) TestAllSuccess(c *C) {
	Init(2)
	RegisterTable("1", false, false)
	StartTable("1", 1, true)
	RegisterTable("2", false, false)
	StartTable("2", 1, true)
	Inc("1")
	Inc("2")
	Close()
	buf := new(bytes.Buffer)
	SetOutput(buf)
	PrintSummary()
	c.Assert(buf.String(), Equals, "\x1b[1A\x1b[J\nSummary:\n\n"+
		"A total of 2 tables have been compared and all are equal.\n"+
		"You can view the comparison details through './output_dir/sync_diff_inspector.log'\n\n",
	)
}
