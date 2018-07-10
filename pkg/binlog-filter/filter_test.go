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

package filter

import (
	"testing"

	. "github.com/pingcap/check"
)

func TestClient(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testRouterSuite{})

type testRouterSuite struct{}

func (t *testRouterSuite) TestRoute(c *C) {
	rules := []*BinlogEventRule{
		{"test_1_*", "abc*", []EventType{DeleteEvent, InsertEevent}, []EventType{CreateIndex, DropIndex}, []string{"^DROP\\s+PROCEDURE", "^CREATE\\s+PROCEDURE"}, nil, Skip},
	}

	cases := []struct {
		schema, table string
		dml, ddl      EventType
		sql           string
		action        ActionType
	}{
		{"test_1_a", "abc1", DeleteEvent, NullEvent, "", Skip},
		{"test_1_a", "abc1", InsertEevent, NullEvent, "", Skip},
		{"test_1_a", "abc1", UpdateEvent, NullEvent, "", Do},
		{"test_1_a", "abc1", NullEvent, CreateIndex, "", Skip},
		{"test_1_a", "abc1", NullEvent, RenameTable, "", Do},
		{"test_1_a", "abc1", NullEvent, NullEvent, "drop procedure abc", Skip},
		{"test_1_a", "abc1", NullEvent, NullEvent, "create procedure abc", Skip},
		{"test_1_a", "abc1", NullEvent, NullEvent, "create function abc", Do},
	}

	// initial binlog event filter
	filter, err := NewBinlogEvent(rules)
	c.Assert(err, IsNil)

	// insert duplicate rules
	for _, rule := range rules {
		err = filter.AddRule(rule)
		c.Assert(err, NotNil)
	}
	for _, cs := range cases {
		action, err := filter.Filter(cs.schema, cs.table, cs.dml, cs.ddl, cs.sql)
		c.Assert(err, IsNil)
		c.Assert(action, Equals, cs.action)
	}

	// test update rules
	rules[0].DMLEevent = []EventType{}
	cases[0].action = Do // delete
	cases[1].action = Do // insert
	err = filter.UpdateRule(rules[0])
	c.Assert(err, IsNil)
	for _, cs := range cases {
		action, err := filter.Filter(cs.schema, cs.table, cs.dml, cs.ddl, cs.sql)
		c.Assert(err, IsNil)
		c.Assert(action, Equals, cs.action)
	}

	// test multiple rules
	rule := &BinlogEventRule{"test_*", "ab*", []EventType{InsertEevent}, []EventType{CreateIndex, TruncateTable}, []string{"^DROP\\s+PROCEDURE"}, nil, Do}
	err = filter.AddRule(rule)
	c.Assert(err, IsNil)
	cases[0].action = Skip //delete
	cases[2].action = Skip // update
	cases[4].action = Skip // rename table
	cases[7].action = Skip // create function
	for _, cs := range cases {
		action, err := filter.Filter(cs.schema, cs.table, cs.dml, cs.ddl, cs.sql)
		c.Assert(err, IsNil)
		c.Assert(action, Equals, cs.action)
	}

	// test remove rule
	err = filter.RemoveRule(rules[0])
	c.Assert(err, IsNil)
	// test remove not existing rule
	err = filter.RemoveRule(rules[0])
	c.Assert(err, NotNil)
	cases[3].action = Do // create index
	cases[5].action = Do // drop procedure
	for _, cs := range cases {
		action, err := filter.Filter(cs.schema, cs.table, cs.dml, cs.ddl, cs.sql)
		c.Assert(err, IsNil)
		c.Assert(action, Equals, cs.action)
	}

	// test mismacthed
	action, err := filter.Filter("xxx_a", "", InsertEevent, NullEvent, "")
	c.Assert(action, Equals, Do)

	// invalid rule
	err = filter.Selector.Insert("test_1_*", "abc*", "error", false)
	c.Assert(err, IsNil)
	_, err = filter.Filter("test_1_a", "abc", InsertEevent, NullEvent, "")
	c.Assert(err, NotNil)
}
