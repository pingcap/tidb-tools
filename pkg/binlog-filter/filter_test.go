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
	selector "github.com/pingcap/tidb-tools/pkg/table-rule-selector"
)

func TestClient(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testFilterSuite{})

type testFilterSuite struct{}

func (t *testFilterSuite) TestFilter(c *C) {
	rules := []*BinlogEventRule{
		{"Test_1_*", "abc*", []EventType{DeleteEvent, InsertEvent, CreateIndex, DropIndex}, []string{"^DROP\\s+PROCEDURE", "^CREATE\\s+PROCEDURE"}, nil, Ignore},
		{"xxx_*", "abc_*", []EventType{AllDML, NoneDDL}, nil, nil, Ignore},
		{"yyy_*", "abc_*", []EventType{EventType("ALL DML")}, nil, nil, Do},
	}

	cases := []struct {
		schema, table string
		event         EventType
		sql           string
		action        ActionType
	}{
		{"test_1_a", "abc1", DeleteEvent, "", Ignore},
		{"test_1_a", "abc1", InsertEvent, "", Ignore},
		{"test_1_a", "abc1", UpdateEvent, "", Do},
		{"test_1_a", "abc1", CreateIndex, "", Ignore},
		{"test_1_a", "abc1", RenameTable, "", Do},
		{"test_1_a", "abc1", NullEvent, "drop procedure abc", Ignore},
		{"test_1_a", "abc1", NullEvent, "create procedure abc", Ignore},
		{"test_1_a", "abc1", NullEvent, "create function abc", Do},
		{"xxx_1", "abc_1", NullEvent, "create function abc", Do},
		{"xxx_1", "abc_1", InsertEvent, "", Ignore},
		{"xxx_1", "abc_1", CreateIndex, "", Do},
		{"yyy_1", "abc_1", InsertEvent, "", Do},
		{"yyy_1", "abc_1", CreateIndex, "", Ignore},
	}

	// initial binlog event filter
	filter, err := NewBinlogEvent(false, rules)
	c.Assert(err, IsNil)

	// insert duplicate rules
	for _, rule := range rules {
		err = filter.AddRule(rule)
		c.Assert(err, NotNil)
	}
	for _, cs := range cases {
		action, err := filter.Filter(cs.schema, cs.table, cs.event, cs.sql)
		c.Assert(err, IsNil)
		c.Assert(action, Equals, cs.action)
	}

	// update rules
	rules[0].Events = []EventType{}
	rules[1].Action = Do
	rules[2].Events = []EventType{"ALL DDL"}
	for _, rule := range rules {
		err = filter.UpdateRule(rule)
		c.Assert(err, IsNil)
	}

	cases[0].action = Do      // delete
	cases[1].action = Do      // insert
	cases[3].action = Do      // create index
	cases[9].action = Do      // match all event and insert
	cases[10].action = Ignore // match none event and create index
	cases[11].action = Ignore // no match
	cases[12].action = Do     // match all ddl
	for _, cs := range cases {
		action, err := filter.Filter(cs.schema, cs.table, cs.event, cs.sql)
		c.Assert(err, IsNil)
		c.Assert(action, Equals, cs.action)
	}

	// test multiple rules
	rule := &BinlogEventRule{"test_*", "ab*", []EventType{InsertEvent, AllDDL}, []string{"^DROP\\s+PROCEDURE"}, nil, Do}
	err = filter.AddRule(rule)
	c.Assert(err, IsNil)
	cases[0].action = Ignore //delete
	cases[2].action = Ignore // update
	cases[4].action = Do     // rename table
	cases[7].action = Ignore // create function
	for _, cs := range cases {
		action, err := filter.Filter(cs.schema, cs.table, cs.event, cs.sql)
		c.Assert(err, IsNil)
		c.Assert(action, Equals, cs.action)
	}

	// remove rule
	err = filter.RemoveRule(rules[0])
	c.Assert(err, IsNil)
	// remove not existing rule
	err = filter.RemoveRule(rules[0])
	c.Assert(err, NotNil)
	cases[3].action = Do // create index
	cases[5].action = Do // drop procedure
	for _, cs := range cases {
		action, err := filter.Filter(cs.schema, cs.table, cs.event, cs.sql)
		c.Assert(err, IsNil)
		c.Assert(action, Equals, cs.action)
	}

	// mismatched
	action, err := filter.Filter("xxx_a", "", InsertEvent, "")
	c.Assert(action, Equals, Do)

	// invalid rule
	err = filter.Selector.Insert("test_1_*", "abc*", "error", selector.Insert)
	c.Assert(err, IsNil)
	_, err = filter.Filter("test_1_a", "abc", InsertEvent, "")
	c.Assert(err, NotNil)
}

func (t *testFilterSuite) TestCaseSensitive(c *C) {
	// we test case insensitive in TestFilter
	rules := []*BinlogEventRule{
		{"Test_1_*", "abc*", []EventType{DeleteEvent, InsertEvent, CreateIndex, DropIndex}, []string{"^DROP\\s+PROCEDURE", "^CREATE\\s+PROCEDURE"}, nil, Ignore},
		{"xxx_*", "abc_*", []EventType{AllDML, NoneDDL}, nil, nil, Ignore},
	}

	cases := []struct {
		schema, table string
		event         EventType
		sql           string
		action        ActionType
	}{
		{"test_1_a", "abc1", DeleteEvent, "", Do},
		{"test_1_a", "abc1", InsertEvent, "", Do},
		{"test_1_a", "abc1", UpdateEvent, "", Do},
		{"test_1_a", "abc1", CreateIndex, "", Do},
		{"test_1_a", "abc1", RenameTable, "", Do},
		{"test_1_a", "abc1", NullEvent, "drop procedure abc", Do},
		{"test_1_a", "abc1", NullEvent, "create procedure abc", Do},
		{"test_1_a", "abc1", NullEvent, "create function abc", Do},
		{"xxx_1", "abc_1", NullEvent, "create function abc", Do},
		{"xxx_1", "abc_1", InsertEvent, "", Ignore},
		{"xxx_1", "abc_1", CreateIndex, "", Do},
	}

	// initial binlog event filter
	filter, err := NewBinlogEvent(true, rules)
	c.Assert(err, IsNil)

	// insert duplicate rules
	for _, rule := range rules {
		err = filter.AddRule(rule)
		c.Assert(err, NotNil)
	}
	for _, cs := range cases {
		action, err := filter.Filter(cs.schema, cs.table, cs.event, cs.sql)
		c.Assert(err, IsNil)
		c.Assert(action, Equals, cs.action)
	}
}

func (t *testFilterSuite) TestGlobalFilter(c *C) {
	schemaRule := &BinlogEventRule{
		SchemaPattern: "*",
		SQLPattern:    []string{"^FLUSH"},
		Action:        Ignore,
	}
	tableRule := &BinlogEventRule{
		SchemaPattern: "*",
		TablePattern:  "*",
		SQLPattern:    []string{"^FLUSH"},
		Action:        Ignore,
	}

	cases := []struct {
		schema string
		table  string
		sql    string
		action ActionType
	}{
		{
			schema: "db",
			table:  "tbl",
			sql:    "FLUSH ENGINE LOGS",
			action: Ignore,
		},
		{
			schema: "db",
			table:  "",
			sql:    "FLUSH ENGINE LOGS",
			action: Ignore,
		},
		{
			schema: "",
			table:  "tbl",
			sql:    "FLUSH ENGINE LOGS",
			action: Ignore,
		},
		{
			schema: "",
			table:  "",
			sql:    "FLUSH ENGINE LOGS",
			action: Ignore,
		},
	}

	// initial binlog event filter with schema rule
	filter, err := NewBinlogEvent(false, []*BinlogEventRule{schemaRule})
	c.Assert(err, IsNil)

	for _, cs := range cases {
		action, err := filter.Filter(cs.schema, cs.table, NullEvent, cs.sql)
		c.Assert(err, IsNil)
		c.Assert(action, Equals, cs.action)
	}

	// remove schema rule
	err = filter.RemoveRule(schemaRule)
	c.Assert(err, IsNil)

	// add table rule
	err = filter.AddRule(tableRule)
	c.Assert(err, IsNil)

	for _, cs := range cases {
		action, err := filter.Filter(cs.schema, cs.table, NullEvent, cs.sql)
		c.Assert(err, IsNil)
		c.Assert(action, Equals, cs.action)
	}
}
