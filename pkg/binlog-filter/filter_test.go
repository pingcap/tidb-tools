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

package filter

import (
	"testing"

	. "github.com/pingcap/check"
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
	for _, rule := range rules {
		err = filter.UpdateRule(rule)
		c.Assert(err, IsNil)
	}

	cases[0].action = Do      // delete
	cases[1].action = Do      // insert
	cases[3].action = Do      // create index
	cases[9].action = Do      // match all event and insert
	cases[10].action = Ignore // match none event and create index
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
	err = filter.Selector.Insert("test_1_*", "abc*", "error", false)
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
