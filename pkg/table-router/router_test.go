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

package router

import (
	"testing"

	. "github.com/pingcap/check"
	selector "github.com/pingcap/tidb-tools/pkg/table-rule-selector"
)

func TestClient(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testRouterSuite{})

type testRouterSuite struct{}

func (t *testRouterSuite) TestRoute(c *C) {
	rules := []*TableRule{
		{"Test_1_*", "abc*", "t1", "abc"},
		{"test_1_*", "test*", "t2", "test"},
		{"test_1_*", "", "test", ""},
		{"test_2_*", "abc*", "t1", "abc"},
		{"test_2_*", "test*", "t2", "test"},
	}

	cases := [][]string{
		{"test_1_a", "abc1", "t1", "abc"},
		{"test_2_a", "abc2", "t1", "abc"},
		{"test_1_a", "test1", "t2", "test"},
		{"test_2_a", "test2", "t2", "test"},
		{"test_1_a", "xyz", "test", "xyz"},
	}

	// initial table router
	router, err := NewTableRouter(false, rules)
	c.Assert(err, IsNil)

	// insert duplicate rules
	for _, rule := range rules {
		err = router.AddRule(rule)
		c.Assert(err, NotNil)
	}
	for _, cs := range cases {
		schema, table, err := router.Route(cs[0], cs[1])
		c.Assert(err, IsNil)
		c.Assert(schema, Equals, cs[2])
		c.Assert(table, Equals, cs[3])
	}

	// update rules
	rules[0].TargetTable = "xxx"
	cases[0][3] = "xxx"
	err = router.UpdateRule(rules[0])
	c.Assert(err, IsNil)
	for _, cs := range cases {
		schema, table, err := router.Route(cs[0], cs[1])
		c.Assert(err, IsNil)
		c.Assert(schema, Equals, cs[2])
		c.Assert(table, Equals, cs[3])
	}

	// remove rule
	err = router.RemoveRule(rules[0])
	c.Assert(err, IsNil)
	// remove not existing rule
	err = router.RemoveRule(rules[0])
	c.Assert(err, NotNil)
	schema, table, err := router.Route(cases[0][0], cases[0][1])
	c.Assert(err, IsNil)
	c.Assert(schema, Equals, "test")
	c.Assert(table, Equals, "abc1")
	// delete removed rule
	rules = rules[1:]
	cases = cases[1:]

	// mismatched
	schema, _, err = router.Route("test_3_a", "")
	c.Assert(err, IsNil)
	c.Assert(schema, Equals, "test_3_a")
	// test multiple schema level rules
	err = router.AddRule(&TableRule{"test_*", "", "error", ""})
	c.Assert(err, IsNil)
	_, _, err = router.Route("test_1_a", "")
	c.Assert(err, NotNil)
	// test multiple table level rules
	err = router.AddRule(&TableRule{"test_1_*", "tes*", "error", "error"})
	c.Assert(err, IsNil)
	_, _, err = router.Route("test_1_a", "test")
	c.Assert(err, NotNil)
	// invalid rule
	err = router.Selector.Insert("test_1_*", "abc*", "error", selector.Insert)
	c.Assert(err, IsNil)
	_, _, err = router.Route("test_1_a", "abc")
	c.Assert(err, NotNil)

	// Add/Update invalid table route rule
	inValidRule := &TableRule{
		SchemaPattern: "test*",
		TablePattern:  "abc*",
	}
	err = router.AddRule(inValidRule)
	c.Assert(err, NotNil)
	err = router.UpdateRule(inValidRule)
	c.Assert(err, NotNil)
}

func (t *testRouterSuite) TestCaseSensitive(c *C) {
	// we test case insensitive in TestRoute
	rules := []*TableRule{
		{"Test_1_*", "abc*", "t1", "abc"},
		{"test_1_*", "test*", "t2", "test"},
		{"test_1_*", "", "test", ""},
		{"test_2_*", "abc*", "t1", "abc"},
		{"test_2_*", "test*", "t2", "test"},
	}

	cases := [][]string{
		{"test_1_a", "abc1", "test", "abc1"},
		{"test_2_a", "abc2", "t1", "abc"},
		{"test_1_a", "test1", "t2", "test"},
		{"test_2_a", "test2", "t2", "test"},
		{"test_1_a", "xyz", "test", "xyz"},
	}

	// initial table router
	router, err := NewTableRouter(true, rules)
	c.Assert(err, IsNil)

	// insert duplicate rules
	for _, rule := range rules {
		err = router.AddRule(rule)
		c.Assert(err, NotNil)
	}
	for _, cs := range cases {
		schema, table, err := router.Route(cs[0], cs[1])
		c.Assert(err, IsNil)
		c.Assert(schema, Equals, cs[2])
		c.Assert(table, Equals, cs[3])
	}
}
