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

package selector

import (
	"testing"

	. "github.com/pingcap/check"
)

func TestClient(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testSelectorSuite{})

type testSelectorSuite struct{}

func (t *testSelectorSuite) TestRoute(c *C) {
	s := NewTrieSelector()
	t.testInsert(c, s)
	t.testMatch(c, s)
}

type dummyRule struct {
	description string
}

func (t *testSelectorSuite) testInsert(c *C, s Selector) {
	err := s.InsertSchema("schema*", &dummyRule{"schema"})
	c.Assert(err, IsNil)
	err = s.InsertSchema("schema*", &dummyRule{"schema"})
	c.Assert(err, NotNil)
	err = s.InsertSchema("sche*a", nil)
	c.Assert(err, NotNil)

	tableRules := map[string]map[string]interface{}{
		"?bc":     {"abc": &dummyRule{"nobody"}, "ab?": &dummyRule{"selected"}},
		"a?c":     {"abc": &dummyRule{"nobody"}, "ab*": &dummyRule{"selected"}},
		"ab?":     {"ab?": &dummyRule{"nobody"}, "ab*": &dummyRule{"selected"}},
		"ab*":     {"abc": &dummyRule{"selected"}, "abc*": &dummyRule{"nobody"}},
		"abc":     {"abc": &dummyRule{"selected"}, "abc?": &dummyRule{"nobody"}},
		"schema*": {"test*": &dummyRule{"test"}, "abc*": &dummyRule{"abc"}, "xyz": &dummyRule{"xyz"}},
		"t*":      {"test*": &dummyRule{"test"}},
	}

	for schema, tables := range tableRules {
		for table, rule := range tables {
			err = s.InsertTable(schema, table, rule)
			c.Assert(err, IsNil)
		}
	}

	err = s.InsertTable("ab**", "", &dummyRule{"test"})
	c.Assert(err, NotNil)

	schemas, tables := s.AllRules()
	c.Assert(schemas, HasLen, 1)
	c.Assert(tables, DeepEquals, tableRules)
}

func (t *testSelectorSuite) testMatch(c *C, s Selector) {
	cases := [][]string{
		// test one level
		{"dbc", "abc", "selected"},
		{"adc", "abc", "selected"},
		{"abd", "abc", "selected"},
		{"abc", "abd", "selected"},
		// test only schema rule
		{"schema1", "xxx", "schema"},
		{"schema1", "", "schema"},
		// test table rule
		{"schema1", "test1", "test"},
		{"t1", "test1", "test"},
		{"schema1", "abc1", "abc"},
	}
	cache := make(map[string]interface{})
	for _, tc := range cases {
		rule := s.Match(tc[0], tc[1])
		c.Assert(rule, NotNil)

		dr, ok := rule.(*dummyRule)
		c.Assert(ok, IsTrue)

		c.Assert(dr.description, Equals, tc[2])
		cache[quoateSchemaTable(tc[0], tc[1])] = rule
	}

	// test cache
	trie, ok := s.(*trieSelector)
	c.Assert(ok, IsTrue)
	c.Assert(trie.cache, DeepEquals, cache)

	// test not mathced
	rule := s.Match("t1", "")
	c.Assert(rule, IsNil)
	cache[quoateSchemaTable("t1", "")] = rule

	rule = s.Match("t1", "abc")
	c.Assert(rule, IsNil)
	cache[quoateSchemaTable("t1", "abc")] = rule

	rule = s.Match("xxx", "abc")
	c.Assert(rule, IsNil)
	cache[quoateSchemaTable("xxx", "abc")] = rule
	c.Assert(trie.cache, DeepEquals, cache)
}
