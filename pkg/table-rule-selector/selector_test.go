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

var _ = Suite(&testSelectorSuite{
	schemas: []string{"schema*"},
	tables: map[string][]string{
		"t*":      {"test*"},
		"schema*": {"test*", "abc*", "xyz"},
		"?bc":     {"t1_abc", "t1_ab?", "abc*"},
		"a?c":     {"t2_abc", "t2_ab*", "a?b"},
		"ab?":     {"t3_ab?", "t3_ab*", "ab?"},
		"ab*":     {"t4_abc", "t4_abc*", "ab*"},
		"abc":     {"abc"},
	},
	matchCase: []struct {
		schame, table string
		matchedNum    int
		matchedRules  []string //schema, table, schema, table...
	}{
		// test one level
		{"dbc", "t1_abc", 2, []string{"?bc", "t1_ab?", "?bc", "t1_abc"}},
		{"adc", "t2_abc", 2, []string{"a?c", "t2_ab*", "a?c", "t2_abc"}},
		{"abd", "t3_abc", 2, []string{"ab?", "t3_ab*", "ab?", "t3_ab?"}},
		{"abc", "t4_abc", 2, []string{"ab*", "t4_abc", "ab*", "t4_abc*"}},
		{"abc", "abc", 4, []string{"?bc", "abc*", "ab*", "ab*", "ab?", "ab?", "abc", "abc"}},
		// test only schema rule
		{"schema1", "xxx", 1, []string{"schema*", ""}},
		{"schema1", "", 1, []string{"schema*", ""}},
		// test table rule
		{"schema1", "test1", 2, []string{"schema*", "", "schema*", "test*"}},
		{"t1", "test1", 1, []string{"t*", "test*"}},
		{"schema1", "abc1", 2, []string{"schema*", "", "schema*", "abc*"}},
	},
})

type testSelectorSuite struct {
	tables  map[string][]string
	schemas []string

	matchCase []struct {
		schame, table string
		matchedNum    int
		matchedRules  []string //schema, table, schema, table...
	}
}

func (t *testSelectorSuite) TestRoute(c *C) {
	s := NewTrieSelector()
	t.testInsert(c, s)
	t.testMatch(c, s)
}

type dummyRule struct {
	description string
}

func (t *testSelectorSuite) testInsert(c *C, s Selector) {
	var err error
	schemaRules, tableRules := t.testGenerateExpectedRules()

	for schema, rule := range schemaRules {
		err = s.InsertSchema(schema, rule)
		c.Assert(err, IsNil)
		err = s.InsertSchema(schema, rule)
		c.Assert(err, NotNil)
	}

	for schema, tables := range tableRules {
		for table, rule := range tables {
			err = s.InsertTable(schema, table, rule)
			c.Assert(err, IsNil)
			err = s.InsertTable(schema, table, rule)
			c.Assert(err, NotNil)
		}
	}

	// insert wrong pattern
	err = s.InsertSchema("sche*a", nil)
	c.Assert(err, NotNil)
	err = s.InsertTable("schema*", "", &dummyRule{"error"})
	c.Assert(err, NotNil)
	err = s.InsertTable("ab**", "", &dummyRule{"error"})
	c.Assert(err, NotNil)
	err = s.InsertTable("abcd", "ab**", &dummyRule{"error"})
	c.Assert(err, NotNil)

	schemas, tables := s.AllRules()
	c.Assert(schemas, DeepEquals, schemaRules)
	c.Assert(tables, DeepEquals, tableRules)
}

func (t *testSelectorSuite) testMatch(c *C, s Selector) {
	cache := make(map[string]RuleSet)
	for _, mc := range t.matchCase {
		rules := s.Match(mc.schame, mc.table)
		expectedRules := make(RuleSet, 0, mc.matchedNum)
		for i := 0; i < mc.matchedNum; i++ {
			rule := &dummyRule{quoateSchemaTable(mc.matchedRules[2*i], mc.matchedRules[2*i+1])}
			expectedRules = append(expectedRules, rule)
		}

		c.Assert(rules, DeepEquals, expectedRules)
		cache[quoateSchemaTable(mc.schame, mc.table)] = expectedRules
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

func (t *testSelectorSuite) testGenerateExpectedRules() (map[string]interface{}, map[string]map[string]interface{}) {
	schemaRules := make(map[string]interface{})
	for _, schema := range t.schemas {
		schemaRules[schema] = &dummyRule{quoateSchemaTable(schema, "")}
	}

	tableRules := make(map[string]map[string]interface{})
	for schema, tables := range t.tables {
		_, ok := tableRules[schema]
		if !ok {
			tableRules[schema] = make(map[string]interface{})
		}
		for _, table := range tables {
			tableRules[schema][table] = &dummyRule{quoateSchemaTable(schema, table)}
		}
	}

	return schemaRules, tableRules
}
