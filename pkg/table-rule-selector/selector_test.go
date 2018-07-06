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
	tables: map[string][]string{
		"t*":      {"test*"},
		"schema*": {"", "test*", "abc*", "xyz"},
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
	// for generate rules,
	// we use dummy rule which contains schema and table pattern information to simplify test
	tables map[string][]string

	matchCase []struct {
		schame, table string
		matchedNum    int
		// // for generate rules,
		// we use dummy rule which contains schema(matchedRules[2*i]) and table(matchedRules[2*i+1]) pattern information to simplify test
		matchedRules []string //schema, table, schema, table...
	}
}

func (t *testSelectorSuite) TestRoute(c *C) {
	s := NewTrieSelector()
	t.testInsert(c, s)
	t.testMatch(c, s)
	t.testReplace(c, s)
}

type dummyRule struct {
	description string
}

func (t *testSelectorSuite) testInsert(c *C, s Selector) {
	var err error
	schemaRules, tableRules := t.testGenerateExpectedRules()

	for schema, rule := range schemaRules {
		err = s.Insert(schema, "", rule, false)
		c.Assert(err, IsNil)
		err = s.Insert(schema, "", rule, false)
		c.Assert(err, NotNil)
		err = s.Insert(schema, "", rule, true)
		c.Assert(err, IsNil)
	}

	for schema, tables := range tableRules {
		for table, rule := range tables {
			err = s.Insert(schema, table, rule, false)
			c.Assert(err, IsNil)
			err = s.Insert(schema, table, rule, false)
			c.Assert(err, NotNil)
			err = s.Insert(schema, table, rule, true)
			c.Assert(err, IsNil)
		}
	}

	// insert wrong pattern
	err = s.Insert("sche*a", "", nil, true)
	c.Assert(err, NotNil)
	err = s.Insert("ab**", "", &dummyRule{"error"}, true)
	c.Assert(err, NotNil)
	err = s.Insert("abcd", "ab**", &dummyRule{"error"}, true)
	c.Assert(err, NotNil)

	schemas, tables := s.AllRules()
	c.Assert(schemas, DeepEquals, schemaRules)
	c.Assert(tables, DeepEquals, tableRules)
}

func (t *testSelectorSuite) testReplace(c *C, s Selector) {
	var err error
	schemaRules, tableRules := t.testGenerateExpectedRules()
	replacedRule := &dummyRule{"replace"}

	for schema := range schemaRules {
		schemaRules[schema] = replacedRule
		err = s.Insert(schema, "", replacedRule, false)
		c.Assert(err, NotNil)
		err = s.Insert(schema, "", replacedRule, true)
		c.Assert(err, IsNil)
	}

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
	tableRules := make(map[string]map[string]interface{})
	for schema, tables := range t.tables {
		_, ok := tableRules[schema]
		if !ok {
			tableRules[schema] = make(map[string]interface{})
		}
		for _, table := range tables {
			if len(table) == 0 {
				schemaRules[schema] = &dummyRule{quoateSchemaTable(schema, "")}
			} else {
				tableRules[schema][table] = &dummyRule{quoateSchemaTable(schema, table)}
			}
		}
	}

	return schemaRules, tableRules
}
