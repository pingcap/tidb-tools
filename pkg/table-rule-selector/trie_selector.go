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
	"fmt"
	"sync"

	"github.com/juju/errors"
)

// 1. asterisk character (*, also called "star") matches zero or more characters,
//    for example, doc* matches doc and document but not dodo;
//    asterisk character must be the last character of wildcard word.
// 2. the question mark ? matches exactly one character
const (
	// asterisk [ * ]
	asterisk = '*'
	// question mark [ ? ]
	question = '?'
)

const maxCacheNum = 1024

// Selector stores rules of schema/table for easy retrieval
type Selector interface {
	// Insert will insert one rule into trie
	// if table is empty, insert rule into schema level
	// otherwise insert rule into table level
	Insert(schema, table string, rule interface{}, replace bool) error
	// Match will return all matched rules
	Match(schema, table string) RuleSet
	// Remove will remove one rule
	Remove(schema, table string) error
	// AllRules will returns all rules
	AllRules() (map[string]interface{}, map[string]map[string]interface{})
}

// RuleSet is a set of rules that selected
type RuleSet []interface{}

func (r RuleSet) clone() RuleSet {
	if r == nil {
		return nil
	}

	c := make(RuleSet, 0, len(r))
	return append(c, r...)
}

type macthedResult struct {
	nodes []*node
	rules RuleSet
}

func (r *macthedResult) empty() bool {
	return r == nil || (len(r.nodes) == 0 && len(r.rules) == 0)
}

type trieSelector struct {
	sync.RWMutex

	cache map[string]RuleSet
	root  *node
}

type node struct {
	characters         map[byte]*item
	asterisk, question *item
}

type item struct {
	child *node

	rule interface{}
	// schema level ->(to) table level
	nextLevel        *node
	nextLevelRuleNum int
}

func newNode() *node {
	return &node{characters: make(map[byte]*item)}
}

// NewTrieSelector returns a trie Selector
func NewTrieSelector() Selector {
	return &trieSelector{cache: make(map[string]RuleSet), root: newNode()}
}

// Insert implements Selector's interface.
func (t *trieSelector) Insert(schema, table string, rule interface{}, replace bool) error {
	if len(schema) == 0 || rule == nil {
		return errors.Errorf("schema pattern %s or rule %v can't be empty", schema, rule)
	}

	var err error
	t.Lock()
	if len(table) == 0 {
		err = t.insertSchema(schema, rule, replace)
	} else {
		err = t.InsertTable(schema, table, rule, replace)
	}
	t.Unlock()

	return errors.Trace(err)
}

func (t *trieSelector) insertSchema(schema string, rule interface{}, replace bool) error {
	_, err := t.insert(t.root, schema, rule, replace)
	if err != nil {
		return errors.Annotate(err, "insert into schema selector")
	}

	return nil
}

func (t *trieSelector) InsertTable(schema, table string, rule interface{}, replace bool) error {
	schemaEntity, err := t.insert(t.root, schema, nil, false)
	if err != nil {
		return errors.Annotate(err, "insert into schema selector")
	}

	if schemaEntity.nextLevel == nil {
		schemaEntity.nextLevel = newNode()
	}

	_, err = t.insert(schemaEntity.nextLevel, table, rule, replace)
	if err != nil {
		return errors.Annotate(err, "insert into table selector")
	}

	return nil
}

// if rule is nil, just extract nodes
func (t *trieSelector) insert(root *node, pattern string, rule interface{}, replace bool) (*item, error) {
	var (
		n           = root
		hadAsterisk = false
		entity      *item
	)

	for i := range pattern {
		if hadAsterisk {
			return nil, errors.Errorf("pattern %s is invaild", pattern)
		}

		switch pattern[i] {
		case asterisk:
			entity = n.asterisk
			hadAsterisk = true
		case question:
			entity = n.question
		default:
			entity = n.characters[pattern[i]]
		}
		if entity == nil {
			entity = &item{}
			switch pattern[i] {
			case asterisk:
				n.asterisk = entity
			case question:
				n.question = entity
			default:
				n.characters[pattern[i]] = entity
			}
		}
		if entity.child == nil {
			entity.child = newNode()
		}
		n = entity.child
	}

	if rule != nil {
		if !replace && entity.rule != nil {
			return nil, errors.AlreadyExistsf("pattern %s", pattern)
		}
		entity.rule = rule
		t.clearCache()
	}

	return entity, nil
}

// Match implements Selector's interface.
func (t *trieSelector) Match(schema, table string) RuleSet {
	// try to find schema/table in cache
	t.RLock()
	cacheKey := quoateSchemaTable(schema, table)
	rules, ok := t.cache[cacheKey]
	t.RUnlock()
	if ok {
		return rules.clone()
	}

	matchedSchemaResult := &macthedResult{
		nodes: make([]*node, 0, 4),
		rules: make(RuleSet, 0, 4),
	}
	rules = nil

	// find matched rules
	t.Lock()
	defer t.Unlock()
	t.matchNode(t.root, schema, matchedSchemaResult)

	// not found matched rules in schema level
	if matchedSchemaResult.empty() {
		t.addToCache(cacheKey, nil)
		return nil
	}

	rules = append(rules, matchedSchemaResult.rules...)

	// only need to find schema level matched rule
	if len(table) == 0 {
		t.addToCache(cacheKey, rules)
		return rules.clone()
	}

	for _, si := range matchedSchemaResult.nodes {
		matchedTableResult := &macthedResult{
			rules: make(RuleSet, 0, 4),
		}
		// find matched rules in table level
		t.matchNode(si, table, matchedTableResult)
		rules = append(rules, matchedTableResult.rules...)
	}
	// not found matched rule in table level, return mathed rule in schema level
	t.addToCache(cacheKey, rules)
	return rules.clone()
}

// Remove implements Selector interface.
// Not implemention.
func (t *trieSelector) Remove(schema, table string) error {
	return nil
}

// AllRules implements Router's AllRules
func (t *trieSelector) AllRules() (map[string]interface{}, map[string]map[string]interface{}) {
	var (
		tableRules  = make(map[string]map[string]interface{})
		schemaNodes = make(map[string]*node)
		schemaRules = make(map[string]interface{})
		characters  []byte
	)
	t.RLock()
	t.travel(t.root, characters, schemaRules, schemaNodes)

	for schema, n := range schemaNodes {
		rules, ok := tableRules[schema]
		if !ok {
			rules = make(map[string]interface{})
		}

		characters = characters[:0]
		t.travel(n, characters, rules, nil)
		if len(rules) > 0 {
			tableRules[schema] = rules
		}
	}
	t.RUnlock()
	return schemaRules, tableRules
}

func (t *trieSelector) travel(n *node, characters []byte, rules map[string]interface{}, nodes map[string]*node) {
	if n == nil {
		return
	}

	if n.asterisk != nil {
		if n.asterisk != nil {
			pattern := append(characters, asterisk)
			insertMatchedItemIntoMap(string(pattern), n.asterisk, rules, nodes)
		}
	}

	if n.question != nil {
		pattern := append(characters, question)
		if n.question != nil {
			insertMatchedItemIntoMap(string(pattern), n.question, rules, nodes)
		}
		t.travel(n.question.child, pattern, rules, nodes)
	}

	for char, item := range n.characters {
		pattern := append(characters, char)
		if item != nil {
			insertMatchedItemIntoMap(string(pattern), item, rules, nodes)
		}
		t.travel(item.child, pattern, rules, nodes)
	}
}

func (t *trieSelector) matchNode(n *node, s string, mr *macthedResult) {
	if n == nil {
		return
	}

	var (
		ok     bool
		entity *item
	)
	for i := range s {
		if n.asterisk != nil {
			appendMatchedItem(n.asterisk, mr)
		}

		if n.question != nil {
			if i == len(s)-1 {
				appendMatchedItem(n.question, mr)
			}

			t.matchNode(n.question.child, s[i+1:], mr)
		}

		entity, ok = n.characters[s[i]]
		if !ok {
			return
		}
		n = entity.child
	}

	if entity != nil {
		appendMatchedItem(entity, mr)
	}

	if n.asterisk != nil {
		appendMatchedItem(n.asterisk, mr)
	}
}

func appendMatchedItem(entity *item, mr *macthedResult) {
	if entity.rule != nil {
		mr.rules = append(mr.rules, entity.rule)
	}

	if entity.nextLevel != nil {
		mr.nodes = append(mr.nodes, entity.nextLevel)
	}
}

func insertMatchedItemIntoMap(pattern string, entity *item, rules map[string]interface{}, nodes map[string]*node) {
	if rules != nil && entity.rule != nil {
		rules[pattern] = entity.rule
	}

	if nodes != nil && entity.nextLevel != nil {
		nodes[pattern] = entity.nextLevel
	}
}

func (t *trieSelector) addToCache(key string, rules RuleSet) {
	t.cache[key] = rules
	if len(t.cache) > maxCacheNum {
		for literal := range t.cache {
			delete(t.cache, literal)
			break
		}
	}
}

func (t *trieSelector) clearCache() {
	t.cache = make(map[string]RuleSet)
}

func quoateSchemaTable(schema, table string) string {
	if len(schema) == 0 {
		return ""
	}

	if len(table) > 0 {
		return fmt.Sprintf("`%s`.`%s`", schema, table)
	}

	return fmt.Sprintf("`%s`", schema)
}
