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
	// InsertSchema will insert one rule of schema into trie
	InsertSchema(schema string, rule interface{}) error
	// InsertTable will insert one rule of schema/table into trie
	InsertTable(schema, table string, rule interface{}) error
	// Match will return matched rule
	// now Match only returns one matched rule, priorities are as follows
	// * which level matched rule to return
	// 1. if table is empty or macthed rule of table level doesn't exist, return matched rule of schema level;
	// 2. otherwise return matched rule of table level.
	// * which matched rule in one level to return
	// 1. the first(shortest) matched rule.
	Match(schema, table string) interface{}
	// Remove will remove one rule
	Remove(schema, table string) error
	// AllRules will returns all rules
	AllRules() (map[string]interface{}, map[string]map[string]interface{})
}

type macthedResult struct {
	nodes []*node
	rules []interface{}
}

func (r *macthedResult) empty() bool {
	return len(r.nodes) == 0 && len(r.rules) == 0
}

type trieSelector struct {
	sync.RWMutex

	cache map[string]interface{}
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
	nextLevel *node
}

func newNode() *node {
	return &node{characters: make(map[byte]*item)}
}

// NewTrieSelector returns a trie Selector
func NewTrieSelector() Selector {
	return &trieSelector{cache: make(map[string]interface{}), root: newNode()}
}

// InsertSchema implements Selector's interface.
func (t *trieSelector) InsertSchema(schema string, rule interface{}) error {
	if len(schema) == 0 || rule == nil {
		return errors.Errorf("schema pattern %s or rule %v can't be empty", schema, rule)
	}

	t.Lock()
	_, err := t.insert(t.root, schema, rule)
	t.Unlock()
	if err != nil {
		return errors.Annotate(err, "insert into schema selector")
	}

	return nil
}

// InsertTable implements Selector's interface.
func (t *trieSelector) InsertTable(schema, table string, rule interface{}) error {
	if len(schema) == 0 || len(table) == 0 || rule == nil {
		return errors.Errorf("schema/table pattern %s/%s or rule %v can't be empty", schema, table, rule)
	}

	t.Lock()
	schemaEntity, err := t.insert(t.root, schema, nil)
	if err != nil {
		t.Unlock()
		return errors.Annotate(err, "insert into schema selector")
	}

	if schemaEntity.nextLevel == nil {
		schemaEntity.nextLevel = newNode()
	}

	_, err = t.insert(schemaEntity.nextLevel, table, rule)
	t.Unlock()
	if err != nil {
		return errors.Annotate(err, "insert into table selector")
	}

	return nil
}

// if rule is nil, just extract nodes
func (t *trieSelector) insert(root *node, pattern string, rule interface{}) (*item, error) {
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
		if entity.rule != nil {
			return nil, errors.AlreadyExistsf("pattern %s", pattern)
		}
		entity.rule = rule
		t.clearCache()
	}

	return entity, nil
}

// Match implements Selector's interface.
func (t *trieSelector) Match(schema, table string) interface{} {
	// try to find schema/table in cache
	t.RLock()
	cacheKey := quoateSchemaTable(schema, table)
	rule, ok := t.cache[cacheKey]
	t.RUnlock()
	if ok {
		return rule
	}

	// find matched rules
	t.Lock()
	var (
		matchedSchemaResult = &macthedResult{
			nodes: make([]*node, 0, 4),
			rules: make([]interface{}, 0, 4),
		}
		matchedSchemaRule interface{}
	)
	t.matchNode(t.root, schema, matchedSchemaResult)

	// not found matched rules in schema level
	if matchedSchemaResult.empty() {
		t.addToCache(cacheKey, nil)
		t.Unlock()
		return nil
	}

	// find first(shortest) matched rule in schema level
	if len(matchedSchemaResult.rules) > 0 {
		matchedSchemaRule = matchedSchemaResult.rules[0]
	}

	// only need to find schema level matched rule
	if len(table) == 0 {
		t.addToCache(cacheKey, matchedSchemaRule)
		t.Unlock()
		return matchedSchemaRule
	}

	for _, si := range matchedSchemaResult.nodes {
		matchedTableResult := &macthedResult{
			rules: make([]interface{}, 0, 4),
		}
		// find matched rules in table level
		t.matchNode(si, table, matchedTableResult)
		if len(matchedTableResult.rules) > 0 {
			t.addToCache(cacheKey, matchedTableResult.rules[0])
			t.Unlock()
			return matchedTableResult.rules[0]
		}
	}
	// not found matched rule in table level, return mathed rule in schema level
	t.addToCache(cacheKey, matchedSchemaRule)
	t.Unlock()
	return matchedSchemaRule
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
		tableRules[schema] = rules
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

func (t *trieSelector) addToCache(key string, targets interface{}) {
	t.cache[key] = targets
	if len(t.cache) > maxCacheNum {
		for literal := range t.cache {
			delete(t.cache, literal)
			break
		}
	}
}

func (t *trieSelector) clearCache() {
	t.cache = make(map[string]interface{})
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
