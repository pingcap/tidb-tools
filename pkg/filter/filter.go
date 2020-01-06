// Copyright 2019 PingCAP, Inc.
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
	"fmt"
	"regexp"
	"strings"
	"sync"

	"github.com/pingcap/errors"
	selector "github.com/pingcap/tidb-tools/pkg/table-rule-selector"
)

// ActionType is do or ignore something
type ActionType bool

// builtin actiontype variable
const (
	Do     ActionType = true
	Ignore ActionType = false
)

// Table represents a table.
type Table struct {
	Schema string `toml:"db-name" json:"db-name" yaml:"db-name"`
	Name   string `toml:"tbl-name" json:"tbl-name" yaml:"tbl-name"`
}

// String implements the fmt.Stringer interface.
func (t *Table) String() string {
	if len(t.Name) > 0 {
		return fmt.Sprintf("`%s`.`%s`", t.Schema, t.Name)
	}
	return fmt.Sprintf("`%s`", t.Schema)
}

type cache struct {
	sync.RWMutex
	items map[string]ActionType // `schema`.`table` => do/ignore
}

func (c *cache) query(key string) (ActionType, bool) {
	c.RLock()
	action, exist := c.items[key]
	c.RUnlock()

	return action, exist
}

func (c *cache) set(key string, action ActionType) {
	c.Lock()
	c.items[key] = action
	c.Unlock()
}

// Rules contains Filter rules.
type Rules struct {
	DoTables []*Table `json:"do-tables" toml:"do-tables" yaml:"do-tables"`
	DoDBs    []string `json:"do-dbs" toml:"do-dbs" yaml:"do-dbs"`

	IgnoreTables []*Table `json:"ignore-tables" toml:"ignore-tables" yaml:"ignore-tables"`
	IgnoreDBs    []string `json:"ignore-dbs" toml:"ignore-dbs" yaml:"ignore-dbs"`
}

// ToLower convert all entries to lowercase
func (r *Rules) ToLower() {
	if r == nil {
		return
	}

	for _, table := range r.DoTables {
		table.Name = strings.ToLower(table.Name)
		table.Schema = strings.ToLower(table.Schema)
	}
	for _, table := range r.IgnoreTables {
		table.Name = strings.ToLower(table.Name)
		table.Schema = strings.ToLower(table.Schema)
	}
	for i, db := range r.IgnoreDBs {
		r.IgnoreDBs[i] = strings.ToLower(db)
	}
	for i, db := range r.DoDBs {
		r.DoDBs[i] = strings.ToLower(db)
	}
}

// Filter implements whitelist and blacklist filters.
type Filter struct {
	selector.Selector
	patternMap map[string]*regexp.Regexp
	rules      *Rules

	c *cache

	caseSensitive bool
}

// New creates a filter use the rules.
func New(caseSensitive bool, rules *Rules) (*Filter, error) {
	if !caseSensitive {
		rules.ToLower()
	}

	f := &Filter{
		Selector:      selector.NewTrieSelector(),
		caseSensitive: caseSensitive,
		rules:         rules,
	}

	f.patternMap = make(map[string]*regexp.Regexp)
	f.c = &cache{
		items: make(map[string]ActionType),
	}
	err := f.initRules()
	if err != nil {
		return nil, err
	}
	return f, nil
}

const (
	dbRule = iota
	tblRuleFull
	tblRuleOnlyDBPart
	tblRuleOnlyTblPart
)

type nodeEndRule struct {
	kind        int
	r           *regexp.Regexp
	isWhiteList bool
}

// initRules initialize the rules to regex expr or trie node.
func (f *Filter) initRules() (err error) {
	if f.rules == nil {
		return
	}

	for _, db := range f.rules.DoDBs {
		if len(db) == 0 {
			return errors.Errorf("DoDB rule's DB string cannot be empty")
		}
		err = f.initSchemaRule(db, true)
		if err != nil {
			return
		}
	}

	for _, table := range f.rules.DoTables {
		if len(table.Schema) == 0 || len(table.Name) == 0 {
			return errors.Errorf("DoTables rule's DB string or Table string cannot be empty")
		}
		err = f.initTableRule(table.Schema, table.Name, true)
		if err != nil {
			return
		}
	}

	for _, db := range f.rules.IgnoreDBs {
		if len(db) == 0 {
			return errors.Errorf("IgnoreDB rule's DB string cannot be empty")
		}
		err = f.initSchemaRule(db, false)
		if err != nil {
			return
		}
	}

	for _, table := range f.rules.IgnoreTables {
		if len(table.Schema) == 0 || len(table.Name) == 0 {
			return errors.Errorf("IgnoreTables rule's DB string or Table string cannot be empty")
		}
		err = f.initTableRule(table.Schema, table.Name, false)
		if err != nil {
			return
		}
	}

	return
}

func (f *Filter) initOneRegex(originStr string) error {
	if _, ok := f.patternMap[originStr]; !ok {
		compileStr := originStr
		if !f.caseSensitive {
			compileStr = "(?i)" + compileStr
		}
		reg, err := regexp.Compile(compileStr)
		if err != nil {
			return errors.Trace(err)
		}
		f.patternMap[originStr] = reg
	}
	return nil
}

func (f *Filter) initSchemaRule(dbStr string, isWhiteList bool) error {
	if strings.HasPrefix(dbStr, "~") {
		return f.initOneRegex(dbStr[1:])
	}
	return f.Selector.Insert(dbStr, "", &nodeEndRule{
		kind:        dbRule,
		isWhiteList: isWhiteList,
	}, selector.Append)
}

func (f *Filter) initTableRule(dbStr, tableStr string, isWhiteList bool) error {
	dbIsRegex := strings.HasPrefix(dbStr, "~")
	tblIsRegex := strings.HasPrefix(tableStr, "~")
	if dbIsRegex && tblIsRegex {
		err := f.initOneRegex(dbStr[1:])
		if err != nil {
			return err
		}
		err = f.initOneRegex(tableStr[1:])
		if err != nil {
			return err
		}
	} else if dbIsRegex && !tblIsRegex {
		err := f.initOneRegex(dbStr[1:])
		if err != nil {
			return err
		}
		err = f.Selector.Insert(tableStr, "", &nodeEndRule{
			kind:        tblRuleOnlyTblPart,
			isWhiteList: isWhiteList,
		}, selector.Append)
		if err != nil {
			return err
		}
	} else if !dbIsRegex && tblIsRegex {
		err := f.initOneRegex(tableStr[1:])
		if err != nil {
			return err
		}
		err = f.Selector.Insert(dbStr, "", &nodeEndRule{
			kind:        tblRuleOnlyDBPart,
			r:           f.patternMap[tableStr[1:]],
			isWhiteList: isWhiteList,
		}, selector.Append)
		if err != nil {
			return err
		}
	} else {
		err := f.Selector.Insert(dbStr, tableStr, &nodeEndRule{
			kind:        tblRuleFull,
			isWhiteList: isWhiteList,
		}, selector.Append)
		if err != nil {
			return err
		}
	}
	return nil
}

// ApplyOn applies filter rules on tables
// rules like
// https://dev.mysql.com/doc/refman/8.0/en/replication-rules-table-options.html
// https://dev.mysql.com/doc/refman/8.0/en/replication-rules-db-options.html
func (f *Filter) ApplyOn(stbs []*Table) []*Table {
	if f == nil || f.rules == nil {
		return stbs
	}

	var tbs []*Table
	for _, tb := range stbs {
		if !f.caseSensitive {
			tb.Schema = strings.ToLower(tb.Schema)
			tb.Name = strings.ToLower(tb.Name)
		}

		if f.Match(tb) {
			tbs = append(tbs, tb)
		}
	}

	return tbs
}

// Match returns true if the specified table should not be removed.
func (f *Filter) Match(tb *Table) bool {
	name := tb.String()
	do, exist := f.c.query(name)
	if !exist {
		do = ActionType(f.filterOnSchemas(tb) && f.filterOnTables(tb))
		f.c.set(tb.String(), do)
	}
	return do == Do
}

func (f *Filter) filterOnSchemas(tb *Table) bool {
	if len(f.rules.DoDBs) > 0 {
		// not macthed do db rules, ignore update
		if !f.findMatchedDoDBs(tb) {
			return false
		}
	} else if len(f.rules.IgnoreDBs) > 0 {
		//  macthed ignore db rules, ignore update
		if f.findMatchedIgnoreDBs(tb) {
			return false
		}
	}

	return true
}

func (f *Filter) findMatchedDoDBs(tb *Table) bool {
	return f.matchDB(f.rules.DoDBs, tb.Schema, true)
}

func (f *Filter) findMatchedIgnoreDBs(tb *Table) bool {
	return f.matchDB(f.rules.IgnoreDBs, tb.Schema, false)
}

func (f *Filter) filterOnTables(tb *Table) bool {
	// schema statement like create/drop/alter database
	if len(tb.Name) == 0 {
		return true
	}

	if len(f.rules.DoTables) > 0 {
		if f.matchTable(f.rules.DoTables, tb, true) {
			return true
		}
	}

	if len(f.rules.IgnoreTables) > 0 {
		if f.matchTable(f.rules.IgnoreTables, tb, false) {
			return false
		}
	}

	return len(f.rules.DoTables) == 0
}

func (f *Filter) matchDB(patternDBS []string, a string, isWhiteListCheck bool) bool {
	for _, b := range patternDBS {
		isRegex := strings.HasPrefix(b, "~")
		if isRegex && f.matchString(b[1:], a) {
			return true
		}
	}
	ruleSet := f.Selector.Match(a, "")
	for _, r := range ruleSet {
		rule := r.(*nodeEndRule)
		if rule.kind == dbRule && rule.isWhiteList == isWhiteListCheck {
			return true
		}
	}
	return false
}

func (f *Filter) matchTable(patternTBS []*Table, tb *Table, isWhiteListCheck bool) bool {
	for _, ptb := range patternTBS {
		dbIsRegex, tblIsRegex := strings.HasPrefix(ptb.Schema, "~"), strings.HasPrefix(ptb.Name, "~")
		if dbIsRegex && tblIsRegex {
			if f.matchString(ptb.Schema[1:], tb.Schema) && f.matchString(ptb.Name[1:], tb.Name) {
				return true
			}
		} else if dbIsRegex && !tblIsRegex {
			if !f.matchString(ptb.Schema[1:], tb.Schema) {
				continue
			}
			ruleSet := f.Selector.Match(tb.Name, "")
			for _, r := range ruleSet {
				rule := r.(*nodeEndRule)
				if rule.kind == tblRuleOnlyTblPart && rule.isWhiteList == isWhiteListCheck {
					return true
				}
			}
		}
		ruleSet := f.Selector.Match(tb.Schema, "")
		for _, r := range ruleSet {
			rule := r.(*nodeEndRule)
			if rule.kind == tblRuleOnlyDBPart && rule.isWhiteList == isWhiteListCheck && rule.r.MatchString(tb.Name) {
				return true
			}
		}
		ruleSet = f.Selector.Match(tb.Schema, tb.Name)
		for _, r := range ruleSet {
			rule := r.(*nodeEndRule)
			if rule.kind == tblRuleFull && rule.isWhiteList == isWhiteListCheck {
				return true
			}
		}
	}

	return false
}

func (f *Filter) matchString(pattern string, t string) bool {
	if re, ok := f.patternMap[pattern]; ok {
		return re.MatchString(t)
	}
	return pattern == t
}
