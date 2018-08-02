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

package column

import (
	"fmt"
	"sync"

	"github.com/juju/errors"
	"github.com/pingcap/tidb-tools/pkg/table-rule-selector"
)

// Expr indicates how to handle column mapping
type Expr string

// poor Expr
const (
	AddPrefix Expr = "add prefix"
	AddSuffix Expr = "add suffix"
	Clone     Expr = "clone column"
)

// Exprs is some built-in expression for column mapping
// only support some poor expressions now, we would unify tableInfo later and support more
var Exprs = map[Expr]func(*columnInfo, []interface{}) []interface{}{
	AddPrefix: addPrefix,
	AddSuffix: addSuffix,
	Clone:     cloneColumn,
}

// Rule is a rule to map column
type Rule struct {
	PatternSchema    string   `yaml:"pattern-schema" json:"pattern-schema" toml:"pattern-schema"`
	PatternTable     string   `yaml:"pattern-table" json:"pattern-table" toml:"pattern-table"`
	SourceColumn     string   `yaml:"source-column" json:"source-column" toml:"source-column"` // modify, add refer column, ignore
	TargetColumn     string   `yaml:"target-column" json:"target-column" toml:"target-column"` // add column, modify
	Expression       Expr     `yaml:"expression" json:"expression" toml:"expression"`
	Arguments        []string `yaml:"arguments" json:"arguments" toml:"arguments"`
	CreateTableQuery string   `yaml:"create-table-query" json:"create-table-query" toml:"create-table-query"`
}

// Valid checks validity of rule.
// add prefix/suffix: it should have target column and one argument
// clone: it should have source and target column
func (r *Rule) Valid() error {
	if _, ok := Exprs[r.Expression]; !ok {
		return errors.NotFoundf("expression %s", r.Expression)
	}

	if (r.Expression == AddPrefix || r.Expression == AddSuffix) && len(r.Arguments) != 1 {
		return errors.NotValidf("arguments %v for add prefix/suffix", r.Arguments)
	}

	return nil
}

// check source and target position
func (r *Rule) adjustColumnPosition(source, target int) (int, int, error) {
	// if not found target, ignore it
	if target == -1 {
		return source, target, errors.NotFoundf("target column %s", r.TargetColumn)
	}

	if r.Expression == Clone {
		if source == -1 {
			return source, target, errors.NotFoundf("source column %s", r.SourceColumn)
		}

		if target < source { // must add column and added column is before source column
			source--
		}
	}

	return source, target, nil
}

type columnInfo struct {
	ignore         bool
	sourcePosition int
	targetPosition int
	rule           *Rule
}

// Mapping maps column to something by rules
type Mapping struct {
	selector.Selector

	cache struct {
		sync.RWMutex
		infos map[string]*columnInfo
	}
}

// NewMapping returns a column mapping
func NewMapping(rules []*Rule) (*Mapping, error) {
	m := &Mapping{
		Selector: selector.NewTrieSelector(),
	}
	m.resetCache()

	for _, rule := range rules {
		if err := m.AddRule(rule); err != nil {
			return nil, errors.Annotatef(err, "initial rule %+v in mapping", rule)
		}
	}

	return m, nil
}

// AddRule adds a rule into mapping
func (m *Mapping) AddRule(rule *Rule) error {
	err := rule.Valid()
	if err != nil {
		return errors.Trace(err)
	}

	m.resetCache()
	err = m.Insert(rule.PatternSchema, rule.PatternTable, rule, false)
	if err != nil {
		return errors.Annotatef(err, "add rule %+v into mapping", rule)
	}

	return nil
}

// UpdateRule updates mapping rule
func (m *Mapping) UpdateRule(rule *Rule) error {
	err := rule.Valid()
	if err != nil {
		return errors.Trace(err)
	}

	m.resetCache()
	err = m.Insert(rule.PatternSchema, rule.PatternTable, rule, true)
	if err != nil {
		return errors.Annotatef(err, "update rule %+v in mapping", rule)
	}

	return nil
}

// RemoveRule removes a rule from mapping
func (m *Mapping) RemoveRule(rule *Rule) error {
	m.resetCache()
	err := m.Remove(rule.PatternSchema, rule.PatternTable)
	if err != nil {
		return errors.Annotatef(err, "remove rule %+v", rule)
	}

	return nil
}

// HandleRowValue handles row value
func (m *Mapping) HandleRowValue(schema, table string, columns []string, vals []interface{}) ([]interface{}, error) {
	info, err := m.queryColumnInfo(schema, table, columns)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if info.ignore == true {
		return vals, nil
	}

	exp, ok := Exprs[info.rule.Expression]
	if !ok {
		return nil, errors.NotFoundf("column mapping expression %s", info.rule.Expression)
	}

	return exp(info, vals), nil
}

// HandleDDL handles ddl
func (m *Mapping) HandleDDL(schema, table string, columns []string, statement string) (string, error) {
	info, err := m.queryColumnInfo(schema, table, columns)
	if err != nil {
		return statement, errors.Trace(err)
	}

	if info.ignore == true {
		return statement, nil
	}

	m.resetCache()
	// only output erro now, wait fix it manually
	return statement, errors.NotImplementedf("ddl %s @ column mapping rule %s/%s:%+v", statement, schema, table, info.rule)
}

func (m *Mapping) queryColumnInfo(schema, table string, columns []string) (*columnInfo, error) {
	m.cache.RLock()
	ci, ok := m.cache.infos[tableName(schema, table)]
	m.cache.RUnlock()
	if ok {
		return ci, nil
	}

	var info = &columnInfo{
		ignore: true,
	}
	rules := m.Match(schema, table)
	if len(rules) == 0 {
		m.cache.Lock()
		m.cache.infos[tableName(schema, table)] = info
		m.cache.Unlock()

		return info, nil
	}

	var (
		schemaRules []*Rule
		tableRules  = make([]*Rule, 0, 1)
	)
	// classify rules into schema level rules and table level
	// table level rules have highest priority
	for i := range rules {
		rule, ok := rules[i].(*Rule)
		if !ok {
			return nil, errors.NotValidf("column mapping rule %+v", rules[i])
		}

		if len(rule.PatternTable) == 0 {
			schemaRules = append(schemaRules, rule)
		} else {
			tableRules = append(tableRules, rule)
		}
	}

	// only support one expression for one table now, refine it later
	var rule *Rule
	if len(table) == 0 || len(tableRules) == 0 {
		if len(schemaRules) > 1 {
			return nil, errors.NotSupportedf("route %s/%s to rule set(%d)", schema, table, len(schemaRules))
		}

		if len(schemaRules) == 1 {
			rule = schemaRules[0]
		}
	} else {
		if len(tableRules) > 1 {
			return nil, errors.NotSupportedf("route %s/%s to rule set(%d)", schema, table, len(tableRules))
		}

		if len(tableRules) == 1 {
			rule = tableRules[0]
		}
	}
	if rule == nil {
		m.cache.Lock()
		m.cache.infos[tableName(schema, table)] = info
		m.cache.Unlock()

		return info, nil
	}

	sourcePosition := findColumnPosition(columns, rule.SourceColumn)
	targetPosition := findColumnPosition(columns, rule.TargetColumn)

	sourcePosition, targetPosition, err := rule.adjustColumnPosition(sourcePosition, targetPosition)
	if err != nil {
		return nil, errors.Trace(err)
	}

	info = &columnInfo{
		sourcePosition: sourcePosition,
		targetPosition: targetPosition,
		rule:           rule,
	}

	m.cache.Lock()
	m.cache.infos[tableName(schema, table)] = info
	m.cache.Unlock()

	return info, nil
}

func (m *Mapping) resetCache() {
	m.cache.Lock()
	m.cache.infos = make(map[string]*columnInfo)
	m.cache.Unlock()
}

func findColumnPosition(cols []string, col string) int {
	for i := range cols {
		if cols[i] == col {
			return i
		}
	}

	return -1
}

func tableName(schema, table string) string {
	return fmt.Sprintf("`%s`.`%s`", schema, table)
}

func addPrefix(info *columnInfo, vals []interface{}) []interface{} {
	prefix := info.rule.Arguments[0]
	vals[info.targetPosition] = fmt.Sprintf("%s:%v", prefix, vals[info.targetPosition])
	return vals
}

func addSuffix(info *columnInfo, vals []interface{}) []interface{} {
	suffix := info.rule.Arguments[0]
	vals[info.targetPosition] = fmt.Sprintf("%v:%s", vals[info.targetPosition], suffix)
	return vals
}

func cloneColumn(info *columnInfo, vals []interface{}) []interface{} {
	newVals := make([]interface{}, 0, len(vals)+1)
	newVals = append(newVals, vals[:info.targetPosition]...)
	newVals = append(newVals, vals[info.sourcePosition])
	newVals = append(newVals, vals[info.targetPosition:]...)

	return newVals
}
