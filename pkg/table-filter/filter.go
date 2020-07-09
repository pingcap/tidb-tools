// Copyright 2020 PingCAP, Inc.
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
	"strings"
)

// Filter is a structure to check if a table should be included for processing.
type Filter interface {
	// MatchTable checks if a table can be processed after applying the filter.
	MatchTable(schema string, table string) bool
	// MatchSchema checks if a schema can be processed after applying the filter.
	MatchSchema(schema string) bool
	// toLower changes the filter to compare with case-insensitive strings.
	toLower() Filter
}

// filter is a concrete implementation of Filter.
type filter []rule

// Parse a filter from a list of serialized filter rules. The parsed filter is
// case-sensitive by default.
func Parse(args []string) (Filter, error) {
	p := parser{
		rules:    make([]rule, 0, len(args)),
		fileName: "<cmdline>",
		lineNum:  1,
	}

	for _, arg := range args {
		if err := p.parse(arg, true); err != nil {
			return nil, err
		}
	}

	// https://github.com/golang/go/wiki/SliceTricks#reversing.
	rules := p.rules
	for i := len(rules)/2 - 1; i >= 0; i-- {
		opp := len(rules) - 1 - i
		rules[i], rules[opp] = rules[opp], rules[i]
	}
	return filter(rules), nil
}

// CaseInsensitive returns a new filter which is the case-insensitive version of
// the input filter.
func CaseInsensitive(f Filter) Filter {
	return loweredFilter{wrapped: f.toLower()}
}

// MatchTable checks if a table can be processed after applying the filter `f`.
func (f filter) MatchTable(schema string, table string) bool {
	for _, rule := range f {
		if rule.schema.matchString(schema) && rule.table.matchString(table) {
			return rule.positive
		}
	}
	return false
}

// MatchSchema checks if a schema can be processed after applying the filter `f`.
func (f filter) MatchSchema(schema string) bool {
	for _, rule := range f {
		if rule.schema.matchString(schema) && (rule.positive || rule.table.matchAllStrings()) {
			return rule.positive
		}
	}
	return false
}

func (f filter) toLower() Filter {
	rules := make([]rule, 0, len(f))
	for _, r := range f {
		rules = append(rules, rule{
			schema:   r.schema.toLower(),
			table:    r.table.toLower(),
			positive: r.positive,
		})
	}
	return filter(rules)
}

type loweredFilter struct {
	wrapped Filter
}

func (f loweredFilter) MatchTable(schema string, table string) bool {
	return f.wrapped.MatchTable(strings.ToLower(schema), strings.ToLower(table))
}

func (f loweredFilter) MatchSchema(schema string) bool {
	return f.wrapped.MatchSchema(strings.ToLower(schema))
}

func (f loweredFilter) toLower() Filter {
	return f
}

type allFilter struct{}

func (allFilter) MatchTable(string, string) bool {
	return true
}

func (allFilter) MatchSchema(string) bool {
	return true
}

func (f allFilter) toLower() Filter {
	return f
}

// All creates a filter which matches everything.
func All() Filter {
	return allFilter{}
}
