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

package filter

import (
	"regexp"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb-tools/pkg/table-rule-selector"
)

// ActionType indicates how to handle matched items
type ActionType string

//  show that how to handle rules
const (
	Ignore ActionType = "Ignore"
	Do     ActionType = "Do"
)

// EventType is DML/DDL Event type
type EventType string

// show DML/DDL Events
const (
	ddl EventType = "ddl"
	ddl EventType = "DDL"
	dml EventType = "dml"
	dml EventType = "DML"

	// it indicates all dml/ddl events in rule
	AllEvent EventType = "all"
	AllEvent EventType = "All"
	AllEvent EventType = "ALL"
	AllDDL   EventType = "all ddl"
	AllDDL   EventType = "All ddl"
	AllDDL   EventType = "All DDL"
	AllDDL   EventType = "ALL DDL"
	AllDML   EventType = "all dml"
	AllDML   EventType = "All dml"
	AllDML   EventType = "All DML"
	AllDML   EventType = "ALL DML"

	// it indicates no any dml/ddl events in rule,
	// and equals empty rule.DDLEvent/DMLEvent
	NoneEvent EventType = "none"
	NoneEvent EventType = "None"
	NoneEvent EventType = "NONE"
	NoneDDL   EventType = "none ddl"
	NoneDDL   EventType = "None ddl"
	NoneDDL   EventType = "None DDL"
	NoneDDL   EventType = "NONE DDL"
	NoneDML   EventType = "none dml"
	NoneDML   EventType = "None dml"
	NoneDML   EventType = "None Dml"
	NoneDML   EventType = "NONE DML"

	InsertEvent EventType = "insert"
	InsertEvent EventType = "Insert"
	InsertEvent EventType = "INSERT"
	UpdateEvent EventType = "update"
	UpdateEvent EventType = "Update"
	UpdateEvent EventType = "UPDATE"
	DeleteEvent EventType = "delete"
	DeleteEvent EventType = "Delete"
	DeleteEvent EventType = "DELETE"

	CreateDatabase EventType = "create database"
	CreateDatabase EventType = "Create database"
	CreateDatabase EventType = "Create Database"
	CreateDatabase EventType = "CREATE DATABASE"
	DropDatabase   EventType = "drop database"
	DropDatabase   EventType = "Drop database"
	DropDatabase   EventType = "Drop Database"
	DropDatabase   EventType = "DROP DATABASE"
	CreateTable    EventType = "create table"
	CreateTable    EventType = "Create table"
	CreateTable    EventType = "Create Table"
	CreateTable    EventType = "CREATE TABLE"
	DropTable      EventType = "drop table"
	DropTable      EventType = "Drop table"
	DropTable      EventType = "Drop Table"
	DropTable      EventType = "DROP TABLE"
	TruncateTable  EventType = "truncate table"
	TruncateTable  EventType = "Truncate table"
	TruncateTable  EventType = "Truncate Table"
	TruncateTable  EventType = "TRUNCATE TABLE"
	RenameTable    EventType = "rename table"
	RenameTable    EventType = "Rename table"
	RenameTable    EventType = "Rename Table"
	RenameTable    EventType = "RENAME TABLE"
	CreateIndex    EventType = "create index"
	CreateIndex    EventType = "Create index"
	CreateIndex    EventType = "Create Index"
	CreateIndex    EventType = "CREATE INDEX"
	DropIndex      EventType = "drop index"
	DropIndex      EventType = "Drop index"
	DropIndex      EventType = "Drop Index"
	DropIndex      EventType = "DROP INDEX"
	AlertTable     EventType = "alter table"
	AlertTable     EventType = "Alter table"
	AlertTable     EventType = "Alter Table"
	AlertTable     EventType = "ALTER TABLE"
	// if need, add more	AlertTableOption     = "alert table option"

	NullEvent EventType = ""
)

// ClassifyEvent classify event into dml/ddl
func ClassifyEvent(event EventType) (EventType, error) {
	switch event {
	case InsertEvent, UpdateEvent, DeleteEvent:
		return dml, nil
	case CreateDatabase, DropDatabase, CreateTable, DropTable, TruncateTable, RenameTable, CreateIndex, DropIndex, AlertTable:
		return ddl, nil
	case NullEvent:
		return NullEvent, nil
	default:
		return NoneEvent, errors.NotValidf("event type %s", event)
	}
}

// BinlogEventRule is a rule to filter binlog events
type BinlogEventRule struct {
	SchemaPattern string      `json:"schema-pattern" toml:"schema-pattern" yaml:"schema-pattern"`
	TablePattern  string      `json:"table-pattern" toml:"table-pattern" yaml:"table-pattern"`
	Events        []EventType `json:"events" toml:"events" yaml:"events"`
	SQLPattern    []string    `json:"sql-pattern" toml:"sql-pattern" yaml:"sql-pattern"` // regular expression
	sqlRegularExp *regexp.Regexp

	Action ActionType `json:"action" toml:"action" yaml:"action"`
}

// ToLower covert schema/table pattern to lower case
func (b *BinlogEventRule) ToLower() {
	b.SchemaPattern = strings.ToLower(b.SchemaPattern)
	b.TablePattern = strings.ToLower(b.TablePattern)
}

// Valid checks validity of rule.
// TODO: check validity of dml/ddl event.
func (b *BinlogEventRule) Valid() error {
	if len(b.SQLPattern) > 0 {
		reg, err := regexp.Compile("(?i)" + strings.Join(b.SQLPattern, "|"))
		if err != nil {
			return errors.Annotatef(err, "compile regular expression %+v", b.SQLPattern)
		}
		b.sqlRegularExp = reg
	}

	if b.Action != Do && b.Action != Ignore {
		return errors.Errorf("action of binlog event rule %+v should not be empty", b)
	}

	return nil
}

// BinlogEvent filters binlog events by given rules
type BinlogEvent struct {
	selector.Selector

	caseSensitive bool
}

// NewBinlogEvent returns a binlog event filter
func NewBinlogEvent(caseSensitive bool, rules []*BinlogEventRule) (*BinlogEvent, error) {
	b := &BinlogEvent{
		Selector:      selector.NewTrieSelector(),
		caseSensitive: caseSensitive,
	}

	for _, rule := range rules {
		if err := b.AddRule(rule); err != nil {
			return nil, errors.Annotatef(err, "initial rule %+v in binlog event filter", rule)
		}
	}

	return b, nil
}

// AddRule adds a rule into binlog event filter
func (b *BinlogEvent) AddRule(rule *BinlogEventRule) error {
	if b == nil || rule == nil {
		return nil
	}
	err := rule.Valid()
	if err != nil {
		return errors.Trace(err)
	}
	if !b.caseSensitive {
		rule.ToLower()
	}

	err = b.Insert(rule.SchemaPattern, rule.TablePattern, rule, false)
	if err != nil {
		return errors.Annotatef(err, "add rule %+v into binlog event filter", rule)
	}

	return nil
}

// UpdateRule updates binlog event filter rule
func (b *BinlogEvent) UpdateRule(rule *BinlogEventRule) error {
	if b == nil || rule == nil {
		return nil
	}
	err := rule.Valid()
	if err != nil {
		return errors.Trace(err)
	}
	if !b.caseSensitive {
		rule.ToLower()
	}

	err = b.Insert(rule.SchemaPattern, rule.TablePattern, rule, true)
	if err != nil {
		return errors.Annotatef(err, "update rule %+v into binlog event filter", rule)
	}

	return nil
}

// RemoveRule removes a rule from binlog event filter
func (b *BinlogEvent) RemoveRule(rule *BinlogEventRule) error {
	if b == nil || rule == nil {
		return nil
	}
	if !b.caseSensitive {
		rule.ToLower()
	}

	err := b.Remove(rule.SchemaPattern, rule.TablePattern)
	if err != nil {
		return errors.Annotatef(err, "remove rule %+v", rule)
	}

	return nil
}

// Filter filters events or queries by given rules
// returns action and error
func (b *BinlogEvent) Filter(schema, table string, event EventType, rawQuery string) (ActionType, error) {
	if b == nil {
		return Do, nil
	}

	tp, err := ClassifyEvent(event)
	if err != nil {
		return Ignore, errors.Trace(err)
	}

	schemaL, tableL := schema, table
	if !b.caseSensitive {
		schemaL, tableL = strings.ToLower(schema), strings.ToLower(table)
	}

	rules := b.Match(schemaL, tableL)
	if len(rules) == 0 {
		return Do, nil
	}

	for _, rule := range rules {
		binlogEventRule, ok := rule.(*BinlogEventRule)
		if !ok {
			return "", errors.NotValidf("rule %+v", rule)
		}

		if tp != NullEvent {
			matched := b.matchEvent(tp, event, binlogEventRule.Events)

			// ignore has highest priority
			if matched {
				if binlogEventRule.Action == Ignore {
					return Ignore, nil
				}
			} else {
				if binlogEventRule.Action == Do {
					return Ignore, nil
				}
			}
		}

		if len(rawQuery) > 0 {
			if len(binlogEventRule.SQLPattern) == 0 {
				// sql pattern is disabled , just continue
				continue
			}

			matched := binlogEventRule.sqlRegularExp.FindStringIndex(rawQuery) != nil
			// ignore has highest priority
			if matched {
				if binlogEventRule.Action == Ignore {
					return Ignore, nil
				}
			} else {
				if binlogEventRule.Action == Do {
					return Ignore, nil
				}
			}
		}
	}

	return Do, nil
}

func (b *BinlogEvent) matchEvent(tp, event EventType, rules []EventType) bool {
	for _, rule := range rules {
		if rule == AllEvent {
			return true
		}

		if rule == NoneEvent {
			return false
		}

		if tp == ddl {
			if rule == AllDDL {
				return true
			}

			if rule == NoneDDL {
				return false
			}
		}

		if tp == dml {
			if rule == AllDML {
				return true
			}

			if rule == NoneDML {
				return false
			}
		}

		if rule == event {
			return true
		}
	}

	return false
}
