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

package parser

import (
	"fmt"
	"strings"

	filter "github.com/pingcap/tidb-tools/pkg/binlog-filter"
	"github.com/pingcap/tidb-tools/pkg/table-router"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/parser"
)

// Parser defines an interface to parser and transform statement
// and returns transformed statements, and whether to support it (need to skip it)
type Parser interface {
	Handle(schema, statement string) ([]string, bool, error)
}

// tidbParser provides more richer features base on tidb/parser:
// * return unsupported statements explicitly
// * custom filter DDL
// * poor transformation features
//   ** transform incompatible statements
//   ** custom table name transformation
//   ** custom column mapping (change column definition)
// it's case sensitive
type tidbParser struct {
	*parser.Parser

	router *router.Table
	filter *filter.BinlogEvent
}

// NewTiDBParser returns a Parser that base on tidb parser
func NewTiDBParser(router *router.Table, filter *filter.BinlogEvent) Parser {
	return &tidbParser{
		Parser: parser.New(),

		router: router,
		filter: filter,
	}
}

// Handle implements Parser interface.
func (t *tidbParser) Handle(schema, statement string) ([]string, bool, error) {
	// filter unsupported ddl (use schema:* table:* global rule)
	action, err := t.filter.Filter("", "", filter.NullEvent, filter.NullEvent, statement)
	if err != nil {
		return nil, false, errors.Annotate(err, "filter unsupported ddl")
	}
	if action == filter.Ignore {
		// need to skip it
		return nil, true, nil
	}

	nodes, err := t.Parse(statement, "", "")
	if err != nil {
		return nil, false, errors.Annotatef(err, "parser ddl %s", statement)
	}
	if len(nodes) == 0 { // skip it
		return nil, true, nil
	}

	node := nodes[0]
	_, isDDL := node.(ast.DDLNode)
	if !isDDL {
		// let sqls be empty
		return nil, true, nil
	}

	sqls, err := t.handleDDL(schema, statement, node)
	if err != nil {
		return nil, false, errors.Annotatef(err, "handle ddl %s", statement)
	}

	return sqls, false, nil
}

func (t *tidbParser) handleDDL(schema, statement string, node ast.StmtNode) ([]string, error) {
	sqls := make([]string, 0, 1)
	// poor filter/transformation feature
	switch v := node.(type) {
	case *ast.CreateDatabaseStmt:
		return t.handleCreateDatabase(v)
	case *ast.DropDatabaseStmt:
		return t.handleDropDatabase(v)
	case *ast.CreateTableStmt:
		return t.handleCreateTable(schema, v)
	case *ast.DropTableStmt:
		return t.handleDropTable(schema, v)
	case *ast.AlterTableStmt:
		return t.handleAlterTable(schema, v)
	case *ast.RenameTableStmt:
		return t.handleRenameTable(schema, v)
	case *ast.TruncateTableStmt:
		return t.handleTruncateTable(schema, v)
	default:
		sqls = append(sqls, statement)
	}

	return sqls, nil
}

func (t *tidbParser) handleCreateDatabase(node *ast.CreateDatabaseStmt) ([]string, error) {
	renamedSchema, _, ignore, err := t.filterAndRenameDDL(node.Name, node.Name, "", filter.CreateDatabase)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if ignore {
		return nil, nil
	}

	return []string{fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s`;", renamedSchema)}, nil
}

func (t *tidbParser) handleDropDatabase(node *ast.DropDatabaseStmt) ([]string, error) {
	renamedSchema, _, ignore, err := t.filterAndRenameDDL(node.Name, node.Name, "", filter.CreateDatabase)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if ignore {
		return nil, nil
	}

	return []string{fmt.Sprintf("DROP DATABASE IF EXISTS `%s`;", renamedSchema)}, nil
}

func (t *tidbParser) handleCreateTable(schema string, node *ast.CreateTableStmt) ([]string, error) {
	schemaName, tableName := node.Table.Schema.O, node.Table.Name.O
	renamedSchema, renamedTable, ignore, err := t.filterAndRenameDDL(schema, schemaName, tableName, filter.CreateDatabase)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if ignore {
		return nil, nil
	}

	if node.ReferTable != nil {
		referSchemaName, referTableName := node.ReferTable.Schema.O, node.ReferTable.Name.O
		renamedReferSchema, renamedReferTable, ignore, err := t.filterAndRenameDDL(schema, referSchemaName, referTableName, filter.CreateDatabase)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if ignore {
			return nil, nil
		}

		return []string{fmt.Sprintf("CREATE TABLE IF NOT EXISTS `%s`.`%s` LIKE `%s`.`%s`", renamedSchema, renamedTable, renamedReferSchema, renamedReferTable)}, nil
	}

	colStrs := make([]string, 0, len(node.Cols))
	for _, col := range node.Cols {
		colStr, err := t.anaLyzeColumnDefinition(col)
		if err != nil {
			return nil, errors.Trace(err)
		}

		colStrs = append(colStrs, colStr)
	}

	tableOptionStr := ""
	return []string{fmt.Sprintf("CREATE TABLE IF NOT EXISTS `%s`.`%s` (%s) %s", renamedSchema, renamedTable, strings.Join(colStrs, ","), tableOptionStr)}, nil
}

func (t *tidbParser) handleDropTable(schema string, node *ast.DropTableStmt) ([]string, error) {
	sqls := make([]string, 0, len(node.Tables))
	for _, table := range node.Tables {
		renamedSchema, renamedTable, ignore, err := t.filterAndRenameDDL(schema, table.Schema.O, table.Name.O, filter.DropTable)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if ignore {
			continue
		}

		sqls = append(sqls, fmt.Sprintf("DROP TABLE IF EXISTS `%s`.`%s`", renamedSchema, renamedTable))
	}

	return sqls, nil
}

func (t *tidbParser) handleRenameTable(schema string, node *ast.RenameTableStmt) ([]string, error) {
	sqls := make([]string, 0, len(node.TableToTables))
	for _, table := range node.TableToTables {
		sql, err := t.makeRenameTable(schema, table.OldTable.Schema.O, table.OldTable.Name.O, table.NewTable.Schema.O, table.NewTable.Name.O)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if len(sql) == 0 {
			continue
		}

		sqls = append(sqls, sql)
	}

	return sqls, nil
}

func (t *tidbParser) handleTruncateTable(schema string, node *ast.TruncateTableStmt) ([]string, error) {
	renamedSchema, renamedTable, ignore, err := t.filterAndRenameDDL(schema, node.Table.Schema.O, node.Table.Name.O, filter.TruncateTable)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if ignore {
		return nil, nil
	}

	return []string{fmt.Sprintf("TRUNCATE TABLE `%s`.`%s`", renamedSchema, renamedTable)}, nil
}

func (t *tidbParser) handleAlterTable(schema string, node *ast.AlterTableStmt) ([]string, error) {
	// implement it later
	renamedSchema, renamedTable, ignore, err := t.filterAndRenameDDL(schema, node.Table.Schema.O, node.Table.Name.O, filter.RenameTable)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if ignore {
		return nil, nil
	}

	var (
		sqls = make([]string, 0, len(node.Specs))
		sql  string
	)

	for _, spec := range node.Specs {
		if spec.Tp == ast.AlterTableRenameTable {
			sql, err = t.makeRenameTable(schema, node.Table.Schema.O, node.Table.Name.O, spec.NewTable.Schema.O, spec.NewTable.Name.O)
		} else {
			sql, err = t.analyzeAlterSpec(spec)
		}

		if err != nil {
			return nil, errors.Trace(err)
		}
		if len(sql) == 0 {
			continue
		}

		sqls = append(sqls, fmt.Sprintf("ALTER TABLE `%s`.`%s` %s", renamedSchema, renamedTable, sql))
	}

	return sqls, nil
}

func (t *tidbParser) analyzeAlterSpec(spec *ast.AlterTableSpec) (string, error) {
	return "", nil
}

func (t *tidbParser) makeRenameTable(schemaInContext, oldSchema, oldTable, renameSchema, renameTable string) (string, error) {
	renamedOldSchema, renamedOldTable, ignore, err := t.filterAndRenameDDL(schemaInContext, oldSchema, oldTable, filter.RenameTable)
	if err != nil {
		return "", errors.Trace(err)
	}
	if ignore {
		return "", nil
	}

	renameSchema, renamedTable, ignore, err := t.filterAndRenameDDL(schemaInContext, renamedOldTable, renameTable, filter.RenameTable)
	if err != nil {
		return "", errors.Trace(err)
	}
	if ignore {
		return "", nil
	}

	return fmt.Sprintf("RENAME TABLE `%s`.`%s` TO `%s`.`5s`", renamedOldSchema, renamedOldTable, renameSchema, renamedTable), nil
}

func (t *tidbParser) filterAndRenameDDL(schemaInContext, schema, table string, event filter.EventType) (string, string, bool, error) {
	if schema == "" {
		schema = schemaInContext
	}

	action, err := t.filter.Filter(schema, table, filter.NullEvent, event, "")
	if err != nil {
		return "", "", false, errors.Annotatef(err, "filter event on %s/%s", event, schema, table)
	}
	if action == filter.Ignore {
		return "", "", true, nil
	}

	renamedSchema, renamedTable, err := t.router.Route(schema, table)
	return renamedSchema, renamedTable, false, errors.Annotatef(err, "router schema %s/%s", schema, table)
}

func (t *tidbParser) anaLyzeColumnDefinition(col *ast.ColumnDef) (string, error) {
	// implement it later
	return "", nil
}
