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
	"strings"

	filter "github.com/pingcap/tidb-tools/pkg/binlog-filter"
	"github.com/pingcap/tidb-tools/pkg/table-router"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/model"
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

	// parse statement, if it doesn't contain any DDL statements, ignore it
	nodes, err := t.Parse(statement, "", "")
	if err != nil {
		return nil, false, errors.Annotatef(err, "parser ddl %s", statement)
	}
	if len(nodes) == 0 { // ignore it
		return nil, true, nil
	}

	var sqls []string
	for _, node := range nodes {
		_, isDDL := node.(ast.DDLNode)
		if !isDDL {
			continue
		}

		// filter and transform statement
		subSQLs, err := t.handleDDL(schema, statement, node)
		if err != nil {
			return nil, false, errors.Annotatef(err, "handle ddl %s", statement)
		}
		sqls = append(sqls, subSQLs...)
	}

	return sqls, false, nil
}

func (t *tidbParser) handleDDL(schema string, statement string, node ast.StmtNode) ([]string, error) {
	var (
		modifiedNodes []ast.StmtNode
		err           error
	)
	// poor filter/transformation implemention
	// unified return ast.StmtNode array to spit some DDL
	switch v := node.(type) {
	case *ast.CreateDatabaseStmt:
		modifiedNodes, err = t.handleCreateDatabase(v)
	case *ast.DropDatabaseStmt:
		modifiedNodes, err = t.handleDropDatabase(v)
	case *ast.CreateTableStmt:
		modifiedNodes, err = t.handleCreateTable(schema, v)
	case *ast.DropTableStmt:
		modifiedNodes, err = t.handleDropTable(schema, v)
	case *ast.AlterTableStmt:
		modifiedNodes, err = t.handleAlterTable(schema, v)
	case *ast.RenameTableStmt:
		modifiedNodes, err = t.handleRenameTable(schema, v)
	case *ast.TruncateTableStmt:
		modifiedNodes, err = t.handleTruncateTable(schema, v)
	case *ast.CreateIndexStmt:
		modifiedNodes, err = t.handleCreateIndex(schema, v)
	case *ast.DropIndexStmt:
		modifiedNodes, err = t.handleDropIndex(schema, v)
	default:
		return nil, errors.NotSupportedf("DDL %+v(%T)", node, node)
	}
	if err != nil {
		return nil, errors.Trace(err)
	}

	// analyze queries from ast nodes
	sqls := make([]string, 0, len(modifiedNodes))
	for _, modifiedNode := range modifiedNodes {
		modifiedSQL, err := AnalyzeASTNode(modifiedNode, statement)
		if err != nil {
			return nil, errors.Trace(err)
		}

		sqls = append(sqls, modifiedSQL)
	}

	return sqls, nil
}

func (t *tidbParser) handleCreateDatabase(node *ast.CreateDatabaseStmt) ([]ast.StmtNode, error) {
	renamedSchema, _, ignore, err := t.filterAndRenameDDL(node.Name, node.Name, "", filter.CreateDatabase)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if ignore {
		return nil, nil
	}
	node.Name = renamedSchema

	return []ast.StmtNode{node}, nil
}

func (t *tidbParser) handleDropDatabase(node *ast.DropDatabaseStmt) ([]ast.StmtNode, error) {
	renamedSchema, _, ignore, err := t.filterAndRenameDDL(node.Name, node.Name, "", filter.DropDatabase)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if ignore {
		return nil, nil
	}
	node.Name = renamedSchema

	return []ast.StmtNode{node}, nil
}

func (t *tidbParser) handleCreateTable(schema string, node *ast.CreateTableStmt) ([]ast.StmtNode, error) {
	schemaName, tableName := node.Table.Schema.O, node.Table.Name.O
	renamedSchema, renamedTable, ignore, err := t.filterAndRenameDDL(schema, schemaName, tableName, filter.CreateTable)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if ignore {
		return nil, nil
	}
	node.Table.Schema = model.CIStr{renamedSchema, strings.ToLower(renamedSchema)}
	node.Table.Name = model.CIStr{renamedTable, strings.ToLower(renamedTable)}

	if node.ReferTable != nil {
		referSchemaName, referTableName := node.ReferTable.Schema.O, node.ReferTable.Name.O
		renamedReferSchema, renamedReferTable, ignore, err := t.filterAndRenameDDL(schema, referSchemaName, referTableName, filter.CreateTable)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if ignore {
			return nil, nil
		}
		node.ReferTable.Schema = model.CIStr{renamedReferSchema, strings.ToLower(renamedReferSchema)}
		node.ReferTable.Name = model.CIStr{renamedReferTable, strings.ToLower(renamedReferTable)}
	}

	return []ast.StmtNode{node}, nil
}

func (t *tidbParser) handleDropTable(schema string, node *ast.DropTableStmt) ([]ast.StmtNode, error) {
	for _, table := range node.Tables {
		renamedSchema, renamedTable, ignore, err := t.filterAndRenameDDL(schema, table.Schema.O, table.Name.O, filter.DropTable)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if ignore {
			continue
		}
		table.Schema = model.CIStr{renamedSchema, strings.ToLower(renamedSchema)}
		table.Name = model.CIStr{renamedTable, strings.ToLower(renamedTable)}
	}

	return []ast.StmtNode{node}, nil
}

func (t *tidbParser) handleTruncateTable(schema string, node *ast.TruncateTableStmt) ([]ast.StmtNode, error) {
	renamedSchema, renamedTable, ignore, err := t.filterAndRenameDDL(schema, node.Table.Schema.O, node.Table.Name.O, filter.TruncateTable)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if ignore {
		return nil, nil
	}
	node.Table.Schema = model.CIStr{renamedSchema, strings.ToLower(renamedSchema)}
	node.Table.Name = model.CIStr{renamedTable, strings.ToLower(renamedTable)}

	return []ast.StmtNode{node}, nil
}

// now we only support syntax `RENAME TABLE tbl_name TO new_tbl_name`
func (t *tidbParser) handleRenameTable(schema string, node *ast.RenameTableStmt) ([]ast.StmtNode, error) {
	nodes := make([]ast.StmtNode, 0, len(node.TableToTables))
	for _, table := range node.TableToTables {
		renamedOldSchema, renamedOldTable, ignore, err := t.filterAndRenameDDL(schema, table.OldTable.Schema.O, table.OldTable.Name.O, filter.RenameTable)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if ignore {
			return nil, nil
		}
		table.OldTable.Schema = model.CIStr{renamedOldSchema, strings.ToLower(renamedOldSchema)}
		table.OldTable.Name = model.CIStr{renamedOldTable, strings.ToLower(renamedOldTable)}

		renamedNewSchema, renamedNewTable, ignore, err := t.filterAndRenameDDL(schema, table.NewTable.Schema.O, table.NewTable.Name.O, filter.RenameTable)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if ignore {
			return nil, nil
		}
		table.NewTable.Schema = model.CIStr{renamedNewSchema, strings.ToLower(renamedNewSchema)}
		table.NewTable.Name = model.CIStr{renamedNewTable, strings.ToLower(renamedNewTable)}

		nodes = append(nodes, &ast.RenameTableStmt{
			OldTable:      table.OldTable,
			NewTable:      table.NewTable,
			TableToTables: []*ast.TableToTable{table},
		})
	}

	return nodes, nil
}

func (t *tidbParser) handleAlterTable(schema string, node *ast.AlterTableStmt) ([]ast.StmtNode, error) {
	// implement it later
	renamedSchema, renamedTable, ignore, err := t.filterAndRenameDDL(schema, node.Table.Schema.O, node.Table.Name.O, filter.RenameTable)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if ignore {
		return nil, nil
	}
	node.Table.Schema = model.CIStr{renamedSchema, strings.ToLower(renamedSchema)}
	node.Table.Name = model.CIStr{renamedTable, strings.ToLower(renamedTable)}

	var (
		nodes       = make([]ast.StmtNode, 0, len(node.Specs))
		renameNodes []ast.StmtNode
	)
	for _, spec := range node.Specs {
		switch spec.Tp {
		case ast.AlterTableRenameTable:
			renamedNewSchema, renamedNewTable, ignore, err := t.filterAndRenameDDL(schema, spec.NewTable.Schema.O, spec.NewTable.Name.O, filter.RenameTable)
			if err != nil {
				return nil, errors.Trace(err)
			}
			if ignore {
				return nil, nil
			}
			spec.NewTable.Schema = model.CIStr{renamedNewSchema, strings.ToLower(renamedNewSchema)}
			spec.NewTable.Name = model.CIStr{renamedNewTable, strings.ToLower(renamedNewTable)}

			renameNodes = append(renameNodes, &ast.RenameTableStmt{
				OldTable:      node.Table,
				NewTable:      spec.NewTable,
				TableToTables: []*ast.TableToTable{{OldTable: node.Table, NewTable: spec.NewTable}},
			})
		case ast.AlterTableAddColumns:
			for i := range spec.NewColumns {
				newSpec := &ast.AlterTableSpec{}
				*newSpec = *spec
				newSpec.NewColumns = spec.NewColumns[i : i+1]

				nodes = append(nodes, &ast.AlterTableStmt{
					Table: node.Table,
					Specs: []*ast.AlterTableSpec{newSpec},
				})
			}
		default:
			nodes = append(nodes, &ast.AlterTableStmt{
				Table: node.Table,
				Specs: []*ast.AlterTableSpec{spec},
			})
		}
	}

	return append(nodes, renameNodes...), nil
}

func (t *tidbParser) handleCreateIndex(schema string, node *ast.CreateIndexStmt) ([]ast.StmtNode, error) {
	renamedSchema, renamedTable, ignore, err := t.filterAndRenameDDL(schema, node.Table.Schema.O, node.Table.Name.O, filter.CreateIndex)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if ignore {
		return nil, nil
	}
	node.Table.Schema = model.CIStr{renamedSchema, strings.ToLower(renamedSchema)}
	node.Table.Name = model.CIStr{renamedTable, strings.ToLower(renamedTable)}

	return []ast.StmtNode{node}, nil
}

func (t *tidbParser) handleDropIndex(schema string, node *ast.DropIndexStmt) ([]ast.StmtNode, error) {
	renamedSchema, renamedTable, ignore, err := t.filterAndRenameDDL(schema, node.Table.Schema.O, node.Table.Name.O, filter.DropIndex)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if ignore {
		return nil, nil
	}
	node.Table.Schema = model.CIStr{renamedSchema, strings.ToLower(renamedSchema)}
	node.Table.Name = model.CIStr{renamedTable, strings.ToLower(renamedTable)}

	return []ast.StmtNode{node}, nil
}

func (t *tidbParser) filterAndRenameDDL(schemaInContext, schema, table string, event filter.EventType) (string, string, bool, error) {
	if schema == "" {
		schema = schemaInContext
	}

	if t.filter != nil {
		action, err := t.filter.Filter(schema, table, filter.NullEvent, event, "")
		if err != nil {
			return "", "", false, errors.Annotatef(err, "filter event on %s/%s", event, schema, table)
		}
		if action == filter.Ignore {
			return "", "", true, nil
		}
	}

	if t.router == nil {
		return schema, table, false, nil
	}

	renamedSchema, renamedTable, err := t.router.Route(schema, table)
	return renamedSchema, renamedTable, false, errors.Annotatef(err, "router schema %s/%s", schema, table)
}
