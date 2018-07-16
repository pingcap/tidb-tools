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
	"github.com/pingcap/tidb/ast"
	"github.com/pkg/errors"
)

// TableName  represents table name like `schema`.`table`
type TableName struct {
	Schema string `json:"schema" toml:"schema"`
	Table  string `json:"table" toml:"table"`
}

// ExtractTableName return result that contains [tableName] excepted create table like and rename table
// for `create table like` DDL, result contains [sourceTableName, sourceRefTableName]
// for rename table ddl, result contains [targetOldTableName, sourceNewTableName]
func ExtractTableName(node ast.StmtNode) ([]TableName, error) {
	res := make([]TableName, 0, 1)
	switch v := node.(type) {
	case *ast.CreateDatabaseStmt:
		res = append(res, TableName{v.Name, ""})
	case *ast.DropDatabaseStmt:
		res = append(res, TableName{v.Name, ""})
	case *ast.CreateTableStmt:
		res = append(res, TableName{v.Table.Schema.L, v.Table.Name.L})
		if v.ReferTable != nil {
			res = append(res, TableName{v.ReferTable.Schema.L, v.ReferTable.Name.L})
		}
	case *ast.DropTableStmt:
		if len(v.Tables) != 1 {
			return nil, errors.Errorf("don't support to drop table with multiple tables")
		}
		res = append(res, TableName{v.Tables[0].Schema.L, v.Tables[0].Name.L})
	case *ast.TruncateTableStmt:
		res = append(res, TableName{v.Table.Schema.L, v.Table.Name.L})
	case *ast.AlterTableStmt:
		res = append(res, TableName{v.Table.Schema.L, v.Table.Name.L})
		if v.Specs[0].NewTable != nil {
			res = append(res, TableName{v.Specs[0].NewTable.Schema.L, v.Specs[0].NewTable.Name.L})
		}
	case *ast.RenameTableStmt:
		res = append(res, TableName{v.OldTable.Schema.L, v.OldTable.Name.L})
		res = append(res, TableName{v.NewTable.Schema.L, v.NewTable.Name.L})
	case *ast.CreateIndexStmt:
		res = append(res, TableName{v.Table.Schema.L, v.Table.Name.L})
	case *ast.DropIndexStmt:
		res = append(res, TableName{v.Table.Schema.L, v.Table.Name.L})
	default:
		return nil, errors.Errorf("unkown type DDL stmtNode(%T) %+v", node, node)
	}

	return res, nil
}
