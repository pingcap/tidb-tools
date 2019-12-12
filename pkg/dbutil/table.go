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

package dbutil

import (
	"context"
	"database/sql"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/ddl"
	_ "github.com/pingcap/tidb/planner/core"        // to setup expression.EvalAstExpr. See: https://github.com/pingcap/tidb/blob/a94cff903cd1e7f3b050db782da84273ef5592f4/planner/core/optimizer.go#L202
	_ "github.com/pingcap/tidb/types/parser_driver" // for parser driver
)

// GetTableInfo returns table information.
func GetTableInfo(ctx context.Context, db *sql.DB, schemaName string, tableName string, sqlMode string) (*model.TableInfo, error) {
	createTableSQL, err := GetCreateTableSQL(ctx, db, schemaName, tableName)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return GetTableInfoBySQL(createTableSQL, sqlMode)
}

// GetTableInfoBySQL returns table information by given create table sql.
func GetTableInfoBySQL(createTableSQL string, sqlMode string) (table *model.TableInfo, err error) {
	parser2, err := GetParser(sqlMode)
	if err != nil {
		return nil, errors.Trace(err)
	}

	stmt, err := parser2.ParseOneStmt(createTableSQL, "", "")
	if err != nil {
		return nil, errors.Trace(err)
	}

	s, ok := stmt.(*ast.CreateTableStmt)
	if ok {
		table, err := ddl.BuildTableInfoFromAST(s)
		if err != nil {
			return nil, errors.Trace(err)
		}

		// put primary key in indices
		if table.PKIsHandle {
			pkIndex := &model.IndexInfo{
				Name:    model.NewCIStr("PRIMARY"),
				Primary: true,
				State:   model.StatePublic,
				Unique:  true,
				Columns: []*model.IndexColumn{
					{
						Name: table.GetPkName(),
					},
				},
			}

			table.Indices = append(table.Indices, pkIndex)
		}

		return table, nil
	}

	return nil, errors.Errorf("get table info from sql %s failed!", createTableSQL)
}

// FindColumnByName finds column by name.
func FindColumnByName(cols []*model.ColumnInfo, name string) *model.ColumnInfo {
	// column name don't distinguish capital and small letter
	name = strings.ToLower(name)
	for _, col := range cols {
		if col.Name.L == name {
			return col
		}
	}

	return nil
}

// EqualTableInfo returns true if this two table info have same columns and indices
func EqualTableInfo(tableInfo1, tableInfo2 *model.TableInfo) bool {
	// check columns
	if len(tableInfo1.Columns) != len(tableInfo2.Columns) {
		return false
	}

	for j, col := range tableInfo1.Columns {
		if col.Name.O != tableInfo2.Columns[j].Name.O {
			return false
		}
		if col.Tp != tableInfo2.Columns[j].Tp {
			return false
		}
	}

	// check index
	if len(tableInfo1.Indices) != len(tableInfo2.Indices) {
		return false
	}

	index2Map := make(map[string]*model.IndexInfo)
	for _, index := range tableInfo2.Indices {
		index2Map[index.Name.O] = index
	}

	for _, index1 := range tableInfo1.Indices {
		index2, ok := index2Map[index1.Name.O]
		if !ok {
			return false
		}

		if len(index1.Columns) != len(index2.Columns) {
			return false
		}
		for j, col := range index1.Columns {
			if col.Name.O != index2.Columns[j].Name.O {
				return false
			}
		}
	}

	return true
}
