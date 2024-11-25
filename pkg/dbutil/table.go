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
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/coreos/go-semver/semver"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	_ "github.com/pingcap/tidb/pkg/planner/core" // to setup expression.EvalAstExpr. See: https://github.com/pingcap/tidb/blob/a94cff903cd1e7f3b050db782da84273ef5592f4/planner/core/optimizer.go#L202
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/types"
	_ "github.com/pingcap/tidb/pkg/types/parser_driver" // for parser driver
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/mock"
)

const (
	AnnotationClusteredReplaceString    = "${1} /*T![clustered_index] CLUSTERED */${2}\n"
	AnnotationNonClusteredReplaceString = "${1} /*T![clustered_index] NONCLUSTERED */${2}\n"
)

func init() {
	collate.SetNewCollationEnabledForTest(false)
}

// addClusteredAnnotation add the `/*T![clustered_index] NONCLUSTERED */` for primary key of create table info
// In the older version, the create table info hasn't `/*T![clustered_index] NONCLUSTERED */`,
// which lead the issue https://github.com/pingcap/tidb-tools/issues/678
//
// Before Get Create Table Info:
// mysql> SHOW CREATE TABLE `test`.`itest`;
//
//	+-------+--------------------------------------------------------------------+
//	| Table | Create Table                                                                                                                              |
//	+-------+--------------------------------------------------------------------+
//	| itest | CREATE TABLE `itest` (
//		`id` int(11) DEFAULT NULL,
//		`name` varchar(24) DEFAULT NULL,
//		PRIMARY KEY (`id`)
//		) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin |
//	+-------+--------------------------------------------------------------------+
//
// After Add the annotation:
//
//	+-------+--------------------------------------------------------------------+
//	| Table | Create Table                                                                                                                              |
//	+-------+--------------------------------------------------------------------+
//	| itest | CREATE TABLE `itest` (
//		`id` int(11) DEFAULT NULL,
//		`name` varchar(24) DEFAULT NULL,
//		PRIMARY KEY (`id`) /*T![clustered_index] CLUSTERED */
//		) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin |
//	+-------+--------------------------------------------------------------------+
func addClusteredAnnotationForPrimaryKey(raw string, replace string) (string, error) {
	reg, regErr := regexp.Compile(`(PRIMARY\sKEY.*\))(\s*,?)\s*\n`)
	if reg == nil || regErr != nil {
		return raw, errors.Annotate(regErr, "failed to compile regex for add clustered annotation, err: %s")
	}
	return reg.ReplaceAllString(raw, replace), nil
}

func isPKISHandle(ctx context.Context, db QueryExecutor, schemaName string, tableName string) bool {
	query := fmt.Sprintf("SELECT _tidb_rowid FROM %s LIMIT 0;", TableName(schemaName, tableName))
	rows, err := db.QueryContext(ctx, query)
	if err != nil && strings.Contains(err.Error(), "Unknown column") {
		return true
	}
	if rows != nil {
		rows.Close()
	}
	return false
}

func GetTableInfoWithVersion(ctx context.Context, db QueryExecutor, schemaName string, tableName string, version *semver.Version) (*model.TableInfo, error) {
	createTableSQL, err := GetCreateTableSQL(ctx, db, schemaName, tableName)
	if err != nil {
		return nil, errors.Trace(err)
	}

	if version != nil && version.Major <= 4 {
		var replaceString string
		if isPKISHandle(ctx, db, schemaName, tableName) {
			replaceString = AnnotationClusteredReplaceString
		} else {
			replaceString = AnnotationNonClusteredReplaceString
		}
		createTableSQL, err = addClusteredAnnotationForPrimaryKey(createTableSQL, replaceString)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	parser2, err := GetParserForDB(ctx, db)
	if err != nil {
		return nil, errors.Trace(err)
	}
	sctx := mock.NewContext()
	// unify the timezone to UTC +0:00
	sctx.GetSessionVars().TimeZone = time.UTC
	sctx.GetSessionVars().SQLMode = mysql.DelSQLMode(sctx.GetSessionVars().SQLMode, mysql.ModeStrictTransTables)
	sctx.GetSessionVars().SQLMode = mysql.DelSQLMode(sctx.GetSessionVars().SQLMode, mysql.ModeStrictAllTables)
	return GetTableInfoBySQLWithSessionContext(sctx, createTableSQL, parser2)
}

// GetTableInfo returns table information.
func GetTableInfo(ctx context.Context, db QueryExecutor, schemaName string, tableName string) (*model.TableInfo, error) {
	createTableSQL, err := GetCreateTableSQL(ctx, db, schemaName, tableName)
	if err != nil {
		return nil, errors.Trace(err)
	}

	parser2, err := GetParserForDB(ctx, db)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return GetTableInfoBySQL(createTableSQL, parser2)
}

// GetTableInfoBySQL returns table information by given create table sql.
func GetTableInfoBySQLWithSessionContext(ctx sessionctx.Context, createTableSQL string, parser2 *parser.Parser) (table *model.TableInfo, err error) {
	return getTableInfoBySQL(ctx, createTableSQL, parser2)
}

// GetTableInfoBySQL returns table information by given create table sql.
func GetTableInfoBySQL(createTableSQL string, parser2 *parser.Parser) (table *model.TableInfo, err error) {
	return getTableInfoBySQL(mock.NewContext(), createTableSQL, parser2)
}

func getTableInfoBySQL(ctx sessionctx.Context, createTableSQL string, parser2 *parser.Parser) (table *model.TableInfo, err error) {
	stmt, err := parser2.ParseOneStmt(createTableSQL, "", "")
	if err != nil {
		return nil, errors.Trace(err)
	}

	metaBuildCtx := ddl.NewMetaBuildContextWithSctx(ctx)
	s, ok := stmt.(*ast.CreateTableStmt)
	if ok {
		table, err := ddl.BuildTableInfoWithStmt(metaBuildCtx, s, mysql.DefaultCharset, "", nil)
		if err != nil {
			return nil, errors.Trace(err)
		}

		// put primary key in indices
		if table.PKIsHandle {
			pkIndex := &model.IndexInfo{
				Name:    pmodel.NewCIStr("PRIMARY"),
				Primary: true,
				State:   model.StatePublic,
				Unique:  true,
				Tp:      pmodel.IndexTypeBtree,
				Columns: []*model.IndexColumn{
					{
						Name:   table.GetPkName(),
						Length: types.UnspecifiedLength,
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
func EqualTableInfo(tableInfo1, tableInfo2 *model.TableInfo) (bool, string) {
	// check columns
	if len(tableInfo1.Columns) != len(tableInfo2.Columns) {
		return false, fmt.Sprintf("column num not equal, one is %d another is %d", len(tableInfo1.Columns), len(tableInfo2.Columns))
	}

	for j, col := range tableInfo1.Columns {
		if col.Name.O != tableInfo2.Columns[j].Name.O {
			return false, fmt.Sprintf("column name not equal, one is %s another is %s", col.Name.O, tableInfo2.Columns[j].Name.O)
		}
		if col.GetType() != tableInfo2.Columns[j].GetType() {
			return false, fmt.Sprintf("column %s's type not equal, one is %v another is %v", col.Name.O, col.GetType(), tableInfo2.Columns[j].GetType())
		}
	}

	// check index
	if len(tableInfo1.Indices) != len(tableInfo2.Indices) {
		return false, fmt.Sprintf("index num not equal, one is %d another is %d", len(tableInfo1.Indices), len(tableInfo2.Indices))
	}

	index2Map := make(map[string]*model.IndexInfo)
	for _, index := range tableInfo2.Indices {
		index2Map[index.Name.O] = index
	}

	for _, index1 := range tableInfo1.Indices {
		index2, ok := index2Map[index1.Name.O]
		if !ok {
			return false, fmt.Sprintf("index %s not exists", index1.Name.O)
		}

		if len(index1.Columns) != len(index2.Columns) {
			return false, fmt.Sprintf("index %s's columns num not equal, one is %d another is %d", index1.Name.O, len(index1.Columns), len(index2.Columns))
		}
		for j, col := range index1.Columns {
			if col.Name.O != index2.Columns[j].Name.O {
				return false, fmt.Sprintf("index %s's column not equal, one has %s another has %s", index1.Name.O, col.Name.O, index2.Columns[j].Name.O)
			}
		}
	}

	return true, ""
}
