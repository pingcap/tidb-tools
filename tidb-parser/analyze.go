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
	"regexp"
	"strings"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/ast"
)

// handle create table temporarily
var createTableRegex = regexp.MustCompile("(?i)^CREATE\\s+(TEMPORARY\\s+)?TABLE\\s+(IF NOT EXISTS\\s+)?\\S+")

// AnalyzeASTNode analyzes and returns text queries
func AnalyzeASTNode(node ast.StmtNode, statement string) (string, error) {
	// poor filter/transformation implemention
	switch v := node.(type) {
	case *ast.CreateDatabaseStmt:
		return AnalyzeCreateDatabase(v)
	case *ast.DropDatabaseStmt:
		return AnalyzeDropDatabase(v)
	case *ast.CreateTableStmt:
		// handle create table temporarily
		return AnalyzeCreateTable(v, statement)
	case *ast.DropTableStmt:
		return AnalyzeDropTable(v)
	case *ast.AlterTableStmt:
		return AnalyzeAlterTable(v)
	case *ast.RenameTableStmt:
		return AnalyzeRenameTable(v)
	case *ast.TruncateTableStmt:
		return AnalyzeTruncateTable(v)
	case *ast.CreateIndexStmt:
		return AnalyzeCreateIndex(v)
	case *ast.DropIndexStmt:
		return AnalyzeDropIndex(v)
	}

	return "", errors.NotSupportedf("DDL %+v(%T)", node, node)
}

// AnalyzeCreateDatabase returns create database query text
func AnalyzeCreateDatabase(node *ast.CreateDatabaseStmt) (string, error) {
	return fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s`;", escapeName(node.Name)), nil
}

// AnalyzeDropDatabase returns drop database query text
func AnalyzeDropDatabase(node *ast.DropDatabaseStmt) (string, error) {
	return fmt.Sprintf("DROP DATABASE IF EXISTS `%s`;", escapeName(node.Name)), nil
}

// AnalyzeDropTable returns drop table query text
func AnalyzeDropTable(node *ast.DropTableStmt) (string, error) {
	tableNames := make([]string, 0, len(node.Tables))
	for _, table := range node.Tables {
		tableNames = append(tableNames, TableName(table.Schema.O, table.Name.O))
	}

	return fmt.Sprintf("DROP TABLE IF EXISTS %s", strings.Join(tableNames, ",")), nil
}

// AnalyzeTruncateTable returns truncate table query text
func AnalyzeTruncateTable(node *ast.TruncateTableStmt) (string, error) {
	return fmt.Sprintf("TRUNCATE TABLE %s", TableName(node.Table.Schema.O, node.Table.Name.O)), nil
}

// AnalyzeRenameTable returns rename table query text
func AnalyzeRenameTable(node *ast.RenameTableStmt) (string, error) {
	t2ts := make([]string, 0, len(node.TableToTables))
	for _, t := range node.TableToTables {
		t2ts = append(t2ts, fmt.Sprintf("%s TO %s", TableName(t.OldTable.Schema.O, t.OldTable.Name.O), TableName(t.NewTable.Schema.O, t.NewTable.Name.O)))
	}

	return fmt.Sprintf("RENAME TABLE %s", strings.Join(t2ts, ",")), nil
}

// AnalyzeCreateIndex returns create index query text
func AnalyzeCreateIndex(node *ast.CreateIndexStmt) (string, error) {
	uniqueStr := ""
	if node.Unique {
		uniqueStr = "UNIQUE"
	}

	indexOpStr := AnalyzeIndexOption(node.IndexOption)
	return fmt.Sprintf("CREATE %s INDEX `%s` ON %s (%s) %s", uniqueStr, escapeName(node.IndexName), TableName(node.Table.Schema.O, node.Table.Name.O), analyzeIndexColNames(node.IndexColNames), indexOpStr), nil
}

// AnalyzeDropIndex returns drop index query text
func AnalyzeDropIndex(node *ast.DropIndexStmt) (string, error) {
	return fmt.Sprintf("DROP INDEX IF EXISTS %s ON %s", escapeName(node.IndexName), TableName(node.Table.Schema.O, node.Table.Name.O)), nil
}

// AnalyzeCreateTable returns create table query text
func AnalyzeCreateTable(node *ast.CreateTableStmt, statement string) (string, error) {
	if node.ReferTable != nil {
		return fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s LIKE %s", TableName(node.Table.Schema.O, node.Table.Name.O), TableName(node.ReferTable.Schema.O, node.ReferTable.Name.O)), nil
	}

	// handle create table temporarily
	sqlPrefix := createTableRegex.FindString(statement)
	index := findLastWord(sqlPrefix)
	endChars := findTableDefineIndex(sqlPrefix[index:])
	return createTableRegex.ReplaceAllString(statement, fmt.Sprintf("%s%s%s", sqlPrefix[:index], TableName(node.Table.Schema.O, node.Table.Name.O), endChars)), nil
}

// AnalyzeAlterTable returns alter table query text
func AnalyzeAlterTable(node *ast.AlterTableStmt) (string, error) {
	var (
		specStrs    = make([]string, 0, len(node.Specs))
		alterPrefix = fmt.Sprintf("ALTER TABLE %s ", TableName(node.Table.Schema.O, node.Table.Name.O))
	)
	for _, spec := range node.Specs {
		specStr, err := AnalyzeAlterTableSpec(spec)
		if err != nil {
			return "", errors.Trace(err)
		}
		if len(specStr) == 0 {
			continue
		}

		specStrs = append(specStrs, specStr)
	}

	return fmt.Sprintf("%s %s", alterPrefix, strings.Join(specStrs, ",")), nil
}

// AnalyzeIndexOption returns index option text
func AnalyzeIndexOption(option *ast.IndexOption) string {
	tp := option.Tp.String()
	if len(tp) > 0 {
		tp = fmt.Sprintf("USING %s", tp)
	}

	if len(option.Comment) > 0 {
		return fmt.Sprintf("%s COMMENT \"%s\"", tp, option.Comment)
	}

	return tp
}
