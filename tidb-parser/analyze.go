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
	"github.com/pingcap/tidb/types"
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
	case *ast.DropIndexStmt:
		return AnalyzeDropIndex(v)
	}

	return "", errors.NotSupportedf("DDL %+v(%T)", node, node)
}

// AnalyzeCreateDatabase returns create database query text
func AnalyzeCreateDatabase(node *ast.CreateDatabaseStmt) (string, error) {
	return fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s`;", node.Name), nil
}

// AnalyzeDropDatabase returns drop database query text
func AnalyzeDropDatabase(node *ast.DropDatabaseStmt) (string, error) {
	return fmt.Sprintf("DROP DATABASE IF EXISTS `%s`;", node.Name), nil
}

// AnalyzeDropTable returns drop table query text
func AnalyzeDropTable(node *ast.DropTableStmt) (string, error) {
	tableNames := make([]string, 0, len(node.Tables))
	for _, table := range node.Tables {
		tableNames = append(tableNames, fmt.Sprintf("`%s`.`%s`", table.Schema, table.Name))
	}

	return fmt.Sprintf("DROP TABLE IF EXISTS %s", strings.Join(tableNames, ",")), nil
}

// AnalyzeTruncateTable returns truncate table query text
func AnalyzeTruncateTable(node *ast.TruncateTableStmt) (string, error) {
	return fmt.Sprintf("TRUNCATE TABLE `%s`.`%s`", node.Table.Schema, node.Table.Name), nil
}

// AnalyzeRenameTable returns rename table query text
func AnalyzeRenameTable(node *ast.RenameTableStmt) (string, error) {
	t2ts := make([]string, 0, len(node.TableToTables))
	for _, t := range node.TableToTables {
		t2ts = append(t2ts, fmt.Sprintf("`%s`.`%s` TO `%s`.`%s`", t.OldTable.Schema, t.OldTable.Name, t.NewTable.Schema, t.NewTable.Name))
	}

	return fmt.Sprintf("RENAME TABLE %s", strings.Join(t2ts, ",")), nil
}

// AnalyzeCreateIndex returns create index query text
func AnalyzeCreateIndex(node *ast.CreateIndexStmt) (string, error) {
	uniqueStr := ""
	if node.Unique {
		uniqueStr = "UNIQUE"
	}

	indexOpStr, err := AnalyzeIndexOption(node.IndexOption)
	if err != nil {
		return "", errors.Trace(err)
	}

	indexColumnList := make([]string, 0, len(node.IndexColNames))
	for _, indexCol := range node.IndexColNames {
		length := ""
		if indexCol.Length != types.UnspecifiedLength {
			length = fmt.Sprintf("(%d)", indexCol.Length)
		}

		indexColumnList = append(indexColumnList, fmt.Sprintf("%s%s", indexCol.Column.Name.O, length))
	}

	return fmt.Sprintf("CREATE %s INDEX %s ON `%s`.`%s` (%s) %s", uniqueStr, node.IndexName, node.Table.Schema, node.Table.Name, strings.Join(indexColumnList, ","), indexOpStr), nil
}

// AnalyzeDropIndex returns drop index query text
func AnalyzeDropIndex(node *ast.DropIndexStmt) (string, error) {
	return fmt.Sprintf("DROP INDEX IF EXISTS %s ON `%s`.`%s`", node.IndexName, node.Table.Schema, node.Table.Name), nil
}

// AnalyzeCreateTable returns create table query text
func AnalyzeCreateTable(node *ast.CreateTableStmt, statement string) (string, error) {
	if node.ReferTable != nil {
		return fmt.Sprintf("CREATE TABLE IF NOT EXISTS `%s`.`%s` LIKE `%s`.`%s`", node.Table.Schema, node.Table.Name, node.ReferTable.Schema, node.ReferTable.Name), nil
	}

	sqlPrefix := createTableRegex.FindString(statement)
	index := findLastWord(sqlPrefix)
	endChars := findTableDefineIndex(sqlPrefix[index:])
	return createTableRegex.ReplaceAllString(statement, fmt.Sprintf("%s`%s`.`%s`%s", sqlPrefix[:index], node.Table.Schema, node.Table.Name, endChars)), nil
}

// AnalyzeAlterTable returns alter table query text
func AnalyzeAlterTable(node *ast.AlterTableStmt) (string, error) {
	var (
		defStrs = make([]string, 0, len(node.Specs))
		prefix  = fmt.Sprintf("ALTER TABLE %s ", tableNameToSQL(node.Table))
	)
	for _, spec := range node.Specs {
		defStr := alterTableSpecToSQL(spec)
		if len(defStr) == 0 {
			continue
		}

		defStrs = append(defStrs, defStr)
	}
	query := fmt.Sprintf("%s %s", prefix, strings.Join(defStrs, ","))

	return query, nil
}

// AnalyzeIndexOption returns index option text
func AnalyzeIndexOption(option *ast.IndexOption) (string, error) {
	tp := option.Tp.String()
	if len(tp) > 0 {
		tp = fmt.Sprintf("USING %s", tp)
	}

	if len(option.Comment) > 0 {
		return fmt.Sprintf("%s COMMENT %s", tp, option.Comment), nil
	}

	return tp, nil
}

// AnalyzeTableOption returns table option text
func AnalyzeTableOption(option *ast.TableOption) (string, error) {
	switch option.Tp {
	case ast.TableOptionEngine:
		if option.StrValue == "" {
			return fmt.Sprintf(" ENGINE = ''"), nil
		}
		return fmt.Sprintf(" ENGINE = %s", option.StrValue), nil
	case ast.TableOptionCollate:
		return fmt.Sprintf("DEAULT COLLATE %s", option.StrValue), nil
	case ast.TableOptionCharset:
		return fmt.Sprintf("DEFAULT CHARACTER SET %s", option.StrValue), nil
	case ast.TableOptionAutoIncrement:
		return fmt.Sprintf("AUTO_INCREMET=%d", option.UintValue), nil
	case ast.TableOptionComment:
		return fmt.Sprintf("COMMENT=%s", option.StrValue), nil
	case ast.TableOptionAvgRowLength:
		return fmt.Sprintf("AVG_ROW_LENGTH=%d", option.UintValue), nil
	case ast.TableOptionConnection:
		return fmt.Sprintf("CONNECTION=%s", option.StrValue), nil
	case ast.TableOptionCheckSum:
		return fmt.Sprintf("CHECKSUM=%d", option.UintValue), nil
	case ast.TableOptionPassword:
		return fmt.Sprintf("PASSWORD=%s", option.StrValue), nil
	case ast.TableOptionCompression:
		return fmt.Sprintf("COMPRESSION=%s", option.StrValue), nil
	case ast.TableOptionKeyBlockSize:
		return fmt.Sprintf("KEY_BLOCK_SIZE=%d", option.UintValue), nil
	case ast.TableOptionMaxRows:
		return fmt.Sprintf("MAX_ROWS=%d", option.UintValue), nil
	case ast.TableOptionMinRows:
		return fmt.Sprintf("MIN_ROWS=%d", option.UintValue), nil
	case ast.TableOptionDelayKeyWrite:
		return fmt.Sprintf("DELAY_KEY_WRITE=%d", option.UintValue), nil
	case ast.TableOptionRowFormat:
		return fmt.Sprintf("%s", TableRowFormat[option.UintValue]), nil
	case ast.TableOptionStatsPersistent:
		return fmt.Sprintf("STATS_PERSISTENT=DEFAULT"), nil
	case ast.TableOptionShardRowID:
		return fmt.Sprintf("SHARD_ROW_ID_BITS=%d", option.UintValue), nil
	}

	return "", nil
}
