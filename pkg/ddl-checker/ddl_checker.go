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

package ddl_checker

import (
	"context"
	"fmt"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/util/logutil"
)

type ExecutableChecker struct {
	session session.Session
	context context.Context
	parser  *parser.Parser
}

func NewExecutableChecker() (*ExecutableChecker, error) {
	logutil.InitLogger(&logutil.LogConfig{
		Level: "error",
	})
	mocktikv, err := mockstore.NewMockTikvStore()
	if err != nil {
		return nil, errors.Trace(err)
	}
	_, err = session.BootstrapSession(mocktikv)
	if err != nil {
		return nil, errors.Trace(err)
	}
	session, err := session.CreateSession4Test(mocktikv)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &ExecutableChecker{
		session: session,
		context: context.Background(),
		parser:  parser.New(),
	}, nil
}

func (ec *ExecutableChecker) Execute(sql string) error {
	_, err := ec.session.Execute(ec.context, sql)
	return err
}

func (ec *ExecutableChecker) IsTableExist(tableName string) bool {
	_, err := ec.session.Execute(ec.context,
		fmt.Sprintf("select 0 from %s limit 1", tableName))
	return err == nil
}

func (ec *ExecutableChecker) Close() {
	ec.session.Close()
}

func (ec *ExecutableChecker) Parse(sql string) (stmt ast.StmtNode, err error) {
	charset, collation := ec.session.GetSessionVars().GetCharsetInfo()
	stmt, err = ec.parser.ParseOneStmt(sql, charset, collation)
	return
}

func GetTableNeededExist(stmt ast.StmtNode) []string {
	switch x := stmt.(type) {
	case *ast.TruncateTableStmt:
		return []string{x.Table.Name.String()}
	case *ast.CreateIndexStmt:
		return []string{x.Table.Name.String()}
	case *ast.DropTableStmt:
		tablesName := make([]string, len(x.Tables))
		for i, table := range x.Tables {
			tablesName[i] = table.Name.String()
		}
		return tablesName
	case *ast.DropIndexStmt:
		return []string{x.Table.Name.String()}
	case *ast.AlterTableStmt:
		return []string{x.Table.Name.String()}
	case *ast.RenameTableStmt:
		return []string{x.OldTable.Name.String()}
	default:
		return []string{}
	}
}

func GetTableNeededNonExist(stmt ast.StmtNode) []string {
	switch x := stmt.(type) {
	case *ast.CreateTableStmt:
		return []string{x.Table.Name.String()}
	case *ast.RenameTableStmt:
		return []string{x.NewTable.Name.String()}
	default:
		return []string{}
	}
}

func IsDDL(stmt ast.StmtNode) bool {
	switch stmt.(type) {
	case *ast.TruncateTableStmt, *ast.CreateDatabaseStmt,
	*ast.CreateTableStmt, *ast.CreateIndexStmt,
	*ast.DropDatabaseStmt, *ast.DropTableStmt,
	*ast.DropIndexStmt, *ast.AlterTableStmt,
	*ast.RenameTableStmt:
		return true
	default:
		return false
	}
}

