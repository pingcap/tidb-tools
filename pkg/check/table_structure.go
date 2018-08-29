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

package check

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/juju/errors"
	"github.com/pingcap/tidb-tools/pkg/dbutil"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/util/charset"
)

// AutoIncrementKeyChecking is an identification for auto increment key checking
const AutoIncrementKeyChecking = "auto-increment key checking"

// hold information of incompatibility option
type incompatibilityOption struct {
	state       State
	instruction string
	errMessage  string
}

// String returns raw text of this incompatibility option
func (o *incompatibilityOption) String() string {
	var text bytes.Buffer

	if len(o.errMessage) > 0 {
		fmt.Fprintf(&text, "information: %s\n", o.errMessage)
	}

	if len(o.instruction) > 0 {
		fmt.Fprintf(&text, "instruction: %s\n", o.instruction)
	}

	return text.String()
}

// TablesChecker checks compatibility of table structures, there are differents between MySQL and TiDB.
// In generally we need to check definitions of columns, constraints and table options.
// Because of the early TiDB engineering design, we did not have a complete list of check items, which are all based on experience now.
type TablesChecker struct {
	db     *sql.DB
	tables map[string][]string // schema => []table; if []table is empty, query tables from db
}

// NewTablesChecker returns a Checker
func NewTablesChecker(db *sql.DB, tables map[string][]string) Checker {
	return &TablesChecker{
		db:     db,
		tables: tables,
	}
}

// Check implements Checker interface
func (c *TablesChecker) Check(ctx context.Context) *Result {
	r := &Result{
		Name:  c.Name(),
		Desc:  "check compatibility of table structure",
		State: StateSuccess,
	}

	var (
		err        error
		options    = make(map[string][]*incompatibilityOption)
		statements = make(map[string]string)
	)
	for schema, tables := range c.tables {
		if len(tables) == 0 {
			tables, err = dbutil.GetTables(ctx, c.db, schema)
			if err != nil {
				markCheckError(r, err)
				return r
			}
		}

		for _, table := range tables {
			tableName := dbutil.TableName(schema, table)
			statement, err := dbutil.GetCreateTableSQL(ctx, c.db, schema, table)
			if err != nil {
				markCheckError(r, err)
				return r
			}

			options[tableName] = c.checkCreateSQL(statement)
			statements[tableName] = statement
		}
	}

	var information bytes.Buffer
	for name, opts := range options {
		if len(opts) == 0 {
			continue
		}

		var (
			errMessages     = make([]string, 0, len(opts))
			warningMessages = make([]string, 0, len(opts))
		)
		fmt.Fprintf(&information, "********** table %s **********\n", name)
		fmt.Fprintf(&information, "statement: %s\n", statements[name])

		for _, option := range opts {
			switch option.state {
			case StateWarning:
				warningMessages = append(warningMessages, option.String())
			case StateFailure:
				errMessages = append(errMessages, option.String())
			}
		}

		if len(errMessages) > 0 {
			fmt.Fprint(&information, "---------- error options ----------\n")
			fmt.Fprintf(&information, "%s\n", strings.Join(errMessages, "\n"))
		}

		if len(warningMessages) > 0 {
			fmt.Fprint(&information, "---------- warning options ----------\n")
			fmt.Fprintf(&information, "%s\n", strings.Join(warningMessages, "\n"))
		}
	}

	if len(options) > 0 {
		r.State = StateWarning
	}
	r.ErrorMsg = information.String()

	return r
}

// Name implements Checker interface
func (c *TablesChecker) Name() string {
	return "table structure compatibility check"
}

func (c *TablesChecker) checkCreateSQL(statement string) []*incompatibilityOption {
	stmt, err := parser.New().ParseOneStmt(statement, "", "")
	if err != nil {
		return []*incompatibilityOption{
			{
				state:      StateFailure,
				errMessage: err.Error(),
			},
		}
	}
	// Analyze ast
	return c.checkAST(stmt)
}

func (c *TablesChecker) checkAST(stmt ast.StmtNode) []*incompatibilityOption {
	st, ok := stmt.(*ast.CreateTableStmt)
	if !ok {
		return []*incompatibilityOption{
			{
				state:      StateFailure,
				errMessage: fmt.Sprintf("Expect CreateTableStmt but got %T", stmt),
			},
		}
	}

	var options []*incompatibilityOption
	// check columns
	for _, def := range st.Cols {
		option := c.checkColumnDef(def)
		if option != nil {
			options = append(options, option)
		}
	}
	// check constrains
	for _, cst := range st.Constraints {
		option := c.checkConstraint(cst)
		if option != nil {
			options = append(options, option)
		}
	}

	// check options
	for _, opt := range st.Options {
		option := c.checkTableOption(opt)
		if option != nil {
			options = append(options, option)
		}
	}
	return options
}

func (c *TablesChecker) checkColumnDef(def *ast.ColumnDef) *incompatibilityOption {
	return nil
}

func (c *TablesChecker) checkConstraint(cst *ast.Constraint) *incompatibilityOption {
	switch cst.Tp {
	case ast.ConstraintForeignKey:
		return &incompatibilityOption{
			state:       StateWarning,
			instruction: fmt.Sprintf("please ref document: https://github.com/pingcap/docs-cn/blob/master/sql/ddl.md"),
			errMessage:  fmt.Sprintf("Foreign Key %s is parsed but ignored by TiDB.", cst.Name),
		}
	}

	return nil
}

func (c *TablesChecker) checkTableOption(opt *ast.TableOption) *incompatibilityOption {
	switch opt.Tp {
	case ast.TableOptionCharset:
		// Check charset
		cs := strings.ToLower(opt.StrValue)
		if cs != "binary" && !charset.ValidCharsetAndCollation(cs, "") {
			return &incompatibilityOption{
				state:       StateFailure,
				instruction: fmt.Sprintf("please ref document: https://github.com/pingcap/docs-cn/blob/master/sql/character-set-support.md"),
				errMessage:  fmt.Sprintf("unsupport charset %s", opt.StrValue),
			}
		}
	}
	return nil
}

// ShardingTablesCheck checks consistency of sharding table structures
// * check whether they have same column list
// * check whether they have auto_increment key
type ShardingTablesCheck struct {
	dbs    map[string]*sql.DB
	tables map[string]map[string][]string // instance => {schema: [table1, table2, ...]}
}

// NewShardingTablesCheck returns a Checker
func NewShardingTablesCheck(dbs map[string]*sql.DB, tables map[string]map[string][]string) Checker {
	return &ShardingTablesCheck{
		dbs:    dbs,
		tables: tables,
	}
}

// Check implements Checker interface
func (c *ShardingTablesCheck) Check(ctx context.Context) *Result {
	r := &Result{
		Name:  c.Name(),
		Desc:  "check consistency of sharding table structures",
		State: StateSuccess,
	}

	var (
		hasAutoIncrementKeyTableNum int
		hasAutoIncrementKeyTables   []string

		stmtNode  *ast.CreateTableStmt
		tableName string
	)
	for instance, schemas := range c.tables {
		db, ok := c.dbs[instance]
		if !ok {
			markCheckError(r, errors.NotFoundf("client for instance %s", instance))
			return r
		}

		for schema, tables := range schemas {
			for _, table := range tables {
				statement, err := dbutil.GetCreateTableSQL(ctx, db, schema, table)
				if err != nil {
					markCheckError(r, err)
					return r
				}

				stmt, err := parser.New().ParseOneStmt(statement, "", "")
				if err != nil {
					markCheckError(r, errors.Annotatef(err, "statement %s", statement))
					return r
				}

				ctStmt, ok := stmt.(*ast.CreateTableStmt)
				if !ok {
					markCheckError(r, errors.Errorf("Expect CreateTableStmt but got %T", stmt))
					return r
				}

				if c.hashAutoIncrementKey(ctStmt) {
					hasAutoIncrementKeyTableNum++
					hasAutoIncrementKeyTables = append(hasAutoIncrementKeyTables, dbutil.TableName(schema, table))
				}

				if stmtNode == nil {
					stmtNode = ctStmt
					tableName = dbutil.TableName(schema, table)
					continue
				}

				err = c.checkConsistency(stmtNode, ctStmt, tableName, dbutil.TableName(schema, table))
				if err != nil {
					markCheckError(r, err)
					r.Instruction = "please set same table structure for sharding tables"
					return r
				}
			}
		}
	}

	if hasAutoIncrementKeyTableNum > 1 {
		r.State = StateWarning
		r.ErrorMsg = fmt.Sprintf("tables %s have auto-increment key, would conflict with each other to cause data corruption", strings.Join(hasAutoIncrementKeyTables, ","))
		r.Instruction = "please set column mapping rules for them, or handle it by yourself"
		r.Extra = AutoIncrementKeyChecking
	}

	return r
}

func (c *ShardingTablesCheck) hashAutoIncrementKey(stmt *ast.CreateTableStmt) bool {
	for _, col := range stmt.Cols {
		var (
			hasAutoIncrementOpt bool
			isUnique            bool
		)
		for _, opt := range col.Options {
			switch opt.Tp {
			case ast.ColumnOptionAutoIncrement:
				hasAutoIncrementOpt = true
			case ast.ColumnOptionPrimaryKey, ast.ColumnOptionUniqKey:
				isUnique = true
			}
		}

		if hasAutoIncrementOpt && isUnique {
			return true
		}
	}
	return false
}

type briefColumnInfo struct {
	name         string
	tp           string
	isUniqueKey  bool
	isPrimaryKey bool
}

func (c *ShardingTablesCheck) checkConsistency(self, other *ast.CreateTableStmt, selfName, otherName string) error {
	selfColumnList := getBriefColumnList(self)
	otherColumnList := getBriefColumnList(other)

	if len(selfColumnList) != len(otherColumnList) {
		return errors.Errorf("column length mismacth (%d vs %s)\n table %s columns %+v\n table %s column %+v", len(selfColumnList), len(otherColumnList), selfName, selfColumnList, other, otherColumnList)
	}

	for i := range selfColumnList {
		if *selfColumnList[i] != *otherColumnList[i] {
			return errors.Errorf("different column definition\n column %v on table %s\n column %v on table %s", selfColumnList[i], selfName, otherColumnList[i], otherName)
		}
	}

	return nil
}

func getBriefColumnList(stmt *ast.CreateTableStmt) []*briefColumnInfo {
	columnList := make([]*briefColumnInfo, 0, len(stmt.Cols))
	for _, col := range stmt.Cols {
		bc := &briefColumnInfo{
			name: col.Name.Name.L,
			tp:   col.Tp.String(),
		}

		for _, opt := range col.Options {
			switch opt.Tp {
			case ast.ColumnOptionPrimaryKey:
				bc.isPrimaryKey = true
			case ast.ColumnOptionUniqKey:
				bc.isUniqueKey = true
			}
		}

		columnList = append(columnList, bc)
	}

	return columnList
}

// Name implements Checker interface
func (c *ShardingTablesCheck) Name() string {
	return "sharding table consistency check"
}
