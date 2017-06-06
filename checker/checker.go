// Copyright 2016 PingCAP, Inc.
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

package main

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/util/charset"
)

type checker struct {
	db       *sql.DB
	dbName   string
	tbls     []string
	warnings int
	errs     int
}

func (c *checker) close() error {
	return closeDB(c.db)
}

func (c *checker) connectDB() (err error) {
	c.db, err = openDB(c.dbName)
	if err != nil {
		log.Fatal("Open database connection failed:", err)
	}
	return
}
func (c *checker) check() error {
	log.Infof("Checking database %s", c.dbName)
	if c.db == nil {
		err := c.connectDB()
		if err != nil {
			return errors.Trace(err)
		}
	}
	if len(c.tbls) == 0 {
		c.tbls = make([]string, 0)
		err := c.getTables()
		if err != nil {
			return errors.Trace(err)
		}
	}
	for _, t := range c.tbls {
		log.Infof("Checking table %s", t)
		err := c.checkTable(t)
		if err != nil {
			c.errs++
			log.Errorf("Check table %s failed with err: %s", t, errors.ErrorStack(err))
		} else {
			log.Infof("Check table %s succ", t)
		}
	}
	return nil
}

func (c *checker) getTables() error {
	rs, err := querySQL(c.db, "show tables;")
	if err != nil {
		return errors.Trace(err)
	}
	defer rs.Close()
	for rs.Next() {
		var name string
		rs.Scan(&name)
		c.tbls = append(c.tbls, name)
	}
	return nil
}

func (c *checker) getCreateTable(tn string) (string, error) {
	stmt := fmt.Sprintf("show create table `%s`;", tn)
	rs, err := querySQL(c.db, stmt)
	if err != nil {
		return "", errors.Trace(err)
	}
	defer rs.Close()
	if rs.Next() {
		var (
			name string
			cs   string
		)
		rs.Scan(&name, &cs)
		return cs, nil
	}
	return "", errors.Errorf("Can not find table %s", tn)
}

func (c *checker) checkTable(tableName string) error {
	createSQL, err := c.getCreateTable(tableName)
	if err != nil {
		return errors.Trace(err)
	}
	err = c.checkCreateSQL(createSQL)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (c *checker) checkCreateSQL(createSQL string) error {
	stmt, err := parser.New().ParseOneStmt(createSQL, "", "")
	if err != nil {
		return errors.Annotatef(err, " parse %s error", createSQL)
	}
	// Analyze ast
	err = c.checkAST(stmt)
	if err != nil {
		log.Errorf("checkAST error: %s", err)
		return errors.Trace(err)
	}
	return nil
}

func (c *checker) checkAST(stmt ast.StmtNode) error {
	st, ok := stmt.(*ast.CreateTableStmt)
	if !ok {
		return errors.Errorf("Expect CreateTableStmt but got %T", stmt)
	}
	var err error

	// check columns
	for _, def := range st.Cols {
		err = c.checkColumnDef(def)
		if err != nil {
			return errors.Trace(err)
		}
	}
	// check constrains
	for _, cst := range st.Constraints {
		err = c.checkConstraint(cst)
		if err != nil {
			return errors.Trace(err)
		}
	}

	// check options
	for _, opt := range st.Options {
		err = c.checkTableOption(opt)
		if err != nil {
			return errors.Trace(err)
		}
	}
	return errors.Trace(err)
}

func (c *checker) checkColumnDef(def *ast.ColumnDef) error {
	return nil
}

func (c *checker) checkConstraint(cst *ast.Constraint) error {
	switch cst.Tp {
	case ast.ConstraintForeignKey:
		log.Errorf("Foreign Key is parsed but ignored by TiDB.")
		c.warnings++
		return nil
	}
	return nil
}

func (c *checker) checkTableOption(opt *ast.TableOption) error {
	switch opt.Tp {
	case ast.TableOptionCharset:
		// Check charset
		cs := strings.ToLower(opt.StrValue)
		if cs != "binary" && !charset.ValidCharsetAndCollation(cs, "") {
			return errors.Errorf("Unsupported charset %s", opt.StrValue)
		}
	}
	return nil
}

func querySQL(db *sql.DB, query string) (*sql.Rows, error) {
	var (
		err  error
		rows *sql.Rows
	)

	log.Debugf("[query][sql]%s", query)

	rows, err = db.Query(query)

	if err != nil {
		log.Errorf("query sql[%s] failed %v", query, errors.ErrorStack(err))
		return nil, errors.Trace(err)
	}
	return rows, nil

}
