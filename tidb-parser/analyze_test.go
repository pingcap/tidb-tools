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

	. "github.com/pingcap/check" // check
	"github.com/pingcap/tidb/parser"
)

var _ = Suite(&testAnalyzeSuite{
	Parser: parser.New(),
})

type testAnalyzeSuite struct {
	*parser.Parser
}

func (t *testAnalyzeSuite) TestCreateDatabase(c *C) {
	sqlCases := []string{
		"create database Test character set latin1 collate latin1_swedish_ci;",
		"create database Test",
	}
	expected := "CREATE DATABASE IF NOT EXISTS `Test`;"

	for _, sql := range sqlCases {
		node, err := t.ParseOneStmt(sql, "", "")
		c.Assert(err, IsNil)

		res, err := AnalyzeASTNode(node, sql)
		c.Assert(err, IsNil)
		c.Assert(res, Equals, expected)
	}
}

func (t *testAnalyzeSuite) TestDropDatabase(c *C) {
	sqlCases := []string{
		"drop database if exists Test",
		"drop schema Test",
	}
	expected := "DROP DATABASE IF EXISTS `Test`;"

	for _, sql := range sqlCases {
		node, err := t.ParseOneStmt(sql, "", "")
		c.Assert(err, IsNil)

		res, err := AnalyzeASTNode(node, sql)
		c.Assert(err, IsNil)
		c.Assert(res, Equals, expected)
	}
}

func (t *testAnalyzeSuite) TestDropTable(c *C) {
	sqlCases := []string{
		"drop table t1 RESTRICT",
		"drop table if exists t.T2 CASCADE",
		"drop table t1, X.t2",
	}
	expectedSQLs := []string{
		"DROP TABLE IF EXISTS `t1`",
		"DROP TABLE IF EXISTS `t`.`T2`",
		"DROP TABLE IF EXISTS `t1`,`X`.`t2`",
	}

	for i := range sqlCases {
		node, err := t.ParseOneStmt(sqlCases[i], "", "")
		c.Assert(err, IsNil)

		res, err := AnalyzeASTNode(node, sqlCases[i])
		c.Assert(err, IsNil)
		c.Assert(res, Equals, expectedSQLs[i])
	}
}

func (t *testAnalyzeSuite) TestTruncateTable(c *C) {
	sqlCases := []string{
		"truncate table T1",
		"truncate X.t1",
	}
	expectedSqls := []string{
		"TRUNCATE TABLE `T1`",
		"TRUNCATE TABLE `X`.`t1`",
	}

	for i := range sqlCases {
		node, err := t.ParseOneStmt(sqlCases[i], "", "")
		c.Assert(err, IsNil)

		res, err := AnalyzeASTNode(node, sqlCases[i])
		c.Assert(err, IsNil)
		c.Assert(res, Equals, expectedSqls[i])
	}
}

func (t *testAnalyzeSuite) TestRenameTable(c *C) {
	sqlCases := []string{
		"rename table T.t1 to T2",
		"rename table t1 to t2, t3 to t4",
	}
	expectedSQLs := []string{
		"RENAME TABLE `T`.`t1` TO `T2`",
		"RENAME TABLE `t1` TO `t2`,`t3` TO `t4`",
	}

	for i := range sqlCases {
		node, err := t.ParseOneStmt(sqlCases[i], "", "")
		c.Assert(err, IsNil)

		res, err := AnalyzeASTNode(node, sqlCases[i])
		c.Assert(err, IsNil)
		c.Assert(res, Equals, expectedSQLs[i])
	}
}

func (t *testAnalyzeSuite) TestDropIndex(c *C) {
	sqlCases := []string{
		"drop index X ON T2",
		"drop index x ON T.t1",
		"drop index x on t1",
	}
	expectedSQLs := []string{
		"DROP INDEX IF EXISTS X ON `T2`",
		"DROP INDEX IF EXISTS x ON `T`.`t1`",
		"DROP INDEX IF EXISTS x ON `t1`",
	}

	for i := range sqlCases {
		node, err := t.ParseOneStmt(sqlCases[i], "", "")
		c.Assert(err, IsNil)

		res, err := AnalyzeASTNode(node, sqlCases[i])
		c.Assert(err, IsNil)
		c.Assert(res, Equals, expectedSQLs[i])
	}
}

func (t *testAnalyzeSuite) TestCreateTable(c *C) {
	// test create like
	sqlCases := []string{
		"create table T1 like T.t1",
		"create table t.T1 like t",
	}
	expectedSQLs := []string{
		"CREATE TABLE IF NOT EXISTS `T1` LIKE `T`.`t1`",
		"CREATE TABLE IF NOT EXISTS `t`.`T1` LIKE `t`",
	}

	for i := range sqlCases {
		node, err := t.ParseOneStmt(sqlCases[i], "", "")
		c.Assert(err, IsNil)

		res, err := AnalyzeASTNode(node, sqlCases[i])
		c.Assert(err, IsNil)
		c.Assert(res, Equals, expectedSQLs[i])
	}

	// test create table temporarily
	// CREATE [TEMPORARY] TABLE [IF NOT EXISTS] tbl_name
	otherCases := []string{
		"CREATE TABLE IF NOT EXISTS T1 (id int)",
		"CREATE TABLE T.t1 (id int)",
		"CREATE TABLE t (id int)",
		"CREATE TEMPORARY TABLE t (id int)",
	}
	otherExpecteds := []string{
		"CREATE TABLE IF NOT EXISTS `X`.`t` (id int)",
		"CREATE TABLE `X`.`t` (id int)",
		"CREATE TABLE `X`.`t` (id int)",
		"CREATE TEMPORARY TABLE `X`.`t` (id int)",
	}
	node, err := t.ParseOneStmt("create table X.t (id int)", "", "")
	c.Assert(err, IsNil)
	for i := range otherCases {
		res, err := AnalyzeASTNode(node, otherCases[i])
		c.Assert(err, IsNil)
		c.Assert(res, Equals, otherExpecteds[i])
	}
}

func (t *testAnalyzeSuite) TestCreateIndex(c *C) {
	sqlCases := []string{
		"create index x using hash ON t (id(10) ASC, name) COMMENT \"haha\"",
		"create unique index x on T (id)",
		"create unique index x on T.t (id) USING btree comment \"haha\"",
	}
	expectedSQLs := []string{
		"CREATE  INDEX x ON `t` (id(10),name) USING HASH COMMENT \"haha\"",
		"CREATE UNIQUE INDEX x ON `T` (id)",
		"CREATE UNIQUE INDEX x ON `T`.`t` (id) USING BTREE COMMENT \"haha\"",
	}

	for i := range sqlCases {
		node, err := t.ParseOneStmt(sqlCases[i], "", "")
		c.Assert(err, IsNil)

		res, err := AnalyzeASTNode(node, sqlCases[i])
		c.Assert(err, IsNil)
		c.Assert(strings.TrimSpace(res), Equals, expectedSQLs[i])
	}
}
