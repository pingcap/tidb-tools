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
	"testing"

	. "github.com/pingcap/check" // check
	filter "github.com/pingcap/tidb-tools/pkg/binlog-filter"
	"github.com/pingcap/tidb-tools/pkg/table-router"
)

func TestClient(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testParserSuite{})

type testParserSuite struct {
	p Parser
}

type testCase struct {
	sql          string
	expectedSqls []string
	ignore       bool
}

func (t *testParserSuite) TestHandle(c *C) {
	r, err := router.NewTableRouter([]*router.TableRule{
		{"test*", "abc*", "test", "abc"},
	})
	c.Assert(err, IsNil)

	filterRules := []*filter.BinlogEventRule{
		{
			SchemaPattern: "*",
			TablePattern:  "",
			DMLEvent:      nil,
			DDLEvent:      nil,
			SQLPattern:    []string{"^DROP\\s+PROCEDURE", "^CREATE\\s+PROCEDURE"},
			Action:        filter.Ignore,
		},
		{
			SchemaPattern: "test*",
			TablePattern:  "abc*",
			DMLEvent:      []filter.EventType{filter.AllEvent},
			DDLEvent:      []filter.EventType{filter.TruncateTable},
			SQLPattern:    nil,
			Action:        filter.Ignore,
		},
	}
	f, err := filter.NewBinlogEvent(filterRules)
	c.Assert(err, IsNil)

	t.p = NewTiDBParser(r, f)

	// test unsupported sqls
	sqls, ignore, err := t.p.Handle("foo", "CREATE PROCEDURE x")
	c.Assert(err, IsNil)
	c.Assert(sqls, HasLen, 0)
	c.Assert(ignore, IsTrue)

	t.testHandleCreateDatabase(c)
	t.testHandleDropDatabase(c)
	t.testHandleCreateTable(c)
	t.testHandleDropTable(c)
	t.testHandleTruncateTable(c)
	t.testHandleCreateIndex(c)
	t.testHandleDropIndex(c)
	t.testHandleRenameTable(c)
	t.testHandleAlterTable(c)
}

func (t *testParserSuite) testHandleCreateDatabase(c *C) {
	cases := []testCase{
		{
			"create database test1",
			[]string{"CREATE DATABASE IF NOT EXISTS `test1`;"},
			false,
		},
		{
			"create database abc1",
			[]string{"CREATE DATABASE IF NOT EXISTS `abc1`;"},
			false,
		},
	}

	for i := range cases {
		sqls, ignore, err := t.p.Handle("", cases[i].sql)
		c.Assert(err, IsNil)
		c.Assert(ignore, Equals, cases[i].ignore)
		c.Assert(sqls, DeepEquals, cases[i].expectedSqls)
	}
}

func (t *testParserSuite) testHandleDropDatabase(c *C) {
	cases := []testCase{
		{
			"drop database test1",
			[]string{"DROP DATABASE IF EXISTS `test1`;"},
			false,
		},
		{
			"drop database abc1",
			[]string{"DROP DATABASE IF EXISTS `abc1`;"},
			false,
		},
	}

	for i := range cases {
		sqls, ignore, err := t.p.Handle("", cases[i].sql)
		c.Assert(err, IsNil)
		c.Assert(ignore, Equals, cases[i].ignore)
		c.Assert(sqls, DeepEquals, cases[i].expectedSqls)
	}
}

func (t *testParserSuite) testHandleCreateTable(c *C) {
	cases := []testCase{
		{
			"create table abc1 (id int)",
			[]string{"create table `test`.`abc` (id int)"},
			false,
		},
		{
			"create table foo.abc1 (id int)",
			[]string{"create table `foo`.`abc1` (id int)"},
			false,
		},
		{
			"create table abc1 like abc2",
			[]string{"CREATE TABLE IF NOT EXISTS `test`.`abc` LIKE `test`.`abc`"},
			false,
		},
		{
			"create table abc1 like foo.abc2",
			[]string{"CREATE TABLE IF NOT EXISTS `test`.`abc` LIKE `foo`.`abc2`"},
			false,
		},
	}

	for i := range cases {
		sqls, ignore, err := t.p.Handle("test1", cases[i].sql)
		c.Assert(err, IsNil)
		c.Assert(ignore, Equals, cases[i].ignore)
		c.Assert(sqls, DeepEquals, cases[i].expectedSqls)
	}
}

func (t *testParserSuite) testHandleDropTable(c *C) {
	cases := []testCase{
		{
			"drop table abc1, foo.abc1, tx",
			[]string{"DROP TABLE IF EXISTS `test`.`abc`,`foo`.`abc1`,`test1`.`tx`"},
			false,
		},
	}

	for i := range cases {
		sqls, ignore, err := t.p.Handle("test1", cases[i].sql)
		c.Assert(err, IsNil)
		c.Assert(ignore, Equals, cases[i].ignore)
		c.Assert(sqls, DeepEquals, cases[i].expectedSqls)
	}
}

func (t *testParserSuite) testHandleTruncateTable(c *C) {
	cases := []testCase{
		{
			"truncate table abc1",
			nil,
			true,
		},
		{
			"truncate table foo.abc1",
			[]string{"TRUNCATE TABLE `foo`.`abc1`"},
			false,
		},
	}

	for i := range cases {
		sqls, ignore, err := t.p.Handle("test1", cases[i].sql)
		c.Assert(err, IsNil)
		c.Assert(ignore, Equals, cases[i].ignore)
		c.Assert(sqls, DeepEquals, cases[i].expectedSqls)
	}
}

func (t *testParserSuite) testHandleCreateIndex(c *C) {
	cases := []testCase{
		{
			"create index x using hash ON abc1 (id(10) ASC, name) COMMENT \"haha\"",
			[]string{"CREATE  INDEX `x` ON `test`.`abc` (`id`(10),`name`) USING HASH COMMENT \"haha\""},
			false,
		},
		{
			"create unique index x on foo.abc1 (id)",
			[]string{"CREATE UNIQUE INDEX `x` ON `foo`.`abc1` (`id`) "},
			false,
		},
	}

	for i := range cases {
		sqls, ignore, err := t.p.Handle("test1", cases[i].sql)
		c.Assert(err, IsNil)
		c.Assert(ignore, Equals, cases[i].ignore)
		c.Assert(sqls, DeepEquals, cases[i].expectedSqls)
	}
}

func (t *testParserSuite) testHandleDropIndex(c *C) {
	cases := []testCase{
		{
			"drop index X ON abc1",
			[]string{"DROP INDEX IF EXISTS X ON `test`.`abc`"},
			false,
		},
		{
			"drop index X ON foo.abc1",
			[]string{"DROP INDEX IF EXISTS X ON `foo`.`abc1`"},
			false,
		},
	}

	for i := range cases {
		sqls, ignore, err := t.p.Handle("test1", cases[i].sql)
		c.Assert(err, IsNil)
		c.Assert(ignore, Equals, cases[i].ignore)
		c.Assert(sqls, DeepEquals, cases[i].expectedSqls)
	}
}

func (t *testParserSuite) testHandleRenameTable(c *C) {
	cases := []testCase{
		{
			"rename table abc1 to abc2, foo.abc3 to abc4, abc4 to foo.abc4",
			[]string{"RENAME TABLE `test`.`abc` TO `test`.`abc`", "RENAME TABLE `foo`.`abc3` TO `test`.`abc`", "RENAME TABLE `test`.`abc` TO `foo`.`abc4`"},
			false,
		},
	}

	for i := range cases {
		sqls, ignore, err := t.p.Handle("test1", cases[i].sql)
		c.Assert(err, IsNil)
		c.Assert(ignore, Equals, cases[i].ignore)
		c.Assert(sqls, DeepEquals, cases[i].expectedSqls)
	}
}

func (t *testParserSuite) testHandleAlterTable(c *C) {
	cases := []testCase{
		{
			"alter table abc1 rename to xx, add column (id int, age int), add index (id)",
			[]string{"ALTER TABLE `test`.`abc` ADD COLUMN `id` int(11) ", "ALTER TABLE `test`.`abc` ADD COLUMN `age` int(11) ", "ALTER TABLE `test`.`abc` ADD INDEX  (`id`) ", "RENAME TABLE `test`.`abc` TO `test1`.`xx`"},
			false,
		},
	}

	for i := range cases {
		sqls, ignore, err := t.p.Handle("test1", cases[i].sql)
		c.Assert(err, IsNil)
		c.Assert(ignore, Equals, cases[i].ignore)
		c.Assert(sqls, DeepEquals, cases[i].expectedSqls)
	}
}
