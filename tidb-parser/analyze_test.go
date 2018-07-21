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
		"CREATE  INDEX `x` ON `t` (`id`(10),`name`) USING HASH COMMENT \"haha\"",
		"CREATE UNIQUE INDEX `x` ON `T` (`id`)",
		"CREATE UNIQUE INDEX `x` ON `T`.`t` (`id`) USING BTREE COMMENT \"haha\"",
	}

	for i := range sqlCases {
		node, err := t.ParseOneStmt(sqlCases[i], "", "")
		c.Assert(err, IsNil)

		res, err := AnalyzeASTNode(node, sqlCases[i])
		c.Assert(err, IsNil)
		c.Assert(strings.TrimSpace(res), Equals, expectedSQLs[i])
	}
}

func (t *testAnalyzeSuite) TestAlterTable(c *C) {
	t.testAlterTableOption(c)
	t.testAlterTableAddColumn(c)
	t.testAlterTableDropColumn(c)
	t.testAlterTableDropIndex(c)
	t.testAlterTableDropForeignKey(c)
	t.testAlterTableAddConstraint(c)
	t.testAlterTableAlterColumn(c)
	t.testAlterTableDropPrimaryKey(c)
	t.testAlterTableRenameTable(c)
	t.testAlterTableModifyColumn(c)
	t.testAlterTableChangeColumn(c)
}

func (t *testAnalyzeSuite) testAlterTableOption(c *C) {
	// unsupported compression and engine=''
	sqlCases := []string{
		"alter table bar add index (id),character set utf8 collate utf8_bin",
		"alter table bar add index (id), character set utf8",
		"alter table bar collate utf8_bin comment 'bar'",
		"alter table bar ENGINE = InnoDB COMMENT='table bar' ROW_FORMAT = COMPRESSED",
		"alter table bar add index (`id`), auto_increment = 1",
		"alter table bar AVG_ROW_LENGTH = 1024",
		"alter table bar CHECKSUM = 1, COMPRESSION = 'zlib',  CONNECTION 'mysql://username:password@hostname:port/database/tablename'",
		"alter table bar PASSWORD 'abc123', KEY_BLOCK_SIZE = 128, MAX_ROWS 2, MIN_ROWS 0",
		"alter table bar DELAY_KEY_WRITE = 0, STATS_PERSISTENT 1, engine=''",
	}
	expectedSQLs := []string{
		"ALTER TABLE `bar` ADD INDEX  (`id`) ,DEFAULT CHARACTER SET utf8 DEFAULT COLLATE utf8_bin",
		"ALTER TABLE `bar` ADD INDEX  (`id`) ,DEFAULT CHARACTER SET utf8",
		"ALTER TABLE `bar` DEFAULT COLLATE utf8_bin COMMENT='bar'",
		"ALTER TABLE `bar` ENGINE=InnoDB COMMENT='table bar' ROW_FORMAT=COMPRESSED",
		"ALTER TABLE `bar` ADD INDEX  (`id`) ,AUTO_INCREMENT=1",
		"ALTER TABLE `bar` AVG_ROW_LENGTH=1024",
		"ALTER TABLE `bar` CHECKSUM=1,COMPRESSION='zlib',CONNECTION='mysql://username:password@hostname:port/database/tablename'",
		"ALTER TABLE `bar` PASSWORD='abc123',KEY_BLOCK_SIZE=128,MAX_ROWS=2,MIN_ROWS=0",
		"ALTER TABLE `bar` DELAY_KEY_WRITE=0,STATS_PERSISTENT=DEFAULT,ENGINE=''",
	}

	for i := range sqlCases {
		node, err := t.ParseOneStmt(sqlCases[i], "", "")
		c.Assert(err, IsNil)

		res, err := AnalyzeASTNode(node, sqlCases[i])
		c.Assert(err, IsNil)
		c.Assert(strings.TrimSpace(res), Equals, expectedSQLs[i])
	}
}

func (t *testAnalyzeSuite) testAlterTableAddColumn(c *C) {
	sqlCases := []string{
		"alter table `bar` add column `id1` int(11) unsigned zerofill not null first,add column `id2` int(11) unsigned zerofill not null after id1",
		"alter table `bar` add column `id1` int(11) not null primary key comment 'id1', add column id2 decimal(14,2) not null default 0.01, add id3 int(11) unique, add id4 int(11) unique key",
		"alter table bar add c1 timestamp not null on update current_timestamp,add c2 timestamp null default 20150606 on update current_timestamp,add c3 timestamp default current_timestamp on update current_timestamp, add c4 timestamp not null default now()  on update now()",
		"alter table bar add c1 varchar(10) CHARACTER SET utf8 COLLATE utf8_general_ci null COMMENT '计费重量体积',add c2 smallint not null default 100, add c3 blob, add c4 text binary",
		"alter /* gh-ost */ table `foo`.`bar` add column ( id int unsigned default 0 comment 'xx', name char(20) not null)",
	}
	expectedSQLs := []string{
		"ALTER TABLE `bar` ADD COLUMN `id1` int(11) UNSIGNED ZEROFILL NOT NULL FIRST,ADD COLUMN `id2` int(11) UNSIGNED ZEROFILL NOT NULL AFTER `id1`",
		"ALTER TABLE `bar` ADD COLUMN `id1` int(11) NOT NULL PRIMARY KEY COMMENT \"id1\",ADD COLUMN `id2` decimal(14,2) NOT NULL DEFAULT 0.01,ADD COLUMN `id3` int(11) UNIQUE KEY,ADD COLUMN `id4` int(11) UNIQUE KEY",
		"ALTER TABLE `bar` ADD COLUMN `c1` timestamp NOT NULL ON UPDATE current_timestamp(),ADD COLUMN `c2` timestamp NULL DEFAULT 20150606 ON UPDATE current_timestamp(),ADD COLUMN `c3` timestamp DEFAULT current_timestamp() ON UPDATE current_timestamp(),ADD COLUMN `c4` timestamp NOT NULL DEFAULT current_timestamp() ON UPDATE current_timestamp()",
		"ALTER TABLE `bar` ADD COLUMN `c1` varchar(10) CHARACTER SET utf8 COLLATE utf8_general_ci NULL COMMENT \"计费重量体积\",ADD COLUMN `c2` smallint(6) NOT NULL DEFAULT 100,ADD COLUMN `c3` blob ,ADD COLUMN `c4` text BINARY",
		"ALTER TABLE `foo`.`bar` ADD COLUMN (`id` int(11) UNSIGNED DEFAULT 0 COMMENT \"xx\",`name` char(20) NOT NULL)",
	}

	for i := range sqlCases {
		node, err := t.ParseOneStmt(sqlCases[i], "", "")
		c.Assert(err, IsNil)

		res, err := AnalyzeASTNode(node, sqlCases[i])
		c.Assert(err, IsNil)
		c.Assert(strings.TrimSpace(res), Equals, expectedSQLs[i])
	}
}

func (t *testAnalyzeSuite) testAlterTableDropColumn(c *C) {
	sqlCases := []string{"alter table foo.bar drop id, drop name"}
	expectedSQLs := []string{"ALTER TABLE `foo`.`bar` DROP COLUMN `id`,DROP COLUMN `name`"}

	for i := range sqlCases {
		node, err := t.ParseOneStmt(sqlCases[i], "", "")
		c.Assert(err, IsNil)

		res, err := AnalyzeASTNode(node, sqlCases[i])
		c.Assert(err, IsNil)
		c.Assert(strings.TrimSpace(res), Equals, expectedSQLs[i])
	}
}

func (t *testAnalyzeSuite) testAlterTableAddConstraint(c *C) {
	sqlCases := []string{
		"alter table `bar` add index (`id1`) using btree COMMENT \"xx\",add key using hash (`id1`,`id2`)",
		"alter table bar ADD CONSTRAINT `pri` PRIMARY KEY (`id`)",
		"alter table bar ADD CONSTRAINT PRIMARY KEY (`id`)",
		"alter table bar add constraint `id` unique (`id`)",
		"alter table bar add unique (`id`)",
		"alter table bar add CONSTRAINT `pp` FOREIGN KEY (product_category, product_id) REFERENCES product(category, id)",
		"alter tabLE bar ADD FULLTEXT INDEX `fulltext` (`name` ASC)",
	}
	expectedSQLs := []string{
		"ALTER TABLE `bar` ADD INDEX  (`id1`) USING BTREE COMMENT \"xx\",ADD INDEX  (`id1`,`id2`) USING HASH",
		"ALTER TABLE `bar` ADD CONSTRAINT `pri` PRIMARY KEY (`id`)",
		"ALTER TABLE `bar` ADD CONSTRAINT PRIMARY KEY (`id`)",
		"ALTER TABLE `bar` ADD CONSTRAINT `id` UNIQUE INDEX (`id`)",
		"ALTER TABLE `bar` ADD CONSTRAINT UNIQUE INDEX (`id`)",
		"ALTER TABLE `bar` ADD CONSTRAINT `pp` FOREIGN KEY (`product_category`,`product_id`) REFERENCES `product` (`category`,`id`)",
		"ALTER TABLE `bar` ADD FULLTEXT INDEX `fulltext` (`name`)",
	}

	for i := range sqlCases {
		node, err := t.ParseOneStmt(sqlCases[i], "", "")
		c.Assert(err, IsNil)

		res, err := AnalyzeASTNode(node, sqlCases[i])
		c.Assert(err, IsNil)
		c.Assert(strings.TrimSpace(res), Equals, expectedSQLs[i])
	}
}

func (t *testAnalyzeSuite) testAlterTableDropIndex(c *C) {
	sqlCases := []string{"alter table `bar` drop index `id1`,drop key `id1_2`"}
	expectedSQLs := []string{"ALTER TABLE `bar` DROP INDEX `id1`,DROP INDEX `id1_2`"}

	for i := range sqlCases {
		node, err := t.ParseOneStmt(sqlCases[i], "", "")
		c.Assert(err, IsNil)

		res, err := AnalyzeASTNode(node, sqlCases[i])
		c.Assert(err, IsNil)
		c.Assert(strings.TrimSpace(res), Equals, expectedSQLs[i])
	}
}

func (t *testAnalyzeSuite) testAlterTableDropForeignKey(c *C) {
	sqlCases := []string{"alter table `bar` drop FOREIGN KEY b"}
	expectedSQLs := []string{"ALTER TABLE `bar` DROP FOREIGN KEY `b`"}

	for i := range sqlCases {
		node, err := t.ParseOneStmt(sqlCases[i], "", "")
		c.Assert(err, IsNil)

		res, err := AnalyzeASTNode(node, sqlCases[i])
		c.Assert(err, IsNil)
		c.Assert(strings.TrimSpace(res), Equals, expectedSQLs[i])
	}
}

func (t *testAnalyzeSuite) testAlterTableModifyColumn(c *C) {
	sqlCases := []string{
		"alter table bar modify id varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci, modify column id text first",
		"alter table bar modify column id enum('signup','unique','sliding')",
	}
	expectedSQLs := []string{
		"ALTER TABLE `bar` MODIFY COLUMN `id` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci ,MODIFY COLUMN `id` text  FIRST",
		"ALTER TABLE `bar` MODIFY COLUMN `id` enum('signup','unique','sliding')",
	}

	for i := range sqlCases {
		node, err := t.ParseOneStmt(sqlCases[i], "", "")
		c.Assert(err, IsNil)

		res, err := AnalyzeASTNode(node, sqlCases[i])
		c.Assert(err, IsNil)
		c.Assert(strings.TrimSpace(res), Equals, expectedSQLs[i])
	}
}

func (t *testAnalyzeSuite) testAlterTableChangeColumn(c *C) {
	sqlCases := []string{
		"alter table bar change id name varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci, change column name age text first",
	}
	expectedSQLs := []string{
		"ALTER TABLE `bar` CHANGE COLUMN  `id` `name` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci ,CHANGE COLUMN  `name` `age` text  FIRST",
	}

	for i := range sqlCases {
		node, err := t.ParseOneStmt(sqlCases[i], "", "")
		c.Assert(err, IsNil)

		res, err := AnalyzeASTNode(node, sqlCases[i])
		c.Assert(err, IsNil)
		c.Assert(strings.TrimSpace(res), Equals, expectedSQLs[i])
	}
}

func (t *testAnalyzeSuite) testAlterTableRenameTable(c *C) {
	sqlCases := []string{"alter table bar rename to bar1, rename as bar2"}
	expectedSQLs := []string{"ALTER TABLE `bar` RENAME TO `bar1`,RENAME TO `bar2`"}

	for i := range sqlCases {
		node, err := t.ParseOneStmt(sqlCases[i], "", "")
		c.Assert(err, IsNil)

		res, err := AnalyzeASTNode(node, sqlCases[i])
		c.Assert(err, IsNil)
		c.Assert(strings.TrimSpace(res), Equals, expectedSQLs[i])
	}
}

func (t *testAnalyzeSuite) testAlterTableAlterColumn(c *C) {
	sqlCases := []string{"alter table bar alter `id` set default 1, alter `name` drop default"}
	expectedSQLs := []string{"ALTER TABLE `bar` ALTER COLUMN `id` SET DEFAULT 1,ALTER COLUMN `name` DROP DEFAULT"}

	for i := range sqlCases {
		node, err := t.ParseOneStmt(sqlCases[i], "", "")
		c.Assert(err, IsNil)

		res, err := AnalyzeASTNode(node, sqlCases[i])
		c.Assert(err, IsNil)
		c.Assert(strings.TrimSpace(res), Equals, expectedSQLs[i])
	}
}

func (t *testAnalyzeSuite) testAlterTableDropPrimaryKey(c *C) {
	sqlCases := []string{"alter table `bar` DROP PRIMARY KEY"}
	expectedSQLs := []string{"ALTER TABLE `bar` DROP PRIMARY KEY"}

	for i := range sqlCases {
		node, err := t.ParseOneStmt(sqlCases[i], "", "")
		c.Assert(err, IsNil)

		res, err := AnalyzeASTNode(node, sqlCases[i])
		c.Assert(err, IsNil)
		c.Assert(strings.TrimSpace(res), Equals, expectedSQLs[i])
	}
}
