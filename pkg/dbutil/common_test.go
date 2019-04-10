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
	"database/sql/driver"

	"github.com/go-sql-driver/mysql"
	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	tmysql "github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/infoschema"
	gmysql "github.com/siddontang/go-mysql/mysql"
)

func (*testDBSuite) TestReplacePlaceholder(c *C) {
	testCases := []struct {
		originStr string
		args      []string
		expectStr string
	}{
		{
			"a > ? AND a < ?",
			[]string{"1", "2"},
			"a > '1' AND a < '2'",
		}, {
			"a = ? AND b = ?",
			[]string{"1", "2"},
			"a = '1' AND b = '2'",
		},
	}

	for _, testCase := range testCases {
		str := ReplacePlaceholder(testCase.originStr, testCase.args)
		c.Assert(str, Equals, testCase.expectStr)
	}

}

func (*testDBSuite) TestTableName(c *C) {
	testCases := []struct {
		schema          string
		table           string
		expectTableName string
	}{
		{
			"test",
			"testa",
			"`test`.`testa`",
		},
		{
			"test-1",
			"test-a",
			"`test-1`.`test-a`",
		},
		{
			"test",
			"t`esta",
			"`test`.`t``esta`",
		},
	}

	for _, testCase := range testCases {
		tableName := TableName(testCase.schema, testCase.table)
		c.Assert(tableName, Equals, testCase.expectTableName)
	}
}

func newMysqlErr(number uint16, message string) *mysql.MySQLError {
	return &mysql.MySQLError{
		Number:  number,
		Message: message,
	}
}

func (s *testDBSuite) TestIsRetryableError(c *C) {
	cases := []struct {
		err         error
		isRetryable bool
	}{
		{newMysqlErr(tmysql.ErrNoDB, "no db error"), false},
		{errors.New("unknown error"), false},
		{newMysqlErr(tmysql.ErrUnknown, "i/o timeout"), true},
		{newMysqlErr(tmysql.ErrDBCreateExists, "db already exists"), false},
		{driver.ErrBadConn, false},
		{newMysqlErr(gmysql.ER_LOCK_DEADLOCK, "Deadlock found when trying to get lock; try restarting transaction"), true},
		{newMysqlErr(tmysql.ErrPDServerTimeout, "pd server timeout"), true},
		{newMysqlErr(tmysql.ErrTiKVServerTimeout, "tikv server timeout"), true},
		{newMysqlErr(tmysql.ErrTiKVServerBusy, "tikv server busy"), true},
		{newMysqlErr(tmysql.ErrResolveLockTimeout, "resolve lock timeout"), true},
		{newMysqlErr(tmysql.ErrRegionUnavailable, "region unavailable"), true},
	}

	for _, t := range cases {
		c.Logf("err %v, expected %v", t.err, t.isRetryable)
		c.Assert(isRetryableError(t.err), Equals, t.isRetryable)
	}
}

func (s *testDBSuite) TestIsIgnoreError(c *C) {
	cases := []struct {
		err       error
		canIgnore bool
	}{
		{newMysqlErr(uint16(infoschema.ErrDatabaseExists.Code()), "Can't create database, database exists"), true},
		{newMysqlErr(uint16(infoschema.ErrDatabaseDropExists.Code()), "Can't drop database, database doesn't exists"), true},
		{newMysqlErr(uint16(infoschema.ErrTableExists.Code()), "Can't create table, table exists"), true},
		{newMysqlErr(uint16(infoschema.ErrTableDropExists.Code()), "Can't drop table, table dosen't exists"), true},
		{newMysqlErr(uint16(infoschema.ErrColumnExists.Code()), "Duplicate column name"), true},
		{newMysqlErr(uint16(infoschema.ErrIndexExists.Code()), "Duplicate Index"), true},

		{newMysqlErr(uint16(999), "fake error"), false},
		{errors.New("unknown error"), false},
	}

	for _, t := range cases {
		c.Logf("err %v, expected %v", t.err, t.canIgnore)
		c.Assert(ignoreError(t.err), Equals, t.canIgnore)
	}
}
