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
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/go-sql-driver/mysql"
	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/meta/model"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	pmysql "github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/types"
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

func (*testDBSuite) TestColumnName(c *C) {
	testCases := []struct {
		column        string
		expectColName string
	}{
		{
			"test",
			"`test`",
		},
		{
			"test-1",
			"`test-1`",
		},
		{
			"t`esta",
			"`t``esta`",
		},
	}

	for _, testCase := range testCases {
		colName := ColumnName(testCase.column)
		c.Assert(colName, Equals, testCase.expectColName)
	}
}

func newMysqlErr(number uint16, message string) *mysql.MySQLError {
	return &mysql.MySQLError{
		Number:  number,
		Message: message,
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

func (s *testDBSuite) TestDeleteRows(c *C) {
	db, mock, err := sqlmock.New()
	c.Assert(err, IsNil)

	// delete twice
	mock.ExpectExec("DELETE FROM").WillReturnResult(sqlmock.NewResult(0, DefaultDeleteRowsNum))
	mock.ExpectExec("DELETE FROM").WillReturnResult(sqlmock.NewResult(0, DefaultDeleteRowsNum-1))

	err = DeleteRows(context.Background(), db, "test", "t", "", nil)
	c.Assert(err, IsNil)

	if err := mock.ExpectationsWereMet(); err != nil {
		c.Errorf("there were unfulfilled expectations: %s", err)
	}
}

func (s *testDBSuite) TestGetParser(c *C) {
	testCases := []struct {
		sqlModeStr string
		hasErr     bool
	}{
		{
			"",
			false,
		}, {
			"ANSI_QUOTES",
			false,
		}, {
			"ANSI_QUOTES,IGNORE_SPACE",
			false,
		}, {
			"ANSI_QUOTES123",
			true,
		}, {
			"ANSI_QUOTES,IGNORE_SPACE123",
			true,
		},
	}

	for _, testCase := range testCases {
		parser, err := getParser(testCase.sqlModeStr)
		if testCase.hasErr {
			c.Assert(err, NotNil)
		} else {
			c.Assert(err, IsNil)
			c.Assert(parser, NotNil)
		}
	}
}

func (s *testDBSuite) TestAnalyzeValuesFromBuckets(c *C) {
	ftTypeDatetime := types.NewFieldType(pmysql.TypeDatetime)
	ftTypeTimestamp := types.NewFieldType(pmysql.TypeTimestamp)
	ftTypeDate := types.NewFieldType(pmysql.TypeDate)

	cases := []struct {
		value  string
		col    *model.ColumnInfo
		expect string
	}{
		{
			"2021-03-05 21:31:03",
			&model.ColumnInfo{FieldType: *ftTypeDatetime},
			"2021-03-05 21:31:03",
		},
		{
			"2021-03-05 21:31:03",
			&model.ColumnInfo{FieldType: *ftTypeTimestamp},
			"2021-03-05 21:31:03",
		},
		{
			"2021-03-05",
			&model.ColumnInfo{FieldType: *ftTypeDate},
			"2021-03-05",
		},
		{
			"1847956477067657216",
			&model.ColumnInfo{FieldType: *ftTypeDatetime},
			"2020-01-01 10:00:00",
		},
		{
			"1847955927311843328",
			&model.ColumnInfo{FieldType: *ftTypeTimestamp},
			"2020-01-01 02:00:00",
		},
		{
			"1847955789872889856",
			&model.ColumnInfo{FieldType: *ftTypeDate},
			"2020-01-01 00:00:00",
		},
	}
	for _, ca := range cases {
		val, err := AnalyzeValuesFromBuckets(ca.value, []*model.ColumnInfo{ca.col})
		c.Assert(err, IsNil)
		c.Assert(val, HasLen, 1)
		c.Assert(val[0], Equals, ca.expect)
	}
}

func (s *testDBSuite) TestFormatTimeZoneOffset(c *C) {
	cases := map[string]time.Duration{
		"+00:00": 0,
		"+01:00": time.Hour,
		"-08:03": -1 * (8*time.Hour + 3*time.Minute),
		"-12:59": -1 * (12*time.Hour + 59*time.Minute),
		"+12:59": 12*time.Hour + 59*time.Minute,
	}

	for k, v := range cases {
		offset := FormatTimeZoneOffset(v)
		c.Assert(k, Equals, offset)
	}
}

func (*testDBSuite) TestGetBucketsInfo(c *C) {
	db, mock, err := sqlmock.New()
	c.Assert(err, IsNil)
	defer db.Close()

	ctx := context.Background()

	// Create test table info
	tableInfo := &model.TableInfo{
		ID:   1001,
		Name: pmodel.NewCIStr("test_table"),
		Columns: []*model.ColumnInfo{
			{ID: 1, Name: pmodel.NewCIStr("id"), Offset: 0},
			{ID: 2, Name: pmodel.NewCIStr("name"), Offset: 1},
			{ID: 3, Name: pmodel.NewCIStr("age"), Offset: 2},
		},
		Indices: []*model.IndexInfo{
			{
				ID:   1,
				Name: pmodel.NewCIStr("PRIMARY"),
				Columns: []*model.IndexColumn{
					{Name: pmodel.NewCIStr("id"), Offset: 0},
				},
				Primary: true,
			},
			{
				ID:   2,
				Name: pmodel.NewCIStr("idx_name"),
				Columns: []*model.IndexColumn{
					{Name: pmodel.NewCIStr("name"), Offset: 1},
				},
				Unique: true,
			},
		},
	}

	// Mock query and expected results
	expectedSQL := "select hist_id,is_index,bucket_id,count,lower_bound,upper_bound from stats_buckets where table_id = \\?;"

	// Create mock rows for stats_buckets query
	rows := sqlmock.NewRows([]string{"hist_id", "is_index", "bucket_id", "count", "lower_bound", "upper_bound"}).
		// PRIMARY index statistics (is_index=1, hist_id=1 maps to index ID 1)
		AddRow(1, 1, 0, 100, "1", "50").
		AddRow(1, 1, 1, 200, "51", "100").
		// idx_name index statistics (is_index=1, hist_id=2 maps to index ID 2)
		AddRow(2, 1, 0, 150, "alice", "john").
		AddRow(2, 1, 1, 300, "kate", "zoe").
		// Column statistics (is_index=0, hist_id=3 maps to column ID 3 = "age")
		AddRow(3, 0, 0, 80, "18", "30").
		AddRow(3, 0, 1, 120, "31", "60")

	mock.ExpectQuery(expectedSQL).WithArgs(1001).WillReturnRows(rows)

	// Execute the function
	buckets, err := GetBucketsInfo(ctx, db, "test_db", "test_table", tableInfo)
	c.Assert(err, IsNil)

	// Verify results
	c.Assert(len(buckets), Equals, 3)

	// Check PRIMARY index buckets
	primaryBuckets, exists := buckets["PRIMARY"]
	c.Assert(exists, Equals, true)
	c.Assert(len(primaryBuckets), Equals, 2)
	c.Assert(primaryBuckets[0].Count, Equals, int64(100))
	c.Assert(primaryBuckets[0].LowerBound, Equals, "1")
	c.Assert(primaryBuckets[0].UpperBound, Equals, "50")
	c.Assert(primaryBuckets[1].Count, Equals, int64(200))
	c.Assert(primaryBuckets[1].LowerBound, Equals, "51")
	c.Assert(primaryBuckets[1].UpperBound, Equals, "100")

	// Check idx_name index buckets
	nameBuckets, exists := buckets["idx_name"]
	c.Assert(exists, Equals, true)
	c.Assert(len(nameBuckets), Equals, 2)
	c.Assert(nameBuckets[0].Count, Equals, int64(150))
	c.Assert(nameBuckets[0].LowerBound, Equals, "alice")
	c.Assert(nameBuckets[0].UpperBound, Equals, "john")

	// Check age column buckets
	ageBuckets, exists := buckets["age"]
	c.Assert(exists, Equals, true)
	c.Assert(len(ageBuckets), Equals, 2)
	c.Assert(ageBuckets[0].Count, Equals, int64(80))
	c.Assert(ageBuckets[0].LowerBound, Equals, "18")
	c.Assert(ageBuckets[0].UpperBound, Equals, "30")

	// Verify all expectations were met
	c.Assert(mock.ExpectationsWereMet(), IsNil)
}

func (*testDBSuite) TestGetBucketsInfoEmptyResult(c *C) {
	db, mock, err := sqlmock.New()
	c.Assert(err, IsNil)
	defer db.Close()

	ctx := context.Background()

	tableInfo := &model.TableInfo{
		ID:   1002,
		Name: pmodel.NewCIStr("empty_table"),
		Columns: []*model.ColumnInfo{
			{ID: 1, Name: pmodel.NewCIStr("id"), Offset: 0},
		},
		Indices: []*model.IndexInfo{
			{
				ID:   1,
				Name: pmodel.NewCIStr("PRIMARY"),
				Columns: []*model.IndexColumn{
					{Name: pmodel.NewCIStr("id"), Offset: 0},
				},
				Primary: true,
			},
		},
	}

	// Mock empty result
	expectedSQL := "select hist_id,is_index,bucket_id,count,lower_bound,upper_bound from stats_buckets where table_id = \\?;"
	rows := sqlmock.NewRows([]string{"hist_id", "is_index", "bucket_id", "count", "lower_bound", "upper_bound"})

	mock.ExpectQuery(expectedSQL).WithArgs(1002).WillReturnRows(rows)

	// Execute the function
	buckets, err := GetBucketsInfo(ctx, db, "test_db", "empty_table", tableInfo)
	c.Assert(err, IsNil)
	c.Assert(len(buckets), Equals, 0)

	// Verify all expectations were met
	c.Assert(mock.ExpectationsWereMet(), IsNil)
}
