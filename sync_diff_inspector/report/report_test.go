// Copyright 2021 PingCAP, Inc.
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

package report

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/BurntSushi/toml"
	"github.com/DATA-DOG/go-sqlmock"
	. "github.com/pingcap/check"
	"github.com/pingcap/parser"
	"github.com/pingcap/tidb-tools/pkg/dbutil"
	"github.com/pingcap/tidb-tools/sync_diff_inspector/source/common"
)

func TestClient(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testReportSuite{})

type testReportSuite struct{}

func (s *testReportSuite) TestReport(c *C) {
	ctx := context.Background()

	db, mock, err := sqlmock.New()
	c.Assert(err, IsNil)

	report := NewReport()
	createTableSQL1 := "create table `test`.`tbl`(`a` int, `b` varchar(10), `c` float, `d` datetime, primary key(`a`, `b`))"
	tableInfo1, err := dbutil.GetTableInfoBySQL(createTableSQL1, parser.New())
	c.Assert(err, IsNil)
	createTableSQL2 := "create table `atest`.`atbl`(`a` int, `b` varchar(10), `c` float, `d` datetime, primary key(`a`, `b`))"
	tableInfo2, err := dbutil.GetTableInfoBySQL(createTableSQL2, parser.New())
	c.Assert(err, IsNil)

	tableDiffs := []*common.TableDiff{
		{
			Schema:    "test",
			Table:     "tbl",
			Info:      tableInfo1,
			Collation: "[123]",
		},
		{
			Schema:    "atest",
			Table:     "atbl",
			Info:      tableInfo2,
			Collation: "[123]",
		},
	}
	configs := []*ReportConfig{
		{
			Host: "127.0.0.1",
			Port: 3306,
			User: "root",
		},
		{
			Host: "127.0.0.1",
			Port: 3307,
			User: "root",
		},
		{
			Host: "127.0.0.1",
			Port: 4000,
			User: "root",
		},
	}

	configsBytes := make([][]byte, 3)
	for i := 0; i < 3; i++ {
		buf := new(bytes.Buffer)
		err := toml.NewEncoder(buf).Encode(configs[i])
		c.Assert(err, IsNil)
		configsBytes[i] = buf.Bytes()
	}
	report.Init(tableDiffs, configsBytes[:2], configsBytes[2])

	mock.ExpectQuery("select sum.*").WillReturnRows(sqlmock.NewRows([]string{"data"}).AddRow("123"))
	mock.ExpectQuery("select sum.*where table_schema='atest'").WillReturnRows(sqlmock.NewRows([]string{"data"}).AddRow("456"))
	report.CalculateTotalSize(ctx, db, 2, 4)

	report.SetTableStructCheckResult("test", "tbl", true)
	report.SetTableDataCheckResult("test", "tbl", true, 100, 200, 100)
	report.SetTableMeetError("test", "tbl", errors.New("eeee"))
	report.SetRowsCnt("test", "tbl", 10000, 100)

	new_report := NewReport()
	new_report.LoadReport(report)

	c.Assert(new_report.TotalSize, Equals, int64(579))
	result, ok := new_report.TableResults["test"]["tbl"]
	c.Assert(ok, IsTrue)
	c.Assert(result.MeetError.Error(), Equals, "eeee")
	c.Assert(result.DataEqual, IsTrue)
	c.Assert(result.StructEqual, IsTrue)
	c.Assert(result.ChunkMap[100].RowsCnt, Equals, int64(10000))

	c.Assert(new_report.getSortedTables(), DeepEquals, []string{"`atest`.`atbl`", "`test`.`tbl`"})
	c.Assert(new_report.getDiffRows(), DeepEquals, [][]string{})

	new_report.SetTableStructCheckResult("atest", "atbl", false)
	new_report.SetTableDataCheckResult("atest", "atbl", false, 111, 222, 100)
	c.Assert(new_report.getSortedTables(), DeepEquals, []string{"`test`.`tbl`"})
	c.Assert(new_report.getDiffRows(), DeepEquals, [][]string{{"`atest`.`atbl`", "false", "+111/-222"}})
}
