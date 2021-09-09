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
	"os"
	"path"
	"testing"

	"github.com/BurntSushi/toml"
	"github.com/DATA-DOG/go-sqlmock"
	. "github.com/pingcap/check"
	"github.com/pingcap/parser"
	"github.com/pingcap/tidb-tools/pkg/dbutil"
	"github.com/pingcap/tidb-tools/sync_diff_inspector/chunk"
	"github.com/pingcap/tidb-tools/sync_diff_inspector/config"
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

	// Test CalculateTotal
	mock.ExpectQuery("select sum.*").WillReturnRows(sqlmock.NewRows([]string{"data"}).AddRow("123"))
	mock.ExpectQuery("select sum.*where table_schema=.*").WillReturnRows(sqlmock.NewRows([]string{"data"}).AddRow("456"))
	report.CalculateTotalSize(ctx, db)

	// Test Table Report
	report.SetTableStructCheckResult("test", "tbl", true)
	report.SetTableDataCheckResult("test", "tbl", true, 100, 200, &chunk.ChunkID{1, 1, 1, 2})
	report.SetTableMeetError("test", "tbl", errors.New("eeee"))
	report.SetRowsCnt("test", "tbl", 10000, &chunk.ChunkID{1, 1, 1, 2})

	new_report := NewReport()
	new_report.LoadReport(report)

	c.Assert(new_report.TotalSize, Equals, int64(579))
	result, ok := new_report.TableResults["test"]["tbl"]
	c.Assert(ok, IsTrue)
	c.Assert(result.MeetError.Error(), Equals, "eeee")
	c.Assert(result.DataEqual, IsTrue)
	c.Assert(result.StructEqual, IsTrue)
	c.Assert(result.ChunkMap["1:1:1:2"].RowsCnt, Equals, int64(10000))

	c.Assert(new_report.getSortedTables(), DeepEquals, []string{"`atest`.`atbl`", "`test`.`tbl`"})
	c.Assert(new_report.getDiffRows(), DeepEquals, [][]string{})

	new_report.SetTableStructCheckResult("atest", "atbl", true)
	new_report.SetTableDataCheckResult("atest", "atbl", false, 111, 222, &chunk.ChunkID{1, 1, 1, 2})
	c.Assert(new_report.getSortedTables(), DeepEquals, []string{"`test`.`tbl`"})
	c.Assert(new_report.getDiffRows(), DeepEquals, [][]string{{"`atest`.`atbl`", "true", "+111/-222"}})

	new_report.SetTableStructCheckResult("atest", "atbl", false)
	c.Assert(new_report.getSortedTables(), DeepEquals, []string{"`test`.`tbl`"})
	c.Assert(new_report.getDiffRows(), DeepEquals, [][]string{{"`atest`.`atbl`", "false", "+111/-222"}})

	buf := new(bytes.Buffer)
	new_report.Print("[123]", buf)
	c.Assert(buf.String(), Equals, "The structure of `atest`.`atbl` is not equal\n"+
		"The data of `atest`.`atbl` is not equal\n"+
		"\n"+
		"The rest of tables are all equal.\n"+
		"The patch file has been generated to './output_dir/patch.sql'\n"+
		"You can view the comparision details through './output_dir/[123]'\n")
}

func (s *testReportSuite) TestCalculateTotal(c *C) {
	ctx := context.Background()

	db, mock, err := sqlmock.New()
	c.Assert(err, IsNil)

	report := NewReport()
	createTableSQL := "create table `test`.`tbl`(`a` int, `b` varchar(10), `c` float, `d` datetime, primary key(`a`, `b`))"
	tableInfo, err := dbutil.GetTableInfoBySQL(createTableSQL, parser.New())
	c.Assert(err, IsNil)

	tableDiffs := []*common.TableDiff{
		{
			Schema:    "test",
			Table:     "tbl",
			Info:      tableInfo,
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

	// Normal
	mock.ExpectQuery("select sum.*").WillReturnRows(sqlmock.NewRows([]string{"data"}).AddRow("123"))
	report.CalculateTotalSize(ctx, db)
	c.Assert(report.TotalSize, Equals, int64(123))
}

func (s *testReportSuite) TestPrint(c *C) {
	report := NewReport()
	createTableSQL := "create table `test`.`tbl`(`a` int, `b` varchar(10), `c` float, `d` datetime, primary key(`a`, `b`))"
	tableInfo, err := dbutil.GetTableInfoBySQL(createTableSQL, parser.New())
	c.Assert(err, IsNil)

	tableDiffs := []*common.TableDiff{
		{
			Schema:    "test",
			Table:     "tbl",
			Info:      tableInfo,
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

	var buf *bytes.Buffer
	// All Pass
	report.SetTableStructCheckResult("test", "tbl", true)
	report.SetTableDataCheckResult("test", "tbl", true, 0, 0, &chunk.ChunkID{0, 0, 0, 1})
	buf = new(bytes.Buffer)
	report.Print("[123]", buf)
	c.Assert(buf.String(), Equals, "A total of 0 table have been compared and all are equal.\n"+
		"You can view the comparision details through './output_dir/[123]'\n")

	// Error
	report.SetTableMeetError("test", "tbl", errors.New("123"))
	report.SetTableStructCheckResult("test", "tbl", false)
	buf = new(bytes.Buffer)
	report.Print("[123]", buf)
	c.Assert(buf.String(), Equals, "Error in comparison process:\n"+
		"123 error occured in `test`.`tbl`\n"+
		"You can view the comparision details through './output_dir/[123]'\n")
}

func (s *testReportSuite) TestGetSnapshot(c *C) {
	report := NewReport()
	createTableSQL1 := "create table `test`.`tbl`(`a` int, `b` varchar(10), `c` float, `d` datetime, primary key(`a`, `b`))"
	tableInfo1, err := dbutil.GetTableInfoBySQL(createTableSQL1, parser.New())
	c.Assert(err, IsNil)
	createTableSQL2 := "create table `atest`.`tbl`(`a` int, `b` varchar(10), `c` float, `d` datetime, primary key(`a`, `b`))"
	tableInfo2, err := dbutil.GetTableInfoBySQL(createTableSQL2, parser.New())
	c.Assert(err, IsNil)
	createTableSQL3 := "create table `xtest`.`tbl`(`a` int, `b` varchar(10), `c` float, `d` datetime, primary key(`a`, `b`))"
	tableInfo3, err := dbutil.GetTableInfoBySQL(createTableSQL3, parser.New())
	c.Assert(err, IsNil)

	tableDiffs := []*common.TableDiff{
		{
			Schema:    "test",
			Table:     "tbl",
			Info:      tableInfo1,
			Collation: "[123]",
		}, {
			Schema:    "atest",
			Table:     "tbl",
			Info:      tableInfo2,
			Collation: "[123]",
		}, {
			Schema:    "xtest",
			Table:     "tbl",
			Info:      tableInfo3,
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

	report.SetTableStructCheckResult("test", "tbl", true)
	report.SetTableDataCheckResult("test", "tbl", false, 100, 100, &chunk.ChunkID{0, 0, 1, 10})
	report.SetTableDataCheckResult("test", "tbl", true, 0, 0, &chunk.ChunkID{0, 0, 3, 10})
	report.SetTableDataCheckResult("test", "tbl", false, 200, 200, &chunk.ChunkID{0, 0, 3, 10})

	report.SetTableStructCheckResult("atest", "tbl", true)
	report.SetTableDataCheckResult("atest", "tbl", false, 100, 100, &chunk.ChunkID{0, 0, 0, 10})
	report.SetTableDataCheckResult("atest", "tbl", true, 0, 0, &chunk.ChunkID{0, 0, 3, 10})
	report.SetTableDataCheckResult("atest", "tbl", false, 200, 200, &chunk.ChunkID{0, 0, 3, 10})

	report.SetTableStructCheckResult("xtest", "tbl", true)
	report.SetTableDataCheckResult("xtest", "tbl", false, 100, 100, &chunk.ChunkID{0, 0, 0, 10})
	report.SetTableDataCheckResult("xtest", "tbl", true, 0, 0, &chunk.ChunkID{0, 0, 1, 10})
	report.SetTableDataCheckResult("xtest", "tbl", false, 200, 200, &chunk.ChunkID{0, 0, 3, 10})

	report_snap, err := report.GetSnapshot(&chunk.ChunkID{0, 0, 1, 10}, "test", "tbl")
	c.Assert(err, IsNil)
	c.Assert(report_snap.TotalSize, Equals, report.TotalSize)
	c.Assert(report_snap.Result, Equals, report.Result)
	for key, value := range report.TableResults {
		if _, ok := report_snap.TableResults[key]; !ok {
			v, ok := value["tbl"]
			c.Assert(ok, IsTrue)
			c.Assert(v.Schema, Equals, "atest")
			continue
		}

		if _, ok := report_snap.TableResults[key]["tbl"]; !ok {
			c.Assert(key, Equals, "atest")
			continue
		}

		v1 := value["tbl"]
		v2 := report_snap.TableResults[key]["tbl"]
		c.Assert(v1.Schema, Equals, v2.Schema)
		c.Assert(v1.Table, Equals, v2.Table)
		c.Assert(v1.StructEqual, Equals, v2.StructEqual)
		c.Assert(v1.DataEqual, Equals, v2.DataEqual)
		c.Assert(v1.MeetError, Equals, v2.MeetError)

		chunkMap1 := v1.ChunkMap
		chunkMap2 := v2.ChunkMap
		for id, r1 := range chunkMap1 {
			sid := new(chunk.ChunkID)
			if _, ok := chunkMap2[id]; !ok {
				c.Assert(sid.FromString(id), IsNil)
				c.Assert(sid.Compare(&chunk.ChunkID{0, 0, 3, 10}), Equals, 0)
				continue
			}
			c.Assert(sid.FromString(id), IsNil)
			c.Assert(sid.Compare(&chunk.ChunkID{0, 0, 1, 10}), LessEqual, 0)
			r2 := chunkMap2[id]
			c.Assert(r1.RowsAdd, Equals, r2.RowsAdd)
			c.Assert(r1.RowsCnt, Equals, r2.RowsCnt)
			c.Assert(r1.RowsDelete, Equals, r2.RowsDelete)
		}

	}
}

func (s *testReportSuite) TestCommitSummary(c *C) {
	report := NewReport()
	createTableSQL1 := "create table `test`.`tbl`(`a` int, `b` varchar(10), `c` float, `d` datetime, primary key(`a`, `b`))"
	tableInfo1, err := dbutil.GetTableInfoBySQL(createTableSQL1, parser.New())
	c.Assert(err, IsNil)
	createTableSQL2 := "create table `atest`.`tbl`(`a` int, `b` varchar(10), `c` float, `d` datetime, primary key(`a`, `b`))"
	tableInfo2, err := dbutil.GetTableInfoBySQL(createTableSQL2, parser.New())
	c.Assert(err, IsNil)
	createTableSQL3 := "create table `xtest`.`tbl`(`a` int, `b` varchar(10), `c` float, `d` datetime, primary key(`a`, `b`))"
	tableInfo3, err := dbutil.GetTableInfoBySQL(createTableSQL3, parser.New())
	c.Assert(err, IsNil)

	tableDiffs := []*common.TableDiff{
		{
			Schema:    "test",
			Table:     "tbl",
			Info:      tableInfo1,
			Collation: "[123]",
		}, {
			Schema:    "atest",
			Table:     "tbl",
			Info:      tableInfo2,
			Collation: "[123]",
		}, {
			Schema:    "xtest",
			Table:     "tbl",
			Info:      tableInfo3,
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

	report.SetTableStructCheckResult("test", "tbl", true)
	report.SetTableDataCheckResult("test", "tbl", true, 100, 200, &chunk.ChunkID{0, 0, 1, 10})
	report.SetRowsCnt("test", "tbl", 10000, &chunk.ChunkID{0, 0, 1, 10})

	report.SetTableStructCheckResult("atest", "tbl", true)
	report.SetTableDataCheckResult("atest", "tbl", false, 100, 200, &chunk.ChunkID{0, 0, 2, 10})
	report.SetRowsCnt("atest", "tbl", 10000, &chunk.ChunkID{0, 0, 2, 10})

	report.SetTableStructCheckResult("xtest", "tbl", false)
	report.SetTableDataCheckResult("xtest", "tbl", false, 100, 200, &chunk.ChunkID{0, 0, 3, 10})
	report.SetRowsCnt("xtest", "tbl", 10000, &chunk.ChunkID{0, 0, 3, 10})

	outputDir := "./"
	err = report.CommitSummary(&config.TaskConfig{OutputDir: outputDir})
	c.Assert(err, IsNil)
	filename := path.Join(outputDir, "summary.txt")
	file, err := os.Open(filename)
	c.Assert(err, IsNil)

	p := make([]byte, 1024)
	file.Read(p)

	c.Assert(string(p), Matches, "Summary\n\n\n\n"+
		"Source Database\n\n\n\n"+
		"host = \"127.0.0.1\"\n"+
		"port = 3306\n"+
		"user = \"root\"\n\n"+
		"host = \"127.0.0.1\"\n"+
		"port = 3307\n"+
		"user = \"root\"\n\n"+
		"Target Databases\n\n\n\n"+
		"host = \"127.0.0.1\"\n"+
		"port = 4000\n"+
		"user = \"root\"\n\n"+
		"Comparison Result\n\n\n\n"+
		"The table structure and data in following tables are equivalent\n\n"+
		"`test`.`tbl`\n\n"+
		"The following tables contains inconsistent data\n\n"+
		"+---------------+--------------------+----------------+\n"+
		"|     TABLE     | STRUCTURE EQUALITY | DATA DIFF ROWS |\n"+
		"+---------------+--------------------+----------------+\n"+
		"| `atest`.`tbl` | true               | +100/-200      |\n"+
		"| `xtest`.`tbl` | false              | +100/-200      |\n"+
		"+---------------+--------------------+----------------+\n"+
		"Time Cost:.*")

	file.Close()
	err = os.Remove(filename)
	c.Assert(err, IsNil)
}
