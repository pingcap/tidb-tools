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
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/mysql"
)

func TestClient(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testTableSuite{})

type testTableSuite struct{}

type testCase struct {
	sql     string
	columns []string
	indexs  []string
	colName string
	fineCol bool
}

func (*testTableSuite) TestTable(c *C) {
	testCases := []*testCase{
		{
			`
			CREATE TABLE itest (a int(11) NOT NULL,
			b double NOT NULL DEFAULT '2',
			c varchar(10) NOT NULL,
			d time DEFAULT NULL,
			PRIMARY KEY (a, b),
			UNIQUE KEY d (d))
			`,
			[]string{"a", "b", "c", "d"},
			[]string{mysql.PrimaryKeyName, "d"},
			"a",
			true,
		}, {
			`
			CREATE TABLE jtest (
				a int(11) NOT NULL,
				b varchar(10) DEFAULT NULL,
				c varchar(255) DEFAULT NULL,
				PRIMARY KEY (a)
			) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_bin
			`,
			[]string{"a", "b", "c"},
			[]string{mysql.PrimaryKeyName},
			"c",
			true,
		}, {
			`
			CREATE TABLE mtest (
				a int(24),
				KEY test (a))
			`,
			[]string{"a"},
			[]string{"test"},
			"d",
			false,
		},
	}

	for _, testCase := range testCases {
		tableInfo, err := GetTableInfoBySQL(testCase.sql)
		c.Assert(err, IsNil)
		for i, column := range tableInfo.Columns {
			c.Assert(testCase.columns[i], Equals, column.Name.O)
		}

		for j, index := range tableInfo.Indices {
			c.Assert(testCase.indexs[j], Equals, index.Name.O)
		}

		col := FindColumnByName(tableInfo.Columns, testCase.colName)
		c.Assert(testCase.fineCol, Equals, col != nil)
	}
}
