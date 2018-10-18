/*************************************************************************
 *
 * PingCAP CONFIDENTIAL
 * __________________
 *
 *  [2015] - [2018] PingCAP Incorporated
 *  All Rights Reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of PingCAP Incorporated and its suppliers,
 * if any.  The intellectual and technical concepts contained
 * herein are proprietary to PingCAP Incorporated
 * and its suppliers and may be covered by P.R.China and Foreign Patents,
 * patents in process, and are protected by trade secret or copyright law.
 * Dissemination of this information or reproduction of this material
 * is strictly forbidden unless prior written permission is obtained
 * from PingCAP Incorporated.
 */

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

func (*testTableSuite) TestTableStructEqual(c *C) {
	createTableSQL1 := "CREATE TABLE `test`.`atest` (`id` int(24), `name` varchar(24), `birthday` datetime, `update_time` time, `money` decimal(20,2), primary key(`id`))"
	tableInfo1, err := GetTableInfoBySQL(createTableSQL1)
	c.Assert(err, IsNil)

	createTableSQL2 := "CREATE TABLE `test`.`atest` (`id` int(24) NOT NULL, `name` varchar(24), `birthday` datetime, `update_time` time, `money` decimal(20,2), primary key(`id`))"
	tableInfo2, err := GetTableInfoBySQL(createTableSQL2)
	c.Assert(err, IsNil)

	createTableSQL3 := "CREATE TABLE `test`.`atest` (`id` int(24), `name` varchar(24), `birthday` datetime, `update_time` time, `money` decimal(20,2), unique key(`id`))"
	tableInfo3, err := GetTableInfoBySQL(createTableSQL3)
	c.Assert(err, IsNil)

	equal := EqualTableInfo(tableInfo1, tableInfo2)
	c.Assert(equal, Equals, true)

	equal = EqualTableInfo(tableInfo1, tableInfo3)
	c.Assert(equal, Equals, false)
}
