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
	. "github.com/pingcap/check"
)

type indexTestCase struct {
	sql     string
	indices []string
	cols    []string
}

func (*testDBSuite) TestIndex(c *C) {
	testCases := []*indexTestCase{
		{
			`
			CREATE TABLE itest (a int(11) NOT NULL,
			b double NOT NULL DEFAULT '2',
			c varchar(10) NOT NULL,
			d time DEFAULT NULL,
			PRIMARY KEY (a, b),
			UNIQUE KEY d(d))
			`,
			[]string{"PRIMARY", "d"},
			[]string{"a", "b", "d"},
		}, {
			`
			CREATE TABLE jtest (
				a int(11) NOT NULL,
				b varchar(10) DEFAULT NULL,
				c varchar(255) DEFAULT NULL,
				KEY c(c),
				UNIQUE KEY b(b, c),
				PRIMARY KEY (a)
			) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_bin
			`,
			[]string{"PRIMARY", "b", "c"},
			[]string{"a", "b", "c"},
		}, {
			`
			CREATE TABLE mtest (
				a int(24),
				KEY test (a))
			`,
			[]string{"test"},
			[]string{"a"},
		},
	}

	for _, testCase := range testCases {
		tableInfo, err := GetTableInfoBySQL(testCase.sql)
		c.Assert(err, IsNil)

		indices := FindAllIndex(tableInfo)
		for i, index := range indices {
			c.Assert(index.Name.O, Equals, testCase.indices[i])
		}

		cols := FindAllColumnWithIndex(tableInfo)
		for j, col := range cols {
			c.Assert(col.Name.O, Equals, testCase.cols[j])
		}
	}
}
