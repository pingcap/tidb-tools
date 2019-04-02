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

package diff

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb-tools/pkg/dbutil"
)

var _ = Suite(&testUtilSuite{})

type testUtilSuite struct{}

func (s *testUtilSuite) TestRemoveColumns(c *C) {
	createTableSQL1 := "CREATE TABLE `test`.`atest` (`a` int, `b` int, `c` int, `d` int, primary key(`a`))"
	tableInfo1, err := dbutil.GetTableInfoBySQL(createTableSQL1)
	c.Assert(err, IsNil)
	tbInfo := removeColumns(tableInfo1, []string{"a"})
	c.Assert(len(tbInfo.Columns), Equals, 3)
	c.Assert(len(tbInfo.Indices), Equals, 0)

	createTableSQL2 := "CREATE TABLE `test`.`atest` (`a` int, `b` int, `c` int, `d` int, primary key(`a`), index idx(`b`, `c`))"
	tableInfo2, err := dbutil.GetTableInfoBySQL(createTableSQL2)
	c.Assert(err, IsNil)
	tbInfo = removeColumns(tableInfo2, []string{"a", "b"})
	c.Assert(len(tbInfo.Columns), Equals, 2)
	c.Assert(len(tbInfo.Indices), Equals, 1)

	createTableSQL3 := "CREATE TABLE `test`.`atest` (`a` int, `b` int, `c` int, `d` int, primary key(`a`), index idx(`b`, `c`))"
	tableInfo3, err := dbutil.GetTableInfoBySQL(createTableSQL3)
	c.Assert(err, IsNil)
	tbInfo = removeColumns(tableInfo3, []string{"b", "c"})
	c.Assert(len(tbInfo.Columns), Equals, 2)
	c.Assert(len(tbInfo.Indices), Equals, 1)
}
