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
	"github.com/pingcap/parser/model"
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

func (s *testUtilSuite) TestRowContainsCols(c *C) {
	row := map[string]*dbutil.ColumnData{
		"a": nil,
		"b": nil,
		"c": nil,
	}

	cols := []*model.ColumnInfo{
		{
			Name: model.NewCIStr("a"),
		}, {
			Name: model.NewCIStr("b"),
		}, {
			Name: model.NewCIStr("c"),
		},
	}

	contain := rowContainsCols(row, cols)
	c.Assert(contain, Equals, true)

	delete(row, "a")
	contain = rowContainsCols(row, cols)
	c.Assert(contain, Equals, false)
}

func (s *testUtilSuite) TestRowToString(c *C) {
	row := make(map[string]*dbutil.ColumnData)
	row["id"] = &dbutil.ColumnData{
		Data:   []byte("1"),
		IsNull: false,
	}

	row["name"] = &dbutil.ColumnData{
		Data:   []byte("abc"),
		IsNull: false,
	}

	row["info"] = &dbutil.ColumnData{
		Data:   nil,
		IsNull: true,
	}

	rowStr := rowToString(row)
	c.Assert(rowStr, Equals, "{ id: 1, name: abc, info: IsNull,  }")
}
