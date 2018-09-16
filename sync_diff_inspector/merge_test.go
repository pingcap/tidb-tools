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

package main

import (
	"container/heap"
	"strconv"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb-tools/pkg/dbutil"
)

var _ = Suite(&testMergerSuite{})

type testMergerSuite struct{}

func (s *testMergerSuite) TestMerge(c *C) {
	createTableSQL := "create table test.test(id int(24), name varchar(24), age int(24), primary key(id, name));"
	tableInfo, err := dbutil.GetTableInfoBySQL(createTableSQL)
	c.Assert(err, IsNil)

	_, orderKeyCols := dbutil.SelectUniqueOrderKey(tableInfo)
	ids := []int{3, 2, 2, 4, 1}
	names := []string{"d", "b", "c", "b", "a"}
	ages := []int{1, 2, 3, 4, 5}
	null := map[string]bool{
		"id":   false,
		"name": false,
		"age":  false,
	}

	expectIDs := []int64{1, 2, 2, 3, 4}
	expectNames := []string{"a", "b", "c", "d", "b"}

	rowDatas := new(RowDatas)
	rowData := &RowDatas {
		Rows:  make([]RowData, 0, len(ids)),
		OrderKeyCols: orderKeyCols,
	}

	heap.Init(rowDatas)
	for i, id := range ids {
		data := map[string][]byte{
			"id":   []byte(strconv.Itoa(id)),
			"name": []byte(names[i]),
			"age":  []byte(strconv.Itoa(ages[i])),
		}
		heap.Push(rowDatas, RowData{
			Data:         data,
			Null:         null,
			//OrderKeyCols: orderKeyCols,
		})
	}

	for i := 0; i < len(ids); i++ {
		rowData := heap.Pop(rowDatas).(RowData)

		id, err := strconv.ParseInt(string(rowData.Data["id"]), 10, 64)
		c.Assert(err, IsNil)
		name := string(rowData.Data["name"])
		c.Assert(id, Equals, expectIDs[i])
		c.Assert(name, Equals, expectNames[i])
	}
}
