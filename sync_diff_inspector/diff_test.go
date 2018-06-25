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
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb-tools/pkg/dbutil"
)

func TestClient(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testDiffSuite{})

type testDiffSuite struct{}

func (*testDiffSuite) TestGenerateSQLs(c *C) {
	createTableSQL := "CREATE TABLE `test`.`atest` (`id` int(24), `name` varchar(24), `birthday` datetime, `update_time` time, `money` decimal(20,2), primary key(`id`))"
	tableInfo, err := dbutil.GetTableInfoBySQL(createTableSQL)
	c.Assert(err, IsNil)

	rowsData := map[string][]byte{
		"id":          []byte("1"),
		"name":        []byte("xxx"),
		"birthday":    []byte("2018-01-01 00:00:00"),
		"update_time": []byte("10:10:10"),
		"money":       []byte("11.1111"),
	}
	_, orderKeyCols := dbutil.SelectUniqueOrderKey(tableInfo)
	replaceSQL := generateDML("replace", rowsData, orderKeyCols, tableInfo, "test")
	deleteSQL := generateDML("delete", rowsData, orderKeyCols, tableInfo, "test")
	c.Assert(replaceSQL, Equals, "REPLACE INTO `test`.`atest`(id,name,birthday,update_time,money) VALUES (1,\"xxx\",\"2018-01-01 00:00:00\",\"10:10:10\",11.1111);")
	c.Assert(deleteSQL, Equals, "DELETE FROM `test`.`atest` where id = 1;")
}

func (*testDiffSuite) TestTableStructEqual(c *C) {
	df := &Diff{}

	createTableSQL1 := "CREATE TABLE `test`.`atest` (`id` int(24), `name` varchar(24), `birthday` datetime, `update_time` time, `money` decimal(20,2), primary key(`id`))"
	tableInfo1, err := dbutil.GetTableInfoBySQL(createTableSQL1)
	c.Assert(err, IsNil)

	createTableSQL2 := "CREATE TABLE `test`.`atest` (`id` int(24) NOT NULL, `name` varchar(24), `birthday` datetime, `update_time` time, `money` decimal(20,2), primary key(`id`))"
	tableInfo2, err := dbutil.GetTableInfoBySQL(createTableSQL2)
	c.Assert(err, IsNil)

	createTableSQL3 := "CREATE TABLE `test`.`atest` (`id` int(24), `name` varchar(24), `birthday` datetime, `update_time` time, `money` decimal(20,2), unique key(`id`))"
	tableInfo3, err := dbutil.GetTableInfoBySQL(createTableSQL3)
	c.Assert(err, IsNil)

	equal, err := df.EqualTableStruct(tableInfo1, tableInfo2)
	c.Assert(err, IsNil)
	c.Assert(equal, Equals, true)

	equal, err = df.EqualTableStruct(tableInfo1, tableInfo3)
	c.Assert(err, IsNil)
	c.Assert(equal, Equals, false)
}
