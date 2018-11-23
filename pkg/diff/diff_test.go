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
	"context"
	"database/sql"
	"fmt"
	"os"
	"strconv"
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
	null := map[string]bool{
		"id":          false,
		"name":        false,
		"birthday":    false,
		"update_time": false,
		"money":       false,
	}
	_, orderKeyCols := dbutil.SelectUniqueOrderKey(tableInfo)
	replaceSQL := generateDML("replace", rowsData, null, orderKeyCols, tableInfo, "test")
	deleteSQL := generateDML("delete", rowsData, null, orderKeyCols, tableInfo, "test")
	c.Assert(replaceSQL, Equals, "REPLACE INTO `test`.`atest`(`id`,`name`,`birthday`,`update_time`,`money`) VALUES (1,\"xxx\",\"2018-01-01 00:00:00\",\"10:10:10\",11.1111);")
	c.Assert(deleteSQL, Equals, "DELETE FROM `test`.`atest` WHERE `id` = 1;")

	// test the unique key
	createTableSQL2 := "CREATE TABLE `test`.`atest` (`id` int(24), `name` varchar(24), `birthday` datetime, `update_time` time, `money` decimal(20,2), unique key(`id`, `name`))"
	tableInfo2, err := dbutil.GetTableInfoBySQL(createTableSQL2)
	c.Assert(err, IsNil)
	_, orderKeyCols2 := dbutil.SelectUniqueOrderKey(tableInfo2)
	replaceSQL = generateDML("replace", rowsData, null, orderKeyCols2, tableInfo2, "test")
	deleteSQL = generateDML("delete", rowsData, null, orderKeyCols2, tableInfo2, "test")
	c.Assert(replaceSQL, Equals, "REPLACE INTO `test`.`atest`(`id`,`name`,`birthday`,`update_time`,`money`) VALUES (1,\"xxx\",\"2018-01-01 00:00:00\",\"10:10:10\",11.1111);")
	c.Assert(deleteSQL, Equals, "DELETE FROM `test`.`atest` WHERE `id` = 1 AND `name` = \"xxx\";")

	// test value is nil
	rowsData["name"] = []byte("")
	null["name"] = true
	replaceSQL = generateDML("replace", rowsData, null, orderKeyCols, tableInfo, "test")
	deleteSQL = generateDML("delete", rowsData, null, orderKeyCols, tableInfo, "test")
	c.Assert(replaceSQL, Equals, "REPLACE INTO `test`.`atest`(`id`,`name`,`birthday`,`update_time`,`money`) VALUES (1,NULL,\"2018-01-01 00:00:00\",\"10:10:10\",11.1111);")
	c.Assert(deleteSQL, Equals, "DELETE FROM `test`.`atest` WHERE `id` = 1;")

	rowsData["id"] = []byte("")
	null["id"] = true
	replaceSQL = generateDML("replace", rowsData, null, orderKeyCols, tableInfo, "test")
	deleteSQL = generateDML("delete", rowsData, null, orderKeyCols, tableInfo, "test")
	c.Assert(replaceSQL, Equals, "REPLACE INTO `test`.`atest`(`id`,`name`,`birthday`,`update_time`,`money`) VALUES (NULL,NULL,\"2018-01-01 00:00:00\",\"10:10:10\",11.1111);")
	c.Assert(deleteSQL, Equals, "DELETE FROM `test`.`atest` WHERE `id` is NULL;")
}

var (
	testSchema string = "test"
	testTable1 string = "testa"
	testTable2 string = "testb"
)

func (t *testDiffSuite) TestDiff(c *C) {
	dbConn, err := getConn()
	c.Assert(err, IsNil)

	_, err = dbConn.Query("create database if not exists test")
	c.Assert(err, IsNIl)

	t.TestStructEqual(dbConn, c)
	t.TestDataEqual(dbConn, c)
}

func (*testDiffSuite) TestStructEqual(conn *sql.DB, c *C) {
	testCases := []struct {
		createSourceTable string
		createTargetTable string
		dropSourceTable   string
		dropTargetTable   string
		structEqual       bool
	}{
		{
			"CREATE TABLE `test`.`testa`(`id` int, `name` varchar(24))",
			"CREATE TABLE `test`.`testb`(`id` int, `name` varchar(24), unique key (`id`))",
			"DROP TABLE `test`.`testa`",
			"DROP TABLE `test`.`testb`",
			false,
		}, {
			"CREATE TABLE `test`.`testa`(`id` int, `name` varchar(24))",
			"CREATE TABLE `test`.`testb`(`id` int, `name2` varchar(24))",
			"DROP TABLE `test`.`testa`",
			"DROP TABLE `test`.`testb`",
			false,
		}, {
			"CREATE TABLE `test`.`testa`(`id` int, `name` varchar(24))",
			"CREATE TABLE `test`.`testb`(`id` int)",
			"DROP TABLE `test`.`testa`",
			"DROP TABLE `test`.`testb`",
			false,
		}, {
			"CREATE TABLE `test`.`testa`(`id` int, `name` varchar(24))",
			"CREATE TABLE `test`.`testb`(`id` int, `name` varchar(24))",
			"DROP TABLE `test`.`testa`",
			"DROP TABLE `test`.`testb`",
			true,
		},
	}

	for _, testCase := range testCases {
		_, err = dbConn.Query(testCase.createSourceTable)
		c.Assert(err, IsNil)
		_, err = dbConn.Query(testCase.createTargetTable)
		c.Assert(err, IsNil)

		tableDiff := createTableDiff(dbConn)
		structEqual, _, err := tableDiff.Equal(context.Background(), func(sql string) error {
			fmt.Println(sql)
		})
		c.Assert(structEqual, Equals, testCase.structEqual)

		_, err = dbConn.Query(testCase.dropSourceTable)
		c.Assert(err, IsNil)
		_, err = dbConn.Query(testCase.dropTargetTable)
		c.Assert(err, IsNil)
	}
	_, err := db.Exec(sql)

}

func (*testDiffSuite) TestDataEqual(dbConn *sql.DB, c *C) {
	_, err := dbConn.Query("CREATE TABLE `testa` (
		`a` date NOT NULL,
		`b` datetime DEFAULT NULL,
		`c` time DEFAULT NULL,
		`d` varchar(10) COLLATE latin1_bin DEFAULT NULL,
		`e` int(10) DEFAULT NULL,
		`h` year(4) DEFAULT NULL,
		PRIMARY KEY (`a`)
	  )")
	  c.Assert(err, IsNil)

	_, err := dbConn.Query("CREATE TABLE `testb` (
		`a` date NOT NULL,
		`b` datetime DEFAULT NULL,
		`c` time DEFAULT NULL,
		`d` varchar(10) COLLATE latin1_bin DEFAULT NULL,
		`e` int(10) DEFAULT NULL,
		`h` year(4) DEFAULT NULL,
		PRIMARY KEY (`a`)
	)")
	c.Assert(err, IsNil)

	

	// INSERT INTO test888(id, name) SELECT id, name FROM test999;
}

func createTableDiff(db *sql.DB) *TableDiff {
	sourceTableInstance := &TableInstance{
		Conn:   db,
		Schema: testSchema,
		Table:  testTable1,
	}

	targetTableInstance := &TableInstance{
		Conn:   db,
		Schema: testSchema,
		Table:  testTable2,
	}

	return &TableDiff{
		SourceTables: []*TableInstance{sourceTableInstance},
		TargetTable:  targetTableInstance,
	}
}

func getConn() (*sql.DB, error) {
	host := os.Getenv("MYSQL_HOST")
	if host == "" {
		host = "127.0.0.1"
	}
	port, _ := strconv.Atoi(os.Getenv("MYSQL_PORT"))
	if port == 0 {
		port = 3306
	}
	user := os.Getenv("MYSQL_USER")
	if user == "" {
		user = "root"
	}
	pswd := os.Getenv("MYSQL_PSWD")

	dbConfig := dbutil.DBConfig{
		Host:     host,
		Port:     port,
		User:     user,
		Password: pswd,
	}

	return dbutil.OpenDB(dbConfig)
}
