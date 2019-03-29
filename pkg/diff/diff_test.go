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
	"testing"

	_ "github.com/go-sql-driver/mysql"
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb-tools/pkg/dbutil"
	"github.com/pingcap/tidb-tools/pkg/importer"
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

	rowsData := map[string]*dbutil.ColumnData{
		"id":          {Data: []byte("1"), IsNull: false},
		"name":        {Data: []byte("xxx"), IsNull: false},
		"birthday":    {Data: []byte("2018-01-01 00:00:00"), IsNull: false},
		"update_time": {Data: []byte("10:10:10"), IsNull: false},
		"money":       {Data: []byte("11.1111"), IsNull: false},
	}

	_, orderKeyCols := dbutil.SelectUniqueOrderKey(tableInfo)
	replaceSQL := generateDML("replace", rowsData, orderKeyCols, tableInfo, "test")
	deleteSQL := generateDML("delete", rowsData, orderKeyCols, tableInfo, "test")
	c.Assert(replaceSQL, Equals, "REPLACE INTO `test`.`atest`(`id`,`name`,`birthday`,`update_time`,`money`) VALUES (1,'xxx','2018-01-01 00:00:00','10:10:10',11.1111);")
	c.Assert(deleteSQL, Equals, "DELETE FROM `test`.`atest` WHERE `id` = 1;")

	// test the unique key
	createTableSQL2 := "CREATE TABLE `test`.`atest` (`id` int(24), `name` varchar(24), `birthday` datetime, `update_time` time, `money` decimal(20,2), unique key(`id`, `name`))"
	tableInfo2, err := dbutil.GetTableInfoBySQL(createTableSQL2)
	c.Assert(err, IsNil)
	_, orderKeyCols2 := dbutil.SelectUniqueOrderKey(tableInfo2)
	replaceSQL = generateDML("replace", rowsData, orderKeyCols2, tableInfo2, "test")
	deleteSQL = generateDML("delete", rowsData, orderKeyCols2, tableInfo2, "test")
	c.Assert(replaceSQL, Equals, "REPLACE INTO `test`.`atest`(`id`,`name`,`birthday`,`update_time`,`money`) VALUES (1,'xxx','2018-01-01 00:00:00','10:10:10',11.1111);")
	c.Assert(deleteSQL, Equals, "DELETE FROM `test`.`atest` WHERE `id` = 1 AND `name` = 'xxx';")

	// test value is nil
	rowsData["name"] = &dbutil.ColumnData{Data: []byte(""), IsNull: true}
	replaceSQL = generateDML("replace", rowsData, orderKeyCols, tableInfo, "test")
	deleteSQL = generateDML("delete", rowsData, orderKeyCols, tableInfo, "test")
	c.Assert(replaceSQL, Equals, "REPLACE INTO `test`.`atest`(`id`,`name`,`birthday`,`update_time`,`money`) VALUES (1,NULL,'2018-01-01 00:00:00','10:10:10',11.1111);")
	c.Assert(deleteSQL, Equals, "DELETE FROM `test`.`atest` WHERE `id` = 1;")

	rowsData["id"] = &dbutil.ColumnData{Data: []byte(""), IsNull: true}
	replaceSQL = generateDML("replace", rowsData, orderKeyCols, tableInfo, "test")
	deleteSQL = generateDML("delete", rowsData, orderKeyCols, tableInfo, "test")
	c.Assert(replaceSQL, Equals, "REPLACE INTO `test`.`atest`(`id`,`name`,`birthday`,`update_time`,`money`) VALUES (NULL,NULL,'2018-01-01 00:00:00','10:10:10',11.1111);")
	c.Assert(deleteSQL, Equals, "DELETE FROM `test`.`atest` WHERE `id` is NULL;")
}

func (t *testDiffSuite) TestDiff(c *C) {
	dbConn, err := createConn()
	c.Assert(err, IsNil)

	_, err = dbConn.Query("CREATE DATABASE IF NOT EXISTS `test`")
	c.Assert(err, IsNil)

	testStructEqual(dbConn, c)
	testCases := []struct {
		sourceTables  []string
		targetTable   string
		hasEmptyTable bool
	}{
		{
			[]string{"testa"},
			"testb",
			false,
		},
		{
			[]string{"testc", "testd"},
			"teste",
			false,
		},
		{
			[]string{"testf", "testg", "testh"},
			"testi",
			false,
		},
		{
			[]string{"testj", "testk"},
			"testl",
			true,
		},
	}
	for _, testCase := range testCases {
		testDataEqual(dbConn, testCase.sourceTables, testCase.targetTable, testCase.hasEmptyTable, c)
	}
}

func testStructEqual(conn *sql.DB, c *C) {
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
		}, {
			"CREATE TABLE `test`.`testa`(`id` int, `name` varchar(24))",
			"CREATE TABLE `test`.`testb`(`id` varchar(24), name varchar(24))",
			"DROP TABLE `test`.`testa`",
			"DROP TABLE `test`.`testb`",
			false,
		},
	}

	for _, testCase := range testCases {
		_, err := conn.Query(testCase.createSourceTable)
		c.Assert(err, IsNil)
		_, err = conn.Query(testCase.createTargetTable)
		c.Assert(err, IsNil)

		tableDiff := createTableDiff(conn, []string{"testa"}, "testb")
		structEqual, _, err := tableDiff.Equal(context.Background(), func(sql string) error {
			fmt.Println(sql)
			return nil
		})
		c.Assert(structEqual, Equals, testCase.structEqual)

		_, err = conn.Query(testCase.dropSourceTable)
		c.Assert(err, IsNil)
		_, err = conn.Query(testCase.dropTargetTable)
		c.Assert(err, IsNil)
	}
}

func testDataEqual(dbConn *sql.DB, sourceTables []string, targetTable string, hasEmptyTable bool, c *C) {
	defer func() {
		for _, sourceTable := range sourceTables {
			_, _ = dbConn.Query(fmt.Sprintf("DROP TABLE `test`.`%s`", sourceTable))
		}
		_, _ = dbConn.Query(fmt.Sprintf("DROP TABLE `test`.`%s`", targetTable))
	}()

	err := generateData(dbConn, dbutil.GetDBConfigFromEnv("test"), sourceTables, targetTable, hasEmptyTable)
	c.Assert(err, IsNil)

	// compare data, should be equal
	fixSqls := make([]string, 0, 10)
	writeSqls := func(sql string) error {
		fixSqls = append(fixSqls, sql)
		return nil
	}

	tableDiff := createTableDiff(dbConn, sourceTables, targetTable)
	structEqual, dataEqual, err := tableDiff.Equal(context.Background(), writeSqls)
	c.Assert(err, IsNil)
	c.Assert(structEqual, Equals, true)
	c.Assert(dataEqual, Equals, true)

	// update data and then compare data, dataEqual should be false
	err = updateData(dbConn, targetTable)
	c.Assert(err, IsNil)

	structEqual, dataEqual, err = tableDiff.Equal(context.Background(), writeSqls)
	c.Assert(err, IsNil)
	c.Assert(structEqual, Equals, true)
	c.Assert(dataEqual, Equals, false)

	// use fixSqls to fix data, and then compare data
	for _, sql := range fixSqls {
		_, err = dbConn.Exec(sql)
		c.Assert(err, IsNil)
	}
	structEqual, dataEqual, err = tableDiff.Equal(context.Background(), writeSqls)
	c.Assert(err, IsNil)
	c.Assert(structEqual, Equals, true)
	c.Assert(dataEqual, Equals, true)
}

func createTableDiff(db *sql.DB, sourceTableNames []string, targetTableName string) *TableDiff {
	sourceTables := []*TableInstance{}
	for _, table := range sourceTableNames {
		sourceTableInstance := &TableInstance{
			Conn:   db,
			Schema: "test",
			Table:  table,
		}

		sourceTables = append(sourceTables, sourceTableInstance)
	}

	targetTableInstance := &TableInstance{
		Conn:   db,
		Schema: "test",
		Table:  targetTableName,
	}

	return &TableDiff{
		SourceTables: sourceTables,
		TargetTable:  targetTableInstance,
	}
}

func createConn() (*sql.DB, error) {
	return dbutil.OpenDB(dbutil.GetDBConfigFromEnv(""))
}

func generateData(dbConn *sql.DB, dbCfg dbutil.DBConfig, sourceTables []string, targetTable string, hasEmptyTable bool) error {
	createTableSQL := fmt.Sprintf(`CREATE TABLE test.%s (
		a date NOT NULL,
		b datetime DEFAULT NULL,
		c time DEFAULT NULL,
		d varchar(10) COLLATE latin1_bin DEFAULT NULL,
		e int(10) DEFAULT NULL,
		h year(4) DEFAULT NULL,
		PRIMARY KEY (a))`, targetTable)

	cfg := &importer.Config{
		TableSQL:    createTableSQL,
		WorkerCount: 5,
		JobCount:    10000,
		Batch:       100,
		DBCfg:       dbCfg,
	}

	// generate data for target table
	importer.DoProcess(cfg)

	// generate data for source tables
	for _, sourceTable := range sourceTables {
		_, err := dbConn.Query(fmt.Sprintf("CREATE TABLE `test`.`%s` LIKE `test`.`%s`", sourceTable, targetTable))
		if err != nil {
			return err
		}
	}

	randomValueNum := int64(len(sourceTables) - 1)
	if hasEmptyTable {
		randomValueNum--
	}

	values, _, err := dbutil.GetRandomValues(context.Background(), dbConn, "test", targetTable, "e", int(randomValueNum), "TRUE", nil, "")
	if err != nil {
		return err
	}

	conditions := make([]string, 0, 3)
	if randomValueNum == 0 {
		conditions = append(conditions, "true")
	} else {
		conditions = append(conditions, fmt.Sprintf("e < %s", values[0]))
		for i := 0; i < len(values)-1; i++ {
			conditions = append(conditions, fmt.Sprintf("e >= %s AND e < %s", values[i], values[i+1]))
		}
		conditions = append(conditions, fmt.Sprintf("e >= %s", values[len(values)-1]))
	}

	// if hasEmptyTable is true, the last source table will be empty.
	for j, condition := range conditions {
		_, err = dbConn.Query(fmt.Sprintf("INSERT INTO `test`.`%s` (`a`, `b`, `c`, `d`, `e`, `h`) SELECT `a`, `b`, `c`, `d`, `e`, `h` FROM `test`.`%s` WHERE %s", sourceTables[j], targetTable, condition))
		if err != nil {
			return err
		}
	}

	return nil
}

func updateData(dbConn *sql.DB, table string) error {
	values, _, err := dbutil.GetRandomValues(context.Background(), dbConn, "test", table, "e", 3, "TRUE", nil, "")
	if err != nil {
		return err
	}

	_, err = dbConn.Exec(fmt.Sprintf("UPDATE `test`.`%s` SET `e` = `e`+1 WHERE `e` = %v", table, values[0]))
	if err != nil {
		return err
	}

	_, err = dbConn.Exec(fmt.Sprintf("DELETE FROM `test`.`%s` where `e` = %v", table, values[1]))
	if err != nil {
		return err
	}

	_, err = dbConn.Exec(fmt.Sprintf("REPLACE INTO `test`.`%s` VALUES('1992-09-27','2018-09-03 16:26:27','14:45:33','i',2048790075,2008)", table))
	if err != nil {
		return err
	}

	return nil
}
