package utils

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/parser"
	"github.com/pingcap/tidb-tools/pkg/dbutil"
	"github.com/pingcap/tidb-tools/pkg/importer"
	"github.com/pingcap/tidb-tools/pkg/utils"
)

func TestClient(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testUtilsSuite{})

type testUtilsSuite struct{}

func createConn() (*sql.DB, error) {
	return dbutil.OpenDB(dbutil.GetDBConfigFromEnv(""), nil)
}

func (*testUtilsSuite) TestGetCountAndCRC32Checksum(c *C) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	conn, err := createConn()
	c.Assert(err, IsNil)
	defer conn.Close()
	_, err = conn.ExecContext(ctx, "CREATE DATABASE IF NOT EXISTS `test`")
	c.Assert(err, IsNil)

	_, err = conn.ExecContext(ctx, "DROP TABLE IF EXISTS `test`.`test_utils`")
	c.Assert(err, IsNil)

	createTableSQL := `CREATE TABLE test.test_utils (
		a int(10) NOT NULL,
		b char(10) NOT NULL,
		PRIMARY KEY (a))`

	dataCount := 10000
	cfg := &importer.Config{
		TableSQL:    createTableSQL,
		WorkerCount: 5,
		JobCount:    dataCount,
		Batch:       100,
		DBCfg:       dbutil.GetDBConfigFromEnv("test"),
	}
	// generate data for test.test_utils
	importer.DoProcess(cfg)
	defer conn.ExecContext(ctx, "DROP TABLE IF EXISTS `test`.`test_utils`")

	// only work on tidb, so don't assert err here
	_, _ = conn.ExecContext(ctx, "ANALYZE TABLE `test`.`test_utils`")

	tableInfo, err := dbutil.GetTableInfo(ctx, conn, "test", "test_utils")
	c.Assert(err, IsNil)

	count, checksum, err := GetCountAndCRC32Checksum(ctx, conn, "test", "test_utils", tableInfo, "a>0", utils.StringsToInterfaces([]string{}))
	c.Assert(err, IsNil)
	fmt.Printf("count: %d, checsum: %d\n", count, checksum)
	c.Assert(count, Equals, int64(dataCount-1))
}

func (*testUtilsSuite) TestGetApproximateMidBySize(c *C) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	conn, err := createConn()
	c.Assert(err, IsNil)
	defer conn.Close()
	_, err = conn.ExecContext(ctx, "CREATE DATABASE IF NOT EXISTS `test`")
	c.Assert(err, IsNil)

	_, err = conn.ExecContext(ctx, "DROP TABLE IF EXISTS `test`.`test_utils`")
	c.Assert(err, IsNil)

	createTableSQL := `CREATE TABLE test.test_utils (
		a int(10) NOT NULL,
		b char(10) NOT NULL,
		PRIMARY KEY (a))`

	dataCount := 10000
	cfg := &importer.Config{
		TableSQL:    createTableSQL,
		WorkerCount: 5,
		JobCount:    dataCount,
		Batch:       100,
		DBCfg:       dbutil.GetDBConfigFromEnv("test"),
	}
	// generate data for test.test_utils
	importer.DoProcess(cfg)
	defer conn.ExecContext(ctx, "DROP TABLE IF EXISTS `test`.`test_utils`")

	// only work on tidb, so don't assert err here
	_, _ = conn.ExecContext(ctx, "ANALYZE TABLE `test`.`test_utils`")

	tableInfo, err := dbutil.GetTableInfo(ctx, conn, "test", "test_utils")
	c.Assert(err, IsNil)

	bounds, err := GetApproximateMidBySize(ctx, conn, "test", "test_utils", tableInfo, "a>-1", utils.StringsToInterfaces([]string{}), int64(dataCount))
	c.Assert(err, IsNil)
	c.Assert(len(bounds), Equals, 2)
	c.Assert(bounds[0], Equals, "5000")
}

func (*testUtilsSuite) TestGenerateSQLs(c *C) {
	createTableSQL := "CREATE TABLE `diff_test`.`atest` (`id` int(24), `name` varchar(24), `birthday` datetime, `update_time` time, `money` decimal(20,2), `id_gen` int(11) GENERATED ALWAYS AS ((`id` + 1)) VIRTUAL, primary key(`id`, `name`))"
	tableInfo, err := dbutil.GetTableInfoBySQL(createTableSQL, parser.New())
	c.Assert(err, IsNil)

	rowsData := map[string]*dbutil.ColumnData{
		"id":          {Data: []byte("1"), IsNull: false},
		"name":        {Data: []byte("xxx"), IsNull: false},
		"birthday":    {Data: []byte("2018-01-01 00:00:00"), IsNull: false},
		"update_time": {Data: []byte("10:10:10"), IsNull: false},
		"money":       {Data: []byte("11.1111"), IsNull: false},
		"id_gen":      {Data: []byte("2"), IsNull: false}, // generated column should not be contained in fix sql
	}

	replaceSQL := GenerateReplaceDML(rowsData, tableInfo, "diff_test")
	deleteSQL := GenerateDeleteDML(rowsData, tableInfo, "diff_test")
	c.Assert(replaceSQL, Equals, "REPLACE INTO `diff_test`.`atest`(`id`,`name`,`birthday`,`update_time`,`money`) VALUES (1,'xxx','2018-01-01 00:00:00','10:10:10',11.1111);")
	c.Assert(deleteSQL, Equals, "DELETE FROM `diff_test`.`atest` WHERE `id` = 1 AND `name` = 'xxx' AND `birthday` = '2018-01-01 00:00:00' AND `update_time` = '10:10:10' AND `money` = 11.1111;")

	// test the unique key
	createTableSQL2 := "CREATE TABLE `diff_test`.`atest` (`id` int(24), `name` varchar(24), `birthday` datetime, `update_time` time, `money` decimal(20,2), unique key(`id`, `name`))"
	tableInfo2, err := dbutil.GetTableInfoBySQL(createTableSQL2, parser.New())
	c.Assert(err, IsNil)
	replaceSQL = GenerateReplaceDML(rowsData, tableInfo2, "diff_test")
	deleteSQL = GenerateDeleteDML(rowsData, tableInfo2, "diff_test")
	c.Assert(replaceSQL, Equals, "REPLACE INTO `diff_test`.`atest`(`id`,`name`,`birthday`,`update_time`,`money`) VALUES (1,'xxx','2018-01-01 00:00:00','10:10:10',11.1111);")
	c.Assert(deleteSQL, Equals, "DELETE FROM `diff_test`.`atest` WHERE `id` = 1 AND `name` = 'xxx' AND `birthday` = '2018-01-01 00:00:00' AND `update_time` = '10:10:10' AND `money` = 11.1111;")

	// test value is nil
	rowsData["name"] = &dbutil.ColumnData{Data: []byte(""), IsNull: true}
	replaceSQL = GenerateReplaceDML(rowsData, tableInfo, "diff_test")
	deleteSQL = GenerateDeleteDML(rowsData, tableInfo, "diff_test")
	c.Assert(replaceSQL, Equals, "REPLACE INTO `diff_test`.`atest`(`id`,`name`,`birthday`,`update_time`,`money`) VALUES (1,NULL,'2018-01-01 00:00:00','10:10:10',11.1111);")
	c.Assert(deleteSQL, Equals, "DELETE FROM `diff_test`.`atest` WHERE `id` = 1 AND `name` is NULL AND `birthday` = '2018-01-01 00:00:00' AND `update_time` = '10:10:10' AND `money` = 11.1111;")

	rowsData["id"] = &dbutil.ColumnData{Data: []byte(""), IsNull: true}
	replaceSQL = GenerateReplaceDML(rowsData, tableInfo, "diff_test")
	deleteSQL = GenerateDeleteDML(rowsData, tableInfo, "diff_test")
	c.Assert(replaceSQL, Equals, "REPLACE INTO `diff_test`.`atest`(`id`,`name`,`birthday`,`update_time`,`money`) VALUES (NULL,NULL,'2018-01-01 00:00:00','10:10:10',11.1111);")
	c.Assert(deleteSQL, Equals, "DELETE FROM `diff_test`.`atest` WHERE `id` is NULL AND `name` is NULL AND `birthday` = '2018-01-01 00:00:00' AND `update_time` = '10:10:10' AND `money` = 11.1111;")

	// test value with "'"
	rowsData["name"] = &dbutil.ColumnData{Data: []byte("a'a"), IsNull: false}
	replaceSQL = GenerateReplaceDML(rowsData, tableInfo, "diff_test")
	deleteSQL = GenerateDeleteDML(rowsData, tableInfo, "diff_test")
	c.Assert(replaceSQL, Equals, "REPLACE INTO `diff_test`.`atest`(`id`,`name`,`birthday`,`update_time`,`money`) VALUES (NULL,'a\\'a','2018-01-01 00:00:00','10:10:10',11.1111);")
	c.Assert(deleteSQL, Equals, "DELETE FROM `diff_test`.`atest` WHERE `id` is NULL AND `name` = 'a\\'a' AND `birthday` = '2018-01-01 00:00:00' AND `update_time` = '10:10:10' AND `money` = 11.1111;")
}
