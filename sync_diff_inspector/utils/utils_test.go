package utils

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb-tools/pkg/dbutil"
	"github.com/pingcap/tidb-tools/pkg/importer"
	"github.com/pingcap/tidb-tools/pkg/utils"
)

func TestClient(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testUtilsSuit{})

type testUtilsSuit struct{}

func createConn() (*sql.DB, error) {
	return dbutil.OpenDB(dbutil.GetDBConfigFromEnv(""), nil)
}

func (*testUtilsSuit) TestGetCountAndCRC32Checksum(c *C) {
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

func (*testUtilsSuit) TestGetApproximateMidBySize(c *C) {
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
