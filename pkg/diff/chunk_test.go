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

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb-tools/pkg/dbutil"
	"github.com/pingcap/tidb-tools/pkg/importer"
)

var _ = Suite(&testChunkSuite{})

type testChunkSuite struct{}

type chunkTestCase struct {
	chunk        *chunkRange
	chunkCnt     int64
	expectChunks []*chunkRange
}

func (*testChunkSuite) TestSplitRange(c *C) {
	dbConn, err := createConn()
	c.Assert(err, IsNil)

	_, err = dbConn.Query("CREATE DATABASE IF NOT EXISTS `test`")
	c.Assert(err, IsNil)

	_, err = dbConn.Query("DROP TABLE IF EXISTS `test`.`testa`")
	c.Assert(err, IsNil)

	createTableSQL := `CREATE TABLE test.testa (
		a date NOT NULL,
		b datetime DEFAULT NULL,
		c time DEFAULT NULL,
		d varchar(10) COLLATE latin1_bin DEFAULT NULL,
		e int(10) DEFAULT NULL,
		h year(4) DEFAULT NULL,
		PRIMARY KEY (a))`

	dataCount := 10000
	cfg := &importer.Config{
		TableSQL:    createTableSQL,
		WorkerCount: 5,
		JobCount:    dataCount,
		Batch:       100,
		DBCfg:       dbutil.GetDBConfigFromEnv("test"),
	}

	// generate data for test.testa
	importer.DoProcess(cfg)
	defer dbConn.Query("DROP TABLE IF EXISTS `test`.`testa`")

	// only work on tidb, so don't assert err here
	_, _ = dbConn.Query("ANALYZE TABLE `test`.`testa`")

	tableInfo, err := dbutil.GetTableInfoWithRowID(context.Background(), dbConn, "test", "testa", false)
	c.Assert(err, IsNil)

	tableInstance := &TableInstance{
		Conn:   dbConn,
		Schema: "test",
		Table:  "testa",
		info:   tableInfo,
	}

	// split chunks
	fields, err := getSplitFields(tableInstance.info, nil)
	c.Assert(err, IsNil)
	chunks, mode, err := getChunksForTable(tableInstance, fields, 100, "TRUE", "", false)
	c.Assert(err, IsNil)

	// get data count from every chunk, and the sum of them should equal to the table's count.
	chunkDataCount := 0
	for _, chunk := range chunks {
		conditions, args := chunk.toString(mode, "")
		count, err := dbutil.GetRowCount(context.Background(), tableInstance.Conn, tableInstance.Schema, tableInstance.Table, dbutil.ReplacePlaceholder(conditions, args))
		c.Assert(err, IsNil)
		chunkDataCount += int(count)
	}
	c.Assert(chunkDataCount, Equals, dataCount)
}

func (*testChunkSuite) TestChunkUpdate(c *C) {
	chunk := &chunkRange{
		bounds: []*bound{
			{
				column:      "a",
				lower:       "1",
				lowerSymbol: ">",
				upper:       "2",
				upperSymbol: "<=",
			}, {
				column:      "b",
				lower:       "3",
				lowerSymbol: ">=",
				upper:       "4",
				upperSymbol: "<",
			},
		},
	}

	testCases := []struct {
		boundArgs  []string
		expectStr  string
		expectArgs []string
	}{
		{
			[]string{"a", "5", ">=", "6", "<="},
			"`a` >= ? AND `a` <= ? AND `b` >= ? AND `b` < ?",
			[]string{"5", "6", "3", "4"},
		}, {
			[]string{"a", "5", ">=", "6", "<"},
			"`a` >= ? AND `a` < ? AND `b` >= ? AND `b` < ?",
			[]string{"5", "6", "3", "4"},
		}, {
			[]string{"c", "7", ">", "8", "<"},
			"`a` > ? AND `a` <= ? AND `b` >= ? AND `b` < ? AND `c` > ? AND `c` < ?",
			[]string{"1", "2", "3", "4", "7", "8"},
		},
	}

	for _, cs := range testCases {
		newChunk := chunk.copyAndUpdate(cs.boundArgs[0], cs.boundArgs[1], cs.boundArgs[2], cs.boundArgs[3], cs.boundArgs[4])
		conditions, args := newChunk.toString(normalMode, "")
		c.Assert(conditions, Equals, cs.expectStr)
		c.Assert(args, DeepEquals, cs.expectArgs)
	}

	// the origin chunk is not changed
	conditions, args := chunk.toString(normalMode, "")
	c.Assert(conditions, Equals, "`a` > ? AND `a` <= ? AND `b` >= ? AND `b` < ?")
	expectArgs := []string{"1", "2", "3", "4"}
	for i, arg := range args {
		c.Assert(arg, Equals, expectArgs[i])
	}
}

func (*testChunkSuite) TestChunkToString(c *C) {
	chunk := &chunkRange{
		bounds: []*bound{
			{
				column:      "a",
				lower:       "1",
				lowerSymbol: ">",
				upper:       "2",
				upperSymbol: "<",
			}, {
				column:      "b",
				lower:       "3",
				lowerSymbol: ">",
				upper:       "4",
				upperSymbol: "<",
			}, {
				column:      "c",
				lower:       "5",
				lowerSymbol: ">",
				upper:       "6",
				upperSymbol: "<",
			},
		},
	}

	conditions, args := chunk.toString(normalMode, "")
	c.Assert(conditions, Equals, "`a` > ? AND `a` < ? AND `b` > ? AND `b` < ? AND `c` > ? AND `c` < ?")
	expectArgs := []string{"1", "2", "3", "4", "5", "6"}
	for i, arg := range args {
		c.Assert(arg, Equals, expectArgs[i])
	}

	conditions, args = chunk.toString(normalMode, "latin1")
	c.Assert(conditions, Equals, "`a` COLLATE 'latin1' > ? AND `a` COLLATE 'latin1' < ? AND `b` COLLATE 'latin1' > ? AND `b` COLLATE 'latin1' < ? AND `c` COLLATE 'latin1' > ? AND `c` COLLATE 'latin1' < ?")
	expectArgs = []string{"1", "2", "3", "4", "5", "6"}
	for i, arg := range args {
		c.Assert(arg, Equals, expectArgs[i])
	}

	conditions, args = chunk.toString(bucketMode, "")
	c.Assert(conditions, Equals, "((`a` > ?) OR (`a` = ? AND `b` > ?) OR (`a` = ? AND `b` = ? AND `c` > ?)) AND ((`a` < ?) OR (`a` = ? AND `b` < ?) OR (`a` = ? AND `b` = ? AND `c` < ?))")
	expectArgs = []string{"1", "1", "3", "1", "3", "5", "2", "2", "4", "2", "4", "6"}
	for i, arg := range args {
		c.Assert(arg, Equals, expectArgs[i])
	}

	conditions, args = chunk.toString(bucketMode, "latin1")
	c.Assert(conditions, Equals, "((`a` COLLATE 'latin1' > ?) OR (`a` = ? AND `b` COLLATE 'latin1' > ?) OR (`a` = ? AND `b` = ? AND `c` COLLATE 'latin1' > ?)) AND ((`a` COLLATE 'latin1' < ?) OR (`a` = ? AND `b` COLLATE 'latin1' < ?) OR (`a` = ? AND `b` = ? AND `c` COLLATE 'latin1' < ?))")
	expectArgs = []string{"1", "1", "3", "1", "3", "5", "2", "2", "4", "2", "4", "6"}
	for i, arg := range args {
		c.Assert(arg, Equals, expectArgs[i])
	}
}
