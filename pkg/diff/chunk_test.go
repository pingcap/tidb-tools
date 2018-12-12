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

	_, err = dbConn.Query("CREATE DATABASE IF NOT EXISTS test")
	c.Assert(err, IsNil)

	_, err = dbConn.Query("DROP TABLE IF EXISTS test.testa")
	c.Assert(err, IsNil)

	createTableSQL := `CREATE TABLE test.testa (
		a date NOT NULL,
		b datetime DEFAULT NULL,
		c time DEFAULT NULL,
		d varchar(10) COLLATE latin1_bin DEFAULT NULL,
		e int(10) DEFAULT NULL,
		h year(4) DEFAULT NULL,
		PRIMARY KEY (a))`

	cfg := &importer.Config{
		TableSQL:    createTableSQL,
		WorkerCount: 1,
		JobCount:    10000,
		Batch:       100,
		DBCfg:       dbutil.GetDBConfigFromEnv("test"),
	}

	// generate data for test.tetsa
	importer.DoProcess(cfg)
	defer dbConn.Query("DROP TABLE IF EXISTS test.testa")

	tableInfo, err := dbutil.GetTableInfoWithRowID(context.Background(), dbConn, "test", "testa", false)
	c.Assert(err, IsNil)

	tableInstance := &TableInstance{
		Conn:   dbConn,
		Schema: "test",
		Table:  "testa",
		info:   tableInfo,
	}

	// split chunks
	fields, err := getSplitFields(tableInstance.Conn, tableInstance.Schema, tableInstance.info, nil)
	c.Assert(err, IsNil)
	chunks, mode, err := getChunksForTable(tableInstance, fields, 100, "true", "")
	c.Assert(err, IsNil)

	// get data count from every chunk, and the sum of them should equal to the table's count.
	allCount := 0
	for _, chunk := range chunks {
		conditions, args := chunk.toString(mode, "")
		count, err := dbutil.GetRowCount(context.Background(), tableInstance.Conn, tableInstance.Schema, tableInstance.Table, dbutil.ReplacePlaceholder(conditions, args))
		c.Assert(err, IsNil)
		allCount += int(count)
	}
	c.Assert(allCount, Equals, 10000)
}

func (*testChunkSuite) TestChunkUpdate(c *C) {
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
			},
		},
	}

	// update a bound
	newChunk := chunk.copy()
	newChunk.update("a", "5", ">=", "6", "<=")
	conditions, args := newChunk.toString("normal", "")
	c.Assert(conditions, Equals, "`a` >= ? AND `a` <= ? AND `b` > ? AND `b` < ?")
	expectArgs := []string{"5", "6", "3", "4"}
	for i, arg := range args {
		c.Assert(arg, Equals, expectArgs[i])
	}

	// add a new bound
	newChunk = chunk.copy()
	newChunk.update("c", "7", ">", "8", "<")
	conditions, args = newChunk.toString("normal", "")
	c.Assert(conditions, Equals, "`a` > ? AND `a` < ? AND `b` > ? AND `b` < ? AND `c` > ? AND `c` < ?")
	expectArgs = []string{"1", "2", "3", "4", "7", "8"}
	for i, arg := range args {
		c.Assert(arg, Equals, expectArgs[i])
	}

	// the origin chunk is not changed
	conditions, args = chunk.toString("normal", "")
	c.Assert(conditions, Equals, "`a` > ? AND `a` < ? AND `b` > ? AND `b` < ?")
	expectArgs = []string{"1", "2", "3", "4"}
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

	conditions, args := chunk.toString("normal", "")
	c.Assert(conditions, Equals, "`a` > ? AND `a` < ? AND `b` > ? AND `b` < ? AND `c` > ? AND `c` < ?")
	expectArgs := []string{"1", "2", "3", "4", "5", "6"}
	for i, arg := range args {
		c.Assert(arg, Equals, expectArgs[i])
	}

	conditions, args = chunk.toString("normal", "latin1")
	c.Assert(conditions, Equals, "`a` COLLATE 'latin1' > ? AND `a` COLLATE 'latin1' < ? AND `b` COLLATE 'latin1' > ? AND `b` COLLATE 'latin1' < ? AND `c` COLLATE 'latin1' > ? AND `c` COLLATE 'latin1' < ?")
	expectArgs = []string{"1", "2", "3", "4", "5", "6"}
	for i, arg := range args {
		c.Assert(arg, Equals, expectArgs[i])
	}

	conditions, args = chunk.toString("bucket", "")
	c.Assert(conditions, Equals, "((`a` > ?) OR (`a` = ? AND `b` > ?) OR (`a` = ? AND `b` = ? AND `c` > ?)) AND ((`a` < ?) OR (`a` = ? AND `b` < ?) OR (`a` = ? AND `b` = ? AND `c` < ?))")
	expectArgs = []string{"1", "1", "3", "1", "3", "5", "2", "2", "4", "2", "4", "6"}
	for i, arg := range args {
		c.Assert(arg, Equals, expectArgs[i])
	}

	conditions, args = chunk.toString("bucket", "latin1")
	c.Assert(conditions, Equals, "((`a` COLLATE 'latin1' > ?) OR (`a` = ? AND `b` COLLATE 'latin1' > ?) OR (`a` = ? AND `b` = ? AND `c` COLLATE 'latin1' > ?)) AND ((`a` COLLATE 'latin1' < ?) OR (`a` = ? AND `b` COLLATE 'latin1' < ?) OR (`a` = ? AND `b` = ? AND `c` COLLATE 'latin1' < ?))")
	expectArgs = []string{"1", "1", "3", "1", "3", "5", "2", "2", "4", "2", "4", "6"}
	for i, arg := range args {
		c.Assert(arg, Equals, expectArgs[i])
	}

}
