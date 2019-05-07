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
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb-tools/pkg/dbutil"
	"github.com/pingcap/tidb-tools/pkg/importer"
)

var _ = Suite(&testChunkSuite{})

type testChunkSuite struct{}

type chunkTestCase struct {
	chunk        *ChunkRange
	chunkCnt     int64
	expectChunks []*ChunkRange
}

func (*testChunkSuite) TestSplitRange(c *C) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conns, err := createConns(ctx)
	c.Assert(err, IsNil)
	defer conns.Close()

	_, err = conns.DB.ExecContext(ctx, "CREATE DATABASE IF NOT EXISTS `test`")
	c.Assert(err, IsNil)

	_, err = conns.DB.ExecContext(ctx, "DROP TABLE IF EXISTS `test`.`testa`")
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
	defer conns.DB.ExecContext(ctx, "DROP TABLE IF EXISTS `test`.`testa`")

	// only work on tidb, so don't assert err here
	_, _ = conns.DB.ExecContext(ctx, "ANALYZE TABLE `test`.`testa`")

	tableInfo, err := dbutil.GetTableInfoWithRowID(ctx, conns.DB, "test", "testa", false)
	c.Assert(err, IsNil)

	tableInstance := &TableInstance{
		Conns:  conns,
		Schema: "test",
		Table:  "testa",
		info:   tableInfo,
	}

	// split chunks
	fields, err := getSplitFields(tableInstance.info, nil)
	c.Assert(err, IsNil)
	chunks, err := getChunksForTable(tableInstance, fields, 100, "TRUE", "", false)
	c.Assert(err, IsNil)

	// get data count from every chunk, and the sum of them should equal to the table's count.
	chunkDataCount := 0
	for _, chunk := range chunks {
		conditions, args := chunk.toString("")
		count, err := dbutil.GetRowCount(ctx, tableInstance.Conns.DB, tableInstance.Schema, tableInstance.Table, dbutil.ReplacePlaceholder(conditions, args))
		c.Assert(err, IsNil)
		chunkDataCount += int(count)
	}
	c.Assert(chunkDataCount, Equals, dataCount)
}

func (*testChunkSuite) TestChunkUpdate(c *C) {
	chunk := &ChunkRange{
		Bounds: []*Bound{
			{
				Column:      "a",
				Lower:       "1",
				LowerSymbol: ">",
				Upper:       "2",
				UpperSymbol: "<=",
			}, {
				Column:      "b",
				Lower:       "3",
				LowerSymbol: ">=",
				Upper:       "4",
				UpperSymbol: "<",
			},
		},
		Mode: normalMode,
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
		conditions, args := newChunk.toString("")
		c.Assert(conditions, Equals, cs.expectStr)
		c.Assert(args, DeepEquals, cs.expectArgs)
	}

	// the origin chunk is not changed
	conditions, args := chunk.toString("")
	c.Assert(conditions, Equals, "`a` > ? AND `a` <= ? AND `b` >= ? AND `b` < ?")
	expectArgs := []string{"1", "2", "3", "4"}
	for i, arg := range args {
		c.Assert(arg, Equals, expectArgs[i])
	}
}

func (*testChunkSuite) TestChunkToString(c *C) {
	chunk := &ChunkRange{
		Bounds: []*Bound{
			{
				Column:      "a",
				Lower:       "1",
				LowerSymbol: ">",
				Upper:       "2",
				UpperSymbol: "<",
			}, {
				Column:      "b",
				Lower:       "3",
				LowerSymbol: ">",
				Upper:       "4",
				UpperSymbol: "<",
			}, {
				Column:      "c",
				Lower:       "5",
				LowerSymbol: ">",
				Upper:       "6",
				UpperSymbol: "<",
			},
		},
		Mode: normalMode,
	}

	conditions, args := chunk.toString("")
	c.Assert(conditions, Equals, "`a` > ? AND `a` < ? AND `b` > ? AND `b` < ? AND `c` > ? AND `c` < ?")
	expectArgs := []string{"1", "2", "3", "4", "5", "6"}
	for i, arg := range args {
		c.Assert(arg, Equals, expectArgs[i])
	}

	conditions, args = chunk.toString("latin1")
	c.Assert(conditions, Equals, "`a` COLLATE 'latin1' > ? AND `a` COLLATE 'latin1' < ? AND `b` COLLATE 'latin1' > ? AND `b` COLLATE 'latin1' < ? AND `c` COLLATE 'latin1' > ? AND `c` COLLATE 'latin1' < ?")
	expectArgs = []string{"1", "2", "3", "4", "5", "6"}
	for i, arg := range args {
		c.Assert(arg, Equals, expectArgs[i])
	}

	chunk.Mode = bucketMode
	conditions, args = chunk.toString("")
	c.Assert(conditions, Equals, "((`a` > ?) OR (`a` = ? AND `b` > ?) OR (`a` = ? AND `b` = ? AND `c` > ?)) AND ((`a` < ?) OR (`a` = ? AND `b` < ?) OR (`a` = ? AND `b` = ? AND `c` < ?))")
	expectArgs = []string{"1", "1", "3", "1", "3", "5", "2", "2", "4", "2", "4", "6"}
	for i, arg := range args {
		c.Assert(arg, Equals, expectArgs[i])
	}

	conditions, args = chunk.toString("latin1")
	c.Assert(conditions, Equals, "((`a` COLLATE 'latin1' > ?) OR (`a` = ? AND `b` COLLATE 'latin1' > ?) OR (`a` = ? AND `b` = ? AND `c` COLLATE 'latin1' > ?)) AND ((`a` COLLATE 'latin1' < ?) OR (`a` = ? AND `b` COLLATE 'latin1' < ?) OR (`a` = ? AND `b` = ? AND `c` COLLATE 'latin1' < ?))")
	expectArgs = []string{"1", "1", "3", "1", "3", "5", "2", "2", "4", "2", "4", "6"}
	for i, arg := range args {
		c.Assert(arg, Equals, expectArgs[i])
	}
}
