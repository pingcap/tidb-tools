// Copyright 2021 PingCAP, Inc.
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

package source

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	. "github.com/pingcap/check"
	"github.com/pingcap/parser"
	"github.com/pingcap/tidb-tools/pkg/dbutil"
	"github.com/pingcap/tidb-tools/sync_diff_inspector/chunk"
	"github.com/pingcap/tidb-tools/sync_diff_inspector/source/common"
	"github.com/pingcap/tidb-tools/sync_diff_inspector/splitter"
)

func TestClient(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testSourceSuite{})

type testSourceSuite struct{}

type tableCaseType struct {
	schema         string
	table          string
	createTableSQL string
	rangeColumns   []string
	rangeLeft      []string
	rangeRight     []string
	rangeInfo      *splitter.RangeInfo
}

func (s *testSourceSuite) TestTiDBSource(c *C) {
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Second)
	defer cancel()

	conn, err := dbutil.OpenDB(dbutil.GetDBConfigFromEnv(""), nil)
	c.Assert(err, IsNil)
	defer conn.Close()

	_, err = conn.ExecContext(ctx, "DROP DATABASE IF EXISTS `source_test`")
	c.Assert(err, IsNil)
	_, err = conn.ExecContext(ctx, "CREATE DATABASE IF EXISTS `source_test`")
	c.Assert(err, IsNil)

	tableCases := []tableCaseType{
		{
			schema:         "source_test",
			table:          "test1",
			createTableSQL: "CREATE TABLE `source_test`.`test1` (`a` int, `b` varchar(24), `c` float, `d` datetime, primary key(`a`, `b`))",
			rangeColumns:   []string{"a", "b"},
			rangeLeft:      []string{"3", "b"},
			rangeRight:     []string{"5", "f"},
		},
	}

	tableDiffs := prepareTiDBTables(c, ctx, conn, tableCases)

	source, err := NewTiDBSource(ctx, tableDiffs, conn)

	for _, tableCase := range tableCases {
		checksumInfo := make(chan *ChecksumInfo)
		source.GetCountAndCrc32(tableCase.rangeInfo, nil, checksumInfo)
		_ = <-checksumInfo
		//c.Assert(checksum, Equals, tableCase.checksum)
	}

	c.Assert(err, IsNil)

}

func prepareTiDBTables(c *C, ctx context.Context, conn *sql.DB, tableCases []tableCaseType) []*common.TableDiff {
	tableDiffs := make([]*common.TableDiff, 0, len(tableCases))
	for n, tableCase := range tableCases {
		tableInfo, err := dbutil.GetTableInfoBySQL(tableCase.createTableSQL, parser.New())
		c.Assert(err, IsNil)
		tableDiffs = append(tableDiffs, &common.TableDiff{
			Schema: "source_test",
			Table:  fmt.Sprintf("test%d", n),
			Info:   tableInfo,
		})
		_, err = conn.ExecContext(ctx, tableCase.createTableSQL)

		chunkRange := chunk.NewChunkRange()
		for i, column := range tableCase.rangeColumns {
			chunkRange.Update(column, tableCase.rangeLeft[i], tableCase.rangeRight[i], true, true)
		}

		chunk.InitChunks([]*chunk.Range{chunkRange}, chunk.Others, 0, 0, "", "")
		rangeInfo := &splitter.RangeInfo{
			ChunkRange: chunkRange,
			TableIndex: 0,
		}
		tableCase.rangeInfo = rangeInfo
	}

	return tableDiffs
}
