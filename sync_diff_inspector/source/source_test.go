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
	"database/sql/driver"
	"fmt"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
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
	rowQuery       string
	rowColumns     []string
	rows           [][]driver.Value
}

type MockChunkIterator struct {
	ctx       context.Context
	tableDiff *common.TableDiff
	rangeInfo *splitter.RangeInfo
	i         int
}

const MAXCHUNKS = 5

func (m *MockChunkIterator) Next() (*chunk.Range, error) {
	if m.i == MAXCHUNKS {
		return nil, nil
	}
	m.i = m.i + 1
	return &chunk.Range{
		ID: m.i,
	}, nil
}

func (m *MockChunkIterator) Close() {

}

type MockAnalyzer struct {
}

func (m *MockAnalyzer) AnalyzeSplitter(ctx context.Context, tableDiff *common.TableDiff, rangeInfo *splitter.RangeInfo) (splitter.ChunkIterator, error) {
	i := 0
	return &MockChunkIterator{
		ctx,
		tableDiff,
		rangeInfo,
		i,
	}, nil
}

func (s *testSourceSuite) TestBasicSource(c *C) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conn, mock, err := sqlmock.New()
	c.Assert(err, IsNil)
	defer conn.Close()

	tableCases := []*tableCaseType{
		{
			schema:         "source_test",
			table:          "test1",
			createTableSQL: "CREATE TABLE `source_test`.`test1` (`a` int, `b` varchar(24), `c` float, `d` datetime, primary key(`a`, `b`))",
			rangeColumns:   []string{"a", "b"},
			rangeLeft:      []string{"3", "b"},
			rangeRight:     []string{"5", "f"},
			rowQuery:       "SELECT a, b",
			rowColumns:     []string{"a", "b"},
			rows: [][]driver.Value{
				{"1", "a"},
				{"2", "b"},
				{"3", "c"},
				{"4", "d"},
			},
		},
		{
			schema:         "source_test",
			table:          "test2",
			createTableSQL: "CREATE TABLE `source_test`.`test2` (`a` int, `b` varchar(24), `c` float, `d` datetime, primary key(`a`, `b`))",
			rangeColumns:   []string{"a", "b"},
			rangeLeft:      []string{"3", "b"},
			rangeRight:     []string{"5", "f"},
		},
	}

	tableDiffs, tableRows := prepareTiDBTables(c, ctx, conn, tableCases)

	basic := &BasicSource{
		tableDiffs: tableDiffs,
		tableRows:  tableRows,
		dbConn:     conn,
		ctx:        ctx,
	}

	for n, tableCase := range tableCases {
		c.Assert(n, Equals, tableCase.rangeInfo.TableIndex)
		countRows := sqlmock.NewRows([]string{"CNT", "CHECKSUM"}).AddRow(123, 456)
		mock.ExpectQuery("SELECT COUNT.*").WillReturnRows(countRows)
		checksumInfo := make(chan *ChecksumInfo, 1)
		go basic.GetCountAndCrc32(tableCase.rangeInfo, nil, checksumInfo)
		checksum := <-checksumInfo
		c.Assert(checksum.Err, IsNil)
		//c.Assert(checksum, Equals, tableCase.checksum)
	}

	// Test ChunkIterator
	iter, err := basic.GetRangeIterator(nil, &MockAnalyzer{})
	c.Assert(err, IsNil)
	i := 0
	for {
		chunk, err := iter.Next()
		c.Assert(err, IsNil)
		if chunk == nil {
			c.Assert(i, Equals, 5*len(tableCases))
			break
		}
		c.Assert(chunk.ChunkRange.ID, Equals, (i%5)+1)
		i++
	}

	// Test RowIterator
	for _, tableCase := range tableCases {
		dataRows := sqlmock.NewRows(tableCase.rowColumns)
		for _, row := range tableCase.rows {
			dataRows.AddRow(row...)
		}
		mock.ExpectQuery(tableCase.rowQuery).WillReturnRows(dataRows)
		rowIter, err := basic.GetRowsIterator(tableCase.rangeInfo)
		c.Assert(err, IsNil)

		i := 0
		for {
			columns, err := rowIter.Next()
			c.Assert(err, IsNil)
			if columns == nil {
				c.Assert(i, Equals, len(tableCase.rows))
				break
			}
			for j, value := range tableCase.rows[i] {
				c.Assert(columns[tableCase.rowColumns[j]].IsNull, Equals, false)
				c.Assert(columns[tableCase.rowColumns[j]].Data, DeepEquals, []byte(value.(string)))
			}

			i++
		}
		rowIter.Close()
	}

}

func prepareTiDBTables(c *C, ctx context.Context, conn *sql.DB, tableCases []*tableCaseType) ([]*common.TableDiff, []*TableRows) {
	tableDiffs := make([]*common.TableDiff, 0, len(tableCases))
	tableRows := make([]*TableRows, 0, len(tableCases))
	for n, tableCase := range tableCases {
		tableInfo, err := dbutil.GetTableInfoBySQL(tableCase.createTableSQL, parser.New())
		c.Assert(err, IsNil)
		tableDiffs = append(tableDiffs, &common.TableDiff{
			Schema: "source_test",
			Table:  fmt.Sprintf("test%d", n),
			Info:   tableInfo,
		})
		tableRows = append(tableRows, &TableRows{
			tableRowsQuery: tableCase.rowQuery,
		})

		chunkRange := chunk.NewChunkRange()
		for i, column := range tableCase.rangeColumns {
			chunkRange.Update(column, tableCase.rangeLeft[i], tableCase.rangeRight[i], true, true)
		}

		chunk.InitChunks([]*chunk.Range{chunkRange}, chunk.Others, 0, 0, "", "")
		rangeInfo := &splitter.RangeInfo{
			ChunkRange: chunkRange,
			TableIndex: n,
		}
		tableCase.rangeInfo = rangeInfo
	}

	return tableDiffs, tableRows
}
