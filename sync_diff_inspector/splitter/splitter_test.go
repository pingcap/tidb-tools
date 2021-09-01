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

package splitter

import (
	"context"
	"fmt"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	. "github.com/pingcap/check"
	"github.com/pingcap/parser"
	"github.com/pingcap/tidb-tools/pkg/dbutil"
	"github.com/pingcap/tidb-tools/sync_diff_inspector/chunk"
	"github.com/pingcap/tidb-tools/sync_diff_inspector/source/common"
	"github.com/pingcap/tidb-tools/sync_diff_inspector/utils"
)

func TestClient(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testSplitterSuite{})

type testSplitterSuite struct{}

type chunkResult struct {
	chunkStr string
	args     []string
}

func (s *testSplitterSuite) TestSplitRangeByRandom(c *C) {
	db, mock, err := sqlmock.New()
	c.Assert(err, IsNil)

	testCases := []struct {
		createTableSQL string
		splitCount     int
		originChunk    *chunk.Range
		randomValues   [][]interface{}
		expectResult   []chunkResult
	}{
		{
			"create table `test`.`test`(`a` int, `b` varchar(10), `c` float, `d` datetime, primary key(`a`, `b`))",
			3,
			chunk.NewChunkRange().CopyAndUpdate("a", "0", "10", true, true).CopyAndUpdate("b", "a", "z", true, true),
			[][]interface{}{
				{5, 7},
				{"g", "n"},
			},
			[]chunkResult{
				{
					"((`a` > ?) OR (`a` = ? AND `b` > ?)) AND ((`a` < ?) OR (`a` = ? AND `b` <= ?))",
					[]string{"0", "0", "a", "5", "5", "g"},
				}, {
					"((`a` > ?) OR (`a` = ? AND `b` > ?)) AND ((`a` < ?) OR (`a` = ? AND `b` <= ?))",
					[]string{"5", "5", "g", "7", "7", "n"},
				}, {
					"((`a` > ?) OR (`a` = ? AND `b` > ?)) AND ((`a` < ?) OR (`a` = ? AND `b` <= ?))",
					[]string{"7", "7", "n", "10", "10", "z"},
				},
			},
		}, {
			"create table `test`.`test`(`a` int, `b` varchar(10), `c` float, `d` datetime, primary key(`b`))",
			3,
			chunk.NewChunkRange().CopyAndUpdate("b", "a", "z", true, true),
			[][]interface{}{
				{"g", "n"},
			},
			[]chunkResult{
				{
					"((`b` > ?)) AND ((`b` <= ?))",
					[]string{"a", "g"},
				}, {
					"((`b` > ?)) AND ((`b` <= ?))",
					[]string{"g", "n"},
				}, {
					"((`b` > ?)) AND ((`b` <= ?))",
					[]string{"n", "z"},
				},
			},
		}, {
			"create table `test`.`test`(`a` int, `b` varchar(10), `c` float, `d` datetime, primary key(`b`))",
			2,
			chunk.NewChunkRange().CopyAndUpdate("b", "a", "z", true, true),
			[][]interface{}{
				{"g"},
			},
			[]chunkResult{
				{
					"((`b` > ?)) AND ((`b` <= ?))",
					[]string{"a", "g"},
				}, {
					"((`b` > ?)) AND ((`b` <= ?))",
					[]string{"g", "z"},
				},
			},
		}, {
			"create table `test`.`test`(`a` int, `b` varchar(10), `c` float, `d` datetime, primary key(`b`))",
			3,
			chunk.NewChunkRange().CopyAndUpdate("b", "a", "z", true, true),
			[][]interface{}{
				{},
			},
			[]chunkResult{
				{
					"((`b` > ?)) AND ((`b` <= ?))",
					[]string{"a", "z"},
				},
			},
		},
	}

	for i, testCase := range testCases {
		tableInfo, err := dbutil.GetTableInfoBySQL(testCase.createTableSQL, parser.New())
		c.Assert(err, IsNil)

		splitCols, err := GetSplitFields(tableInfo, nil)
		c.Assert(err, IsNil)
		createFakeResultForRandomSplit(mock, 0, testCase.randomValues)

		chunks, err := splitRangeByRandom(db, testCase.originChunk, testCase.splitCount, "test", "test", splitCols, "", "")
		c.Assert(err, IsNil)
		for j, chunk := range chunks {
			chunkStr, args := chunk.ToString("")
			c.Log(i, j, chunkStr, args)
			c.Assert(chunkStr, Equals, testCase.expectResult[j].chunkStr)
			c.Assert(args, DeepEquals, testCase.expectResult[j].args)
		}
	}
}

func (s *testSplitterSuite) TestRandomSpliter(c *C) {
	ctx := context.Background()
	db, mock, err := sqlmock.New()
	c.Assert(err, IsNil)

	testCases := []struct {
		createTableSQL string
		count          int
		fields         string
		IgnoreColumns  []string
		randomValues   [][]interface{}
		expectResult   []chunkResult
	}{
		{
			"create table `test`.`test`(`a` int, `b` varchar(10), `c` float, `d` datetime, primary key(`a`, `b`))",
			10,
			"",
			nil,
			[][]interface{}{
				{1, 2, 3, 4, 5},
				{"a", "b", "c", "d", "e"},
			},
			[]chunkResult{
				{
					"(`a` < ?) OR (`a` = ? AND `b` <= ?)",
					[]string{"1", "1", "a"},
				}, {
					"((`a` > ?) OR (`a` = ? AND `b` > ?)) AND ((`a` < ?) OR (`a` = ? AND `b` <= ?))",
					[]string{"1", "1", "a", "2", "2", "b"},
				}, {
					"((`a` > ?) OR (`a` = ? AND `b` > ?)) AND ((`a` < ?) OR (`a` = ? AND `b` <= ?))",
					[]string{"2", "2", "b", "3", "3", "c"},
				}, {
					"((`a` > ?) OR (`a` = ? AND `b` > ?)) AND ((`a` < ?) OR (`a` = ? AND `b` <= ?))",
					[]string{"3", "3", "c", "4", "4", "d"},
				}, {
					"((`a` > ?) OR (`a` = ? AND `b` > ?)) AND ((`a` < ?) OR (`a` = ? AND `b` <= ?))",
					[]string{"4", "4", "d", "5", "5", "e"},
				}, {
					"(`a` > ?) OR (`a` = ? AND `b` > ?)",
					[]string{"5", "5", "e"},
				},
			},
		}, {
			"create table `test`.`test`(`a` int, `b` varchar(10), `c` float, `d` datetime, primary key(`b`))",
			10,
			"",
			nil,
			[][]interface{}{
				{"a", "b", "c", "d", "e"},
			},
			[]chunkResult{
				{
					"(`b` <= ?)",
					[]string{"a"},
				}, {
					"((`b` > ?)) AND ((`b` <= ?))",
					[]string{"a", "b"},
				}, {
					"((`b` > ?)) AND ((`b` <= ?))",
					[]string{"b", "c"},
				}, {
					"((`b` > ?)) AND ((`b` <= ?))",
					[]string{"c", "d"},
				}, {
					"((`b` > ?)) AND ((`b` <= ?))",
					[]string{"d", "e"},
				}, {
					"(`b` > ?)",
					[]string{"e"},
				},
			},
		}, {
			"create table `test`.`test`(`a` int, `b` varchar(10), `c` float)",
			10,
			"b,c",
			nil,
			[][]interface{}{
				{"a", "b", "c", "d", "e"},
				{1.1, 2.2, 3.3, 4.4, 5.5},
			},
			[]chunkResult{
				{
					"(`b` < ?) OR (`b` = ? AND `c` <= ?)",
					[]string{"a", "a", "1.1"},
				}, {
					"((`b` > ?) OR (`b` = ? AND `c` > ?)) AND ((`b` < ?) OR (`b` = ? AND `c` <= ?))",
					[]string{"a", "a", "1.1", "b", "b", "2.2"},
				}, {
					"((`b` > ?) OR (`b` = ? AND `c` > ?)) AND ((`b` < ?) OR (`b` = ? AND `c` <= ?))",
					[]string{"b", "b", "2.2", "c", "c", "3.3"},
				}, {
					"((`b` > ?) OR (`b` = ? AND `c` > ?)) AND ((`b` < ?) OR (`b` = ? AND `c` <= ?))",
					[]string{"c", "c", "3.3", "d", "d", "4.4"},
				}, {
					"((`b` > ?) OR (`b` = ? AND `c` > ?)) AND ((`b` < ?) OR (`b` = ? AND `c` <= ?))",
					[]string{"d", "d", "4.4", "e", "e", "5.5"},
				}, {
					"(`b` > ?) OR (`b` = ? AND `c` > ?)",
					[]string{"e", "e", "5.5"},
				},
			},
		}, {
			"create table `test`.`test`(`a` int, `b` varchar(10), `c` float)",
			10,
			"",
			[]string{"a"},
			[][]interface{}{
				{"a", "b", "c", "d", "e"},
			},
			[]chunkResult{
				{
					"(`b` <= ?)",
					[]string{"a"},
				}, {
					"((`b` > ?)) AND ((`b` <= ?))",
					[]string{"a", "b"},
				}, {
					"((`b` > ?)) AND ((`b` <= ?))",
					[]string{"b", "c"},
				}, {
					"((`b` > ?)) AND ((`b` <= ?))",
					[]string{"c", "d"},
				}, {
					"((`b` > ?)) AND ((`b` <= ?))",
					[]string{"d", "e"},
				}, {
					"(`b` > ?)",
					[]string{"e"},
				},
			},
		}, {
			"create table `test`.`test`(`a` int, `b` varchar(10), `c` float)",
			10,
			"",
			nil,
			[][]interface{}{
				{1, 2, 3, 4, 5},
			},
			[]chunkResult{
				{
					"(`a` <= ?)",
					[]string{"1"},
				}, {
					"((`a` > ?)) AND ((`a` <= ?))",
					[]string{"1", "2"},
				}, {
					"((`a` > ?)) AND ((`a` <= ?))",
					[]string{"2", "3"},
				}, {
					"((`a` > ?)) AND ((`a` <= ?))",
					[]string{"3", "4"},
				}, {
					"((`a` > ?)) AND ((`a` <= ?))",
					[]string{"4", "5"},
				}, {
					"(`a` > ?)",
					[]string{"5"},
				},
			},
		},
	}

	for i, testCase := range testCases {
		tableInfo, err := dbutil.GetTableInfoBySQL(testCase.createTableSQL, parser.New())
		c.Assert(err, IsNil)

		tableDiff := &common.TableDiff{
			Schema:        "test",
			Table:         "test",
			Info:          utils.IgnoreColumns(tableInfo, testCase.IgnoreColumns),
			IgnoreColumns: testCase.IgnoreColumns,
			Fields:        testCase.fields,
			ChunkSize:     5,
		}

		createFakeResultForRandomSplit(mock, testCase.count, testCase.randomValues)

		iter, err := NewRandomIterator(ctx, "", tableDiff, db)
		c.Assert(err, IsNil)

		j := 0
		for {
			chunk, err := iter.Next()
			c.Assert(err, IsNil)
			if chunk == nil {
				break
			}
			chunkStr, args := chunk.ToString("")
			c.Log(i, j, chunkStr, args)
			c.Assert(chunkStr, Equals, testCase.expectResult[j].chunkStr)
			c.Assert(args, DeepEquals, testCase.expectResult[j].args)
			j = j + 1
		}
	}

	// Test Checkpoint
	stopJ := 3
	tableInfo, err := dbutil.GetTableInfoBySQL(testCases[0].createTableSQL, parser.New())
	c.Assert(err, IsNil)

	tableDiff := &common.TableDiff{
		Schema: "test",
		Table:  "test",
		Info:   tableInfo,
		//IgnoreColumns: []string{"c"},
		//Fields:        "a,b",
		ChunkSize: 5,
	}

	createFakeResultForRandomSplit(mock, testCases[0].count, testCases[0].randomValues)

	iter, err := NewRandomIterator(ctx, "", tableDiff, db)
	c.Assert(err, IsNil)

	var chunk *chunk.Range
	for j := 0; j < stopJ; j++ {
		chunk, err = iter.Next()
		c.Assert(err, IsNil)
	}

	bounds1 := chunk.Bounds

	rangeInfo := &RangeInfo{
		ChunkRange: chunk,
	}

	createFakeResultForRandomSplit(mock, testCases[0].count, testCases[0].randomValues)

	iter, err = NewRandomIteratorWithCheckpoint(ctx, "", tableDiff, db, rangeInfo)
	c.Assert(err, IsNil)

	chunk, err = iter.Next()
	c.Assert(err, IsNil)

	for i, bound := range chunk.Bounds {
		c.Assert(bounds1[i].Upper, DeepEquals, bound.Lower)
	}

}

func createFakeResultForRandomSplit(mock sqlmock.Sqlmock, count int, randomValues [][]interface{}) {
	createFakeResultForCount(mock, count)

	// generate fake result for get random value for column a
	for _, randomVs := range randomValues {
		randomRows := sqlmock.NewRows([]string{"a"})
		for _, value := range randomVs {
			randomRows.AddRow(value)
		}
		mock.ExpectQuery("ORDER BY rand_value").WillReturnRows(randomRows)
	}
}

func (s *testSplitterSuite) TestBucketSpliter(c *C) {
	ctx := context.Background()
	db, mock, err := sqlmock.New()
	c.Assert(err, IsNil)

	createTableSQL := "create table `test`.`test`(`a` int, `b` varchar(10), `c` float, `d` datetime, primary key(`a`, `b`))"
	tableInfo, err := dbutil.GetTableInfoBySQL(createTableSQL, parser.New())
	c.Assert(err, IsNil)

	testCases := []struct {
		chunkSize     int64
		aRandomValues []interface{}
		bRandomValues []interface{}
		expectResult  []chunkResult
	}{
		{
			// chunk size less than the count of bucket 64, and the bucket's count 64 >= 32, so will split by random in every bucket
			32,
			[]interface{}{32, 32 * 3, 32 * 5, 32 * 7, 32 * 9},
			[]interface{}{6, 6 * 3, 6 * 5, 6 * 7, 6 * 9},
			[]chunkResult{
				{
					"(`a` < ?) OR (`a` = ? AND `b` <= ?)",
					[]string{"32", "32", "6"},
				}, {
					"((`a` > ?) OR (`a` = ? AND `b` > ?)) AND ((`a` < ?) OR (`a` = ? AND `b` <= ?))",
					[]string{"32", "32", "6", "63", "63", "11"},
				}, {
					"((`a` > ?) OR (`a` = ? AND `b` > ?)) AND ((`a` < ?) OR (`a` = ? AND `b` <= ?))",
					[]string{"63", "63", "11", "96", "96", "18"},
				}, {
					"((`a` > ?) OR (`a` = ? AND `b` > ?)) AND ((`a` < ?) OR (`a` = ? AND `b` <= ?))",
					[]string{"96", "96", "18", "127", "127", "23"},
				}, {
					"((`a` > ?) OR (`a` = ? AND `b` > ?)) AND ((`a` < ?) OR (`a` = ? AND `b` <= ?))",
					[]string{"127", "127", "23", "160", "160", "30"},
				}, {
					"((`a` > ?) OR (`a` = ? AND `b` > ?)) AND ((`a` < ?) OR (`a` = ? AND `b` <= ?))",
					[]string{"160", "160", "30", "191", "191", "35"},
				}, {
					"((`a` > ?) OR (`a` = ? AND `b` > ?)) AND ((`a` < ?) OR (`a` = ? AND `b` <= ?))",
					[]string{"191", "191", "35", "224", "224", "42"},
				}, {
					"((`a` > ?) OR (`a` = ? AND `b` > ?)) AND ((`a` < ?) OR (`a` = ? AND `b` <= ?))",
					[]string{"224", "224", "42", "255", "255", "47"},
				}, {
					"((`a` > ?) OR (`a` = ? AND `b` > ?)) AND ((`a` < ?) OR (`a` = ? AND `b` <= ?))",
					[]string{"255", "255", "47", "288", "288", "54"},
				}, {
					"((`a` > ?) OR (`a` = ? AND `b` > ?)) AND ((`a` < ?) OR (`a` = ? AND `b` <= ?))",
					[]string{"288", "288", "54", "319", "319", "59"},
				}, {
					"(`a` > ?) OR (`a` = ? AND `b` > ?)",
					[]string{"319", "319", "59"},
				},
			},
		}, {
			// chunk size less than the count of bucket 64, but 64 is  less than 2*50, so will not split every bucket
			50,
			nil,
			nil,
			[]chunkResult{
				{
					"(`a` < ?) OR (`a` = ? AND `b` <= ?)",
					[]string{"63", "63", "11"},
				}, {
					"((`a` > ?) OR (`a` = ? AND `b` > ?)) AND ((`a` < ?) OR (`a` = ? AND `b` <= ?))",
					[]string{"63", "63", "11", "127", "127", "23"},
				}, {
					"((`a` > ?) OR (`a` = ? AND `b` > ?)) AND ((`a` < ?) OR (`a` = ? AND `b` <= ?))",
					[]string{"127", "127", "23", "191", "191", "35"},
				}, {
					"((`a` > ?) OR (`a` = ? AND `b` > ?)) AND ((`a` < ?) OR (`a` = ? AND `b` <= ?))",
					[]string{"191", "191", "35", "255", "255", "47"},
				}, {
					"((`a` > ?) OR (`a` = ? AND `b` > ?)) AND ((`a` < ?) OR (`a` = ? AND `b` <= ?))",
					[]string{"255", "255", "47", "319", "319", "59"},
				}, {
					"(`a` > ?) OR (`a` = ? AND `b` > ?)",
					[]string{"319", "319", "59"},
				},
			},
		}, {
			// chunk size is equal to the count of bucket 64, so every becket will generate a chunk
			64,
			nil,
			nil,
			[]chunkResult{
				{
					"(`a` < ?) OR (`a` = ? AND `b` <= ?)",
					[]string{"63", "63", "11"},
				}, {
					"((`a` > ?) OR (`a` = ? AND `b` > ?)) AND ((`a` < ?) OR (`a` = ? AND `b` <= ?))",
					[]string{"63", "63", "11", "127", "127", "23"},
				}, {
					"((`a` > ?) OR (`a` = ? AND `b` > ?)) AND ((`a` < ?) OR (`a` = ? AND `b` <= ?))",
					[]string{"127", "127", "23", "191", "191", "35"},
				}, {
					"((`a` > ?) OR (`a` = ? AND `b` > ?)) AND ((`a` < ?) OR (`a` = ? AND `b` <= ?))",
					[]string{"191", "191", "35", "255", "255", "47"},
				}, {
					"((`a` > ?) OR (`a` = ? AND `b` > ?)) AND ((`a` < ?) OR (`a` = ? AND `b` <= ?))",
					[]string{"255", "255", "47", "319", "319", "59"},
				}, {
					"(`a` > ?) OR (`a` = ? AND `b` > ?)",
					[]string{"319", "319", "59"},
				},
			},
		}, {
			// chunk size is greater than the count of bucket 64, will combine two bucket into chunk
			127,
			nil,
			nil,
			[]chunkResult{
				{
					"(`a` < ?) OR (`a` = ? AND `b` <= ?)",
					[]string{"127", "127", "23"},
				}, {
					"((`a` > ?) OR (`a` = ? AND `b` > ?)) AND ((`a` < ?) OR (`a` = ? AND `b` <= ?))",
					[]string{"127", "127", "23", "255", "255", "47"},
				}, {
					"(`a` > ?) OR (`a` = ? AND `b` > ?)",
					[]string{"255", "255", "47"},
				},
			},
		}, {
			// chunk size is equal to the double count of bucket 64, will combine two bucket into one chunk
			128,
			nil,
			nil,
			[]chunkResult{
				{
					"(`a` < ?) OR (`a` = ? AND `b` <= ?)",
					[]string{"127", "127", "23"},
				}, {
					"((`a` > ?) OR (`a` = ? AND `b` > ?)) AND ((`a` < ?) OR (`a` = ? AND `b` <= ?))",
					[]string{"127", "127", "23", "255", "255", "47"},
				}, {
					"(`a` > ?) OR (`a` = ? AND `b` > ?)",
					[]string{"255", "255", "47"},
				},
			},
		}, {
			// chunk size is greate than the double count of bucket 64, will combine three bucket into one chunk
			129,
			nil,
			nil,
			[]chunkResult{
				{
					"(`a` < ?) OR (`a` = ? AND `b` <= ?)",
					[]string{"191", "191", "35"},
				}, {
					"(`a` > ?) OR (`a` = ? AND `b` > ?)",
					[]string{"191", "191", "35"},
				},
			},
		}, {
			// chunk size is greater than the total count, only generate one chunk
			400,
			nil,
			nil,
			[]chunkResult{
				{
					"TRUE",
					nil,
				},
			},
		},
	}

	tableDiff := &common.TableDiff{
		Schema: "test",
		Table:  "test",
		Info:   tableInfo,
	}

	for i, testCase := range testCases {
		createFakeResultForBucketSplit(mock, testCase.aRandomValues, testCase.bRandomValues)
		tableDiff.ChunkSize = testCase.chunkSize
		iter, err := NewBucketIterator(ctx, "", tableDiff, db)
		c.Assert(err, IsNil)

		j := 0
		for {
			chunk, err := iter.Next()
			c.Assert(err, IsNil)
			if chunk == nil {
				break
			}
			chunkStr, args := chunk.ToString("")
			c.Log(i, j, chunkStr, args)
			c.Assert(chunkStr, Equals, testCase.expectResult[j].chunkStr)
			c.Assert(args, DeepEquals, testCase.expectResult[j].args)
			j = j + 1
		}
	}

	// Test Checkpoint
	stopJ := 3
	createFakeResultForBucketSplit(mock, testCases[0].aRandomValues[:stopJ], testCases[0].bRandomValues[:stopJ])
	tableDiff.ChunkSize = testCases[0].chunkSize
	iter, err := NewBucketIterator(ctx, "", tableDiff, db)
	c.Assert(err, IsNil)
	j := 0
	var chunk *chunk.Range
	for ; j < stopJ; j++ {
		chunk, err = iter.Next()
		c.Assert(err, IsNil)
	}
	bounds1 := chunk.Bounds

	rangeInfo := &RangeInfo{
		ChunkRange: chunk,
		TableIndex: 0,
		IndexID:    iter.GetIndexID(),
	}

	createFakeResultForBucketSplit(mock, nil, nil)
	createFakeResultForCount(mock, 64)
	createFakeResultForRandom(mock, testCases[0].aRandomValues[stopJ:], testCases[0].bRandomValues[stopJ:])
	mock.ExpectQuery("SELECT COUNT\\(DISTINCE a.*").WillReturnRows(sqlmock.NewRows([]string{"SEL"}).AddRow("123"))
	iter, err = NewBucketIteratorWithCheckpoint(ctx, "", tableDiff, db, rangeInfo)
	c.Assert(err, IsNil)
	chunk, err = iter.Next()
	c.Assert(err, IsNil)

	for i, bound := range chunk.Bounds {
		c.Assert(bounds1[i].Upper, DeepEquals, bound.Lower)
	}
}

func createFakeResultForBucketSplit(mock sqlmock.Sqlmock, aRandomValues, bRandomValues []interface{}) {
	/*
		+---------+------------+-------------+----------+-----------+-------+---------+-------------+-------------+
		| Db_name | Table_name | Column_name | Is_index | Bucket_id | Count | Repeats | Lower_Bound | Upper_Bound |
		+---------+------------+-------------+----------+-----------+-------+---------+-------------+-------------+
		| test    | test       | PRIMARY     |        1 |         0 |    64 |       1 | (0, 0)      | (63, 11)    |
		| test    | test       | PRIMARY     |        1 |         1 |   128 |       1 | (64, 12)    | (127, 23)   |
		| test    | test       | PRIMARY     |        1 |         2 |   192 |       1 | (128, 24)   | (191, 35)   |
		| test    | test       | PRIMARY     |        1 |         3 |   256 |       1 | (192, 36)   | (255, 47)   |
		| test    | test       | PRIMARY     |        1 |         4 |   320 |       1 | (256, 48)   | (319, 59)   |
		+---------+------------+-------------+----------+-----------+-------+---------+-------------+-------------+
	*/

	statsRows := sqlmock.NewRows([]string{"Db_name", "Table_name", "Column_name", "Is_index", "Bucket_id", "Count", "Repeats", "Lower_Bound", "Upper_Bound"})
	for i := 0; i < 5; i++ {
		statsRows.AddRow("test", "test", "PRIMARY", 1, (i+1)*64, (i+1)*64, 1, fmt.Sprintf("(%d, %d)", i*64, i*12), fmt.Sprintf("(%d, %d)", (i+1)*64-1, (i+1)*12-1))
	}
	mock.ExpectQuery("SHOW STATS_BUCKETS").WillReturnRows(statsRows)

	mock.ExpectQuery("SELECT COUNT\\(DISTINCE a.*").WillReturnRows(sqlmock.NewRows([]string{"SEL"}).AddRow("123"))
	createFakeResultForRandom(mock, aRandomValues, bRandomValues)
}

func createFakeResultForCount(mock sqlmock.Sqlmock, count int) {
	if count > 0 {
		// generate fake result for get the row count of this table
		countRows := sqlmock.NewRows([]string{"cnt"}).AddRow(count)
		mock.ExpectQuery("SELECT COUNT.*").WillReturnRows(countRows)
	}
}

func createFakeResultForRandom(mock sqlmock.Sqlmock, aRandomValues, bRandomValues []interface{}) {
	for i := 0; i < len(aRandomValues); i++ {
		aRandomRows := sqlmock.NewRows([]string{"a"})
		aRandomRows.AddRow(aRandomValues[i])
		mock.ExpectQuery("ORDER BY rand_value").WillReturnRows(aRandomRows)

		bRandomRows := sqlmock.NewRows([]string{"b"})
		bRandomRows.AddRow(bRandomValues[i])
		mock.ExpectQuery("ORDER BY rand_value").WillReturnRows(bRandomRows)
	}
}

func (s *testSplitterSuite) TestLimitSpliter(c *C) {
	ctx := context.Background()
	db, mock, err := sqlmock.New()
	c.Assert(err, IsNil)

	createTableSQL := "create table `test`.`test`(`a` int, `b` varchar(10), `c` float, `d` datetime, primary key(`a`, `b`))"
	tableInfo, err := dbutil.GetTableInfoBySQL(createTableSQL, parser.New())
	c.Assert(err, IsNil)

	testCases := []struct {
		limitAValues []string
		limitBValues []string
		expectResult []chunkResult
	}{
		{
			[]string{"1000", "2000", "3000", "4000"},
			[]string{"a", "b", "c", "d"},
			[]chunkResult{
				{
					"(`a` < ?) OR (`a` = ? AND `b` <= ?)",
					[]string{"1000", "1000", "a"},
				}, {
					"((`a` > ?) OR (`a` = ? AND `b` > ?)) AND ((`a` < ?) OR (`a` = ? AND `b` <= ?))",
					[]string{"1000", "1000", "a", "2000", "2000", "b"},
				}, {
					"((`a` > ?) OR (`a` = ? AND `b` > ?)) AND ((`a` < ?) OR (`a` = ? AND `b` <= ?))",
					[]string{"2000", "2000", "b", "3000", "3000", "c"},
				}, {
					"((`a` > ?) OR (`a` = ? AND `b` > ?)) AND ((`a` < ?) OR (`a` = ? AND `b` <= ?))",
					[]string{"3000", "3000", "c", "4000", "4000", "d"},
				}, {
					"(`a` > ?) OR (`a` = ? AND `b` > ?)",
					[]string{"4000", "4000", "d"},
				},
			},
		},
	}

	tableDiff := &common.TableDiff{
		Schema:    "test",
		Table:     "test",
		Info:      tableInfo,
		ChunkSize: 1000,
	}

	for i, testCase := range testCases {
		mock.ExpectQuery("SELECT COUNT\\(DISTINCE a.*").WillReturnRows(sqlmock.NewRows([]string{"SEL"}).AddRow("123"))
		createFakeResultForLimitSplit(mock, testCase.limitAValues, testCase.limitBValues, true)

		iter, err := NewLimitIterator(ctx, "", tableDiff, db)
		c.Assert(err, IsNil)

		j := 0
		for {
			chunk, err := iter.Next()
			c.Assert(err, IsNil)
			if chunk == nil {
				break
			}
			chunkStr, args := chunk.ToString("")
			c.Log(i, j, chunkStr, args)
			c.Assert(chunkStr, Equals, testCase.expectResult[j].chunkStr)
			c.Assert(args, DeepEquals, testCase.expectResult[j].args)
			j = j + 1
		}
	}

	// Test Checkpoint
	stopJ := 2
	mock.ExpectQuery("SELECT COUNT\\(DISTINCE a.*").WillReturnRows(sqlmock.NewRows([]string{"SEL"}).AddRow("123"))
	createFakeResultForLimitSplit(mock, testCases[0].limitAValues[:stopJ], testCases[0].limitBValues[:stopJ], true)
	iter, err := NewLimitIterator(ctx, "", tableDiff, db)
	c.Assert(err, IsNil)
	j := 0
	var chunk *chunk.Range
	for ; j < stopJ; j++ {
		chunk, err = iter.Next()
		c.Assert(err, IsNil)
	}
	bounds1 := chunk.Bounds

	rangeInfo := &RangeInfo{
		ChunkRange: chunk,
		IndexID:    iter.GetIndexID(),
	}

	mock.ExpectQuery("SELECT COUNT\\(DISTINCE a.*").WillReturnRows(sqlmock.NewRows([]string{"SEL"}).AddRow("123"))
	createFakeResultForLimitSplit(mock, testCases[0].limitAValues[stopJ:], testCases[0].limitBValues[stopJ:], true)
	iter, err = NewLimitIteratorWithCheckpoint(ctx, "", tableDiff, db, rangeInfo)
	c.Assert(err, IsNil)
	chunk, err = iter.Next()
	c.Assert(err, IsNil)

	for i, bound := range chunk.Bounds {
		c.Assert(bounds1[i].Upper, DeepEquals, bound.Lower)
	}
}

func createFakeResultForLimitSplit(mock sqlmock.Sqlmock, aValues []string, bValues []string, needEnd bool) {
	for i, a := range aValues {
		limitRows := sqlmock.NewRows([]string{"a", "b"})
		limitRows.AddRow(a, bValues[i])
		mock.ExpectQuery("SELECT `a`,.*").WillReturnRows(limitRows)
	}

	if needEnd {
		mock.ExpectQuery("SELECT `a`,.*").WillReturnRows(sqlmock.NewRows([]string{"a", "b"}))
	}
}

func (s *testSplitterSuite) TestRangeInfo(c *C) {
	rangeInfo := &RangeInfo{
		ChunkRange: chunk.NewChunkRange(),
		TableIndex: 1,
		IndexID:    2,
		ProgressID: "324312",
	}
	rangeInfo.Update("a", "1", "2", true, true, "[23]", "[sdg]")

	chunkRange := rangeInfo.GetChunk()
	c.Assert(chunkRange.Where, Equals, "((((`a` COLLATE '[23]' > ?)) AND ((`a` COLLATE '[23]' <= ?))) AND [sdg])")
	c.Assert(chunkRange.Args, DeepEquals, []string{"1", "2"})

	c.Assert(rangeInfo.GetTableIndex(), Equals, 1)

	rangeInfo2 := FromNode(rangeInfo.ToNode())

	chunkRange = rangeInfo2.GetChunk()
	c.Assert(chunkRange.Where, Equals, "((((`a` COLLATE '[23]' > ?)) AND ((`a` COLLATE '[23]' <= ?))) AND [sdg])")
	c.Assert(chunkRange.Args, DeepEquals, []string{"1", "2"})

	c.Assert(rangeInfo2.GetTableIndex(), Equals, 1)

}
