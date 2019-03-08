// Copyright 2019 PingCAP, Inc.
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
	"fmt"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb-tools/pkg/dbutil"
)

var _ = Suite(&testSpliterSuite{})

type testSpliterSuite struct{}

func (s *testSpliterSuite) TestRandomSpliter(c *C) {
	db, mock, err := sqlmock.New()
	c.Assert(err, IsNil)

	createTableSQL := "create table `test`.`test`(`a` int, `b` varchar(10), `c` float, `d` datetime, primary key(`a`, `b`))"
	tableInfo, err := dbutil.GetTableInfoBySQL(createTableSQL)
	c.Assert(err, IsNil)

	tableInstance := &TableInstance{
		Conn:   db,
		Schema: "test",
		Table:  "test",
		info:   tableInfo,
	}

	testCases := []struct {
		count              int
		aMin               int
		aMax               int
		bMin               string
		bMax               string
		aRandomValues      []int
		bRandomValues      []string
		aRandomValueCounts []int
	}{
		// only use column a
		{10, 0, 10, "x", "z", []int{1, 2, 3, 4}, nil, []int{1, 1, 1, 1}},
		// will use column b
		{10, 0, 10, "x", "z", []int{1, 2, 3, 4}, []string{"y"}, []int{1, 2, 1, 1}},
		// will split the max value by column b
		{10, 0, 10, "x", "z", []int{1, 2, 3, 10}, []string{"y"}, []int{1, 1, 1, 1}},
		// will split the min value by column b
		{10, 0, 10, "x", "z", []int{0, 2, 3, 4}, []string{"y"}, []int{1, 1, 1, 1}},
		// will split the min value and max value by column b
		{4, 0, 10, "x", "z", []int{0, 10}, []string{"y"}, []int{1, 1}},
		// will split all values by column b
		{4, 0, 10, "x", "z", []int{0, 5, 10}, []string{"y"}, []int{1, 2, 1}},
	}

	expectResult := [][]struct {
		chunkStr string
		args     []string
	}{
		{
			{"`a` < ?", []string{"0"}},
			{"`a` >= ? AND `a` < ?", []string{"0", "1"}},
			{"`a` >= ? AND `a` < ?", []string{"1", "2"}},
			{"`a` >= ? AND `a` < ?", []string{"2", "3"}},
			{"`a` >= ? AND `a` < ?", []string{"3", "4"}},
			{"`a` >= ? AND `a` <= ?", []string{"4", "10"}},
			{"`a` > ?", []string{"10"}},
		},
		{
			{"`a` < ?", []string{"0"}},
			{"`a` >= ? AND `a` < ?", []string{"0", "1"}},
			{"`a` >= ? AND `a` < ?", []string{"1", "2"}},
			{"`a` = ? AND `b` < ?", []string{"2", "x"}},
			{"`a` = ? AND `b` >= ? AND `b` < ?", []string{"2", "x", "y"}},
			{"`a` = ? AND `b` >= ? AND `b` <= ?", []string{"2", "y", "z"}},
			{"`a` = ? AND `b` > ?", []string{"2", "z"}},
			{"`a` > ? AND `a` < ?", []string{"2", "3"}},
			{"`a` >= ? AND `a` < ?", []string{"3", "4"}},
			{"`a` >= ? AND `a` <= ?", []string{"4", "10"}},
			{"`a` > ?", []string{"10"}},
		},
		{
			{"`a` < ?", []string{"0"}},
			{"`a` >= ? AND `a` < ?", []string{"0", "1"}},
			{"`a` >= ? AND `a` < ?", []string{"1", "2"}},
			{"`a` >= ? AND `a` < ?", []string{"2", "3"}},
			{"`a` >= ? AND `a` < ?", []string{"3", "10"}},
			{"`a` = ? AND `b` < ?", []string{"10", "x"}},
			{"`a` = ? AND `b` >= ? AND `b` < ?", []string{"10", "x", "y"}},
			{"`a` = ? AND `b` >= ? AND `b` <= ?", []string{"10", "y", "z"}},
			{"`a` = ? AND `b` > ?", []string{"10", "z"}},
			{"`a` > ?", []string{"10"}},
		},
		{
			{"`a` < ?", []string{"0"}},
			{"`a` = ? AND `b` < ?", []string{"0", "x"}},
			{"`a` = ? AND `b` >= ? AND `b` < ?", []string{"0", "x", "y"}},
			{"`a` = ? AND `b` >= ? AND `b` <= ?", []string{"0", "y", "z"}},
			{"`a` = ? AND `b` > ?", []string{"0", "z"}},
			{"`a` > ? AND `a` < ?", []string{"0", "2"}},
			{"`a` >= ? AND `a` < ?", []string{"2", "3"}},
			{"`a` >= ? AND `a` < ?", []string{"3", "4"}},
			{"`a` >= ? AND `a` <= ?", []string{"4", "10"}},
			{"`a` > ?", []string{"10"}},
		},
		{
			{"`a` < ?", []string{"0"}},
			{"`a` = ? AND `b` < ?", []string{"0", "x"}},
			{"`a` = ? AND `b` >= ? AND `b` < ?", []string{"0", "x", "y"}},
			{"`a` = ? AND `b` >= ? AND `b` <= ?", []string{"0", "y", "z"}},
			{"`a` = ? AND `b` > ?", []string{"0", "z"}},
			{"`a` > ? AND `a` < ?", []string{"0", "10"}},
			{"`a` = ? AND `b` < ?", []string{"10", "x"}},
			{"`a` = ? AND `b` >= ? AND `b` < ?", []string{"10", "x", "y"}},
			{"`a` = ? AND `b` >= ? AND `b` <= ?", []string{"10", "y", "z"}},
			{"`a` = ? AND `b` > ?", []string{"10", "z"}},
			{"`a` > ?", []string{"10"}},
		},
		{
			{"`a` < ?", []string{"0"}},
			{"`a` = ? AND `b` < ?", []string{"0", "x"}},
			{"`a` = ? AND `b` >= ? AND `b` < ?", []string{"0", "x", "y"}},
			{"`a` = ? AND `b` >= ? AND `b` <= ?", []string{"0", "y", "z"}},
			{"`a` = ? AND `b` > ?", []string{"0", "z"}},
			{"`a` > ? AND `a` < ?", []string{"0", "5"}},
			{"`a` = ? AND `b` < ?", []string{"5", "x"}},
			{"`a` = ? AND `b` >= ? AND `b` < ?", []string{"5", "x", "y"}},
			{"`a` = ? AND `b` >= ? AND `b` <= ?", []string{"5", "y", "z"}},
			{"`a` = ? AND `b` > ?", []string{"5", "z"}},
			{"`a` > ? AND `a` < ?", []string{"5", "10"}},
			{"`a` = ? AND `b` < ?", []string{"10", "x"}},
			{"`a` = ? AND `b` >= ? AND `b` < ?", []string{"10", "x", "y"}},
			{"`a` = ? AND `b` >= ? AND `b` <= ?", []string{"10", "y", "z"}},
			{"`a` = ? AND `b` > ?", []string{"10", "z"}},
			{"`a` > ?", []string{"10"}},
		},
	}

	for i, t := range testCases {
		createFakeResultForRandomSplit(mock, t.count, t.aMin, t.aMax, t.bMin, t.bMax, t.aRandomValues, t.bRandomValues, t.aRandomValueCounts)

		rSpliter := new(randomSpliter)
		chunks, err := rSpliter.split(tableInstance, tableInfo.Columns, 2, "TRUE", "")
		c.Assert(err, IsNil)

		for j, chunk := range chunks {
			chunkStr, args := chunk.toString(normalMode, "")
			c.Assert(chunkStr, Equals, expectResult[i][j].chunkStr)
			c.Assert(args, DeepEquals, expectResult[i][j].args)
		}
	}

	// test case for split a range use same column
	// split (0, 10) to (0, 5) and [5, 10)
	randomRows := sqlmock.NewRows([]string{"a", "count"}).AddRow("5", 1)
	mock.ExpectQuery("ORDER BY RAND()").WillReturnRows(randomRows)

	expectChunks := []struct {
		chunkStr string
		args     []string
	}{
		{"`a` > ? AND `a` < ?", []string{"0", "5"}},
		{"`a` >= ? AND `a` < ?", []string{"5", "10"}},
	}

	r := &randomSpliter{
		table: tableInstance,
	}

	oriChunk := newChunkRange().copyAndUpdate("a", "0", gt, "10", lt)
	chunks, err := r.splitRange(db, oriChunk, 2, "test", "test", tableInfo.Columns)
	c.Assert(err, IsNil)
	for i, chunk := range chunks {
		chunkStr, args := chunk.toString(normalMode, "")
		c.Assert(chunkStr, Equals, expectChunks[i].chunkStr)
		c.Assert(args, DeepEquals, expectChunks[i].args)
	}
}

func createFakeResultForRandomSplit(mock sqlmock.Sqlmock, count, aMin, aMax int, bMin, bMax string, aRandomValues []int, bRandomValues []string, aRandomValueCounts []int) {
	// generate fake result for get the row count of this table
	countRows := sqlmock.NewRows([]string{"cnt"}).AddRow(count)
	mock.ExpectQuery("SELECT COUNT.*").WillReturnRows(countRows)

	// generate fake result for get min and max value for column a
	aMinMaxRows := sqlmock.NewRows([]string{"MIN", "MAX"}).AddRow(aMin, aMax)
	mock.ExpectQuery("SELECT .* MIN(.+a.+) as MIN, .*").WillReturnRows(aMinMaxRows)

	// generate fake result for get random value for column a
	aRandomRows := sqlmock.NewRows([]string{"a", "count"})
	for i, randomValue := range aRandomValues {
		aRandomRows.AddRow(randomValue, aRandomValueCounts[i])
	}
	mock.ExpectQuery("ORDER BY RAND()").WillReturnRows(aRandomRows)

	if len(bRandomValues) == 0 {
		return
	}

	num := 0
	splitValues := make(map[int]int)
	for i, value := range aRandomValues {
		splitValues[value] = aRandomValueCounts[i]
	}
	splitValues[aMin]++
	splitValues[aMax]++
	for _, count := range splitValues {
		if count > 1 {
			num += (count - 1)
		}
	}

	// means need split more num times for column b
	for i := 0; i < num; i++ {
		// generate fake result for get min and max value for column b
		bMinMaxRows := sqlmock.NewRows([]string{"MIN", "MAX"}).AddRow(bMin, bMax)
		mock.ExpectQuery("SELECT .* MIN(.+b.+) as MIN, .*").WillReturnRows(bMinMaxRows)

		// generate fake result for get random value for column b
		bRandomRows := sqlmock.NewRows([]string{"b", "count"})
		for _, randomValue := range bRandomValues {
			bRandomRows.AddRow(randomValue, 1)
		}
		mock.ExpectQuery("SELECT b, COUNT.*").WillReturnRows(bRandomRows)
	}

	return
}

func (s *testSpliterSuite) TestBucketSpliter(c *C) {
	db, mock, err := sqlmock.New()
	c.Assert(err, IsNil)

	createTableSQL := "create table `test`.`test`(`a` int, `b` varchar(10), `c` float, `d` datetime, primary key(`a`, `b`))"
	tableInfo, err := dbutil.GetTableInfoBySQL(createTableSQL)
	c.Assert(err, IsNil)

	chunkSizes := []int{2, 64, 127, 128}
	expectResult := [][]struct {
		chunkStr string
		args     []string
	}{
		{
			{"(`a` <= ?) OR (`a` = ? AND `b` <= ?)", []string{"63", "63", "11"}},
			{"((`a` > ?) OR (`a` = ? AND `b` > ?)) AND ((`a` <= ?) OR (`a` = ? AND `b` <= ?))", []string{"63", "63", "11", "127", "127", "23"}},
			{"((`a` > ?) OR (`a` = ? AND `b` > ?)) AND ((`a` <= ?) OR (`a` = ? AND `b` <= ?))", []string{"127", "127", "23", "191", "191", "35"}},
			{"((`a` > ?) OR (`a` = ? AND `b` > ?)) AND ((`a` <= ?) OR (`a` = ? AND `b` <= ?))", []string{"191", "191", "35", "255", "255", "47"}},
			{"(`a` > ?) OR (`a` = ? AND `b` > ?)", []string{"255", "255", "47"}},
		},
		{
			{"(`a` <= ?) OR (`a` = ? AND `b` <= ?)", []string{"127", "127", "23"}},
			{"((`a` > ?) OR (`a` = ? AND `b` > ?)) AND ((`a` <= ?) OR (`a` = ? AND `b` <= ?))", []string{"127", "127", "23", "255", "255", "47"}},
			{"(`a` > ?) OR (`a` = ? AND `b` > ?)", []string{"255", "255", "47"}},
		},
		{
			{"(`a` <= ?) OR (`a` = ? AND `b` <= ?)", []string{"127", "127", "23"}},
			{"((`a` > ?) OR (`a` = ? AND `b` > ?)) AND ((`a` <= ?) OR (`a` = ? AND `b` <= ?))", []string{"127", "127", "23", "255", "255", "47"}},
			{"(`a` > ?) OR (`a` = ? AND `b` > ?)", []string{"255", "255", "47"}},
		},
		{
			{"(`a` <= ?) OR (`a` = ? AND `b` <= ?)", []string{"191", "191", "35"}},
			{"(`a` > ?) OR (`a` = ? AND `b` > ?)", []string{"191", "191", "35"}},
		},
	}

	tableInstance := &TableInstance{
		Conn:   db,
		Schema: "test",
		Table:  "test",
		info:   tableInfo,
	}

	for i, chunkSize := range chunkSizes {
		fmt.Println(i)
		createFakeResultForBucketSplit(mock)
		bSpliter := new(bucketSpliter)
		chunks, err := bSpliter.split(tableInstance, tableInfo.Columns, chunkSize, "TRUE", "")
		c.Assert(err, IsNil)

		for j, chunk := range chunks {
			chunkStr, args := chunk.toString(bucketMode, "")
			c.Assert(chunkStr, Equals, expectResult[i][j].chunkStr)
			c.Assert(args, DeepEquals, expectResult[i][j].args)
		}
	}

}

func createFakeResultForBucketSplit(mock sqlmock.Sqlmock) {
	/*
		+---------+------------+-------------+----------+-----------+-------+---------+-------------+-------------+
		| Db_name | Table_name | Column_name | Is_index | Bucket_id | Count | Repeats | Lower_Bound | Upper_Bound |
		+---------+------------+-------------+----------+-----------+-------+---------+-------------+-------------+
		| test    | test       | PRIMARY     |        1 |         0 |    64 |       1 | (0, 0)      | (63, 11)    |
		| test    | test       | PRIMARY     |        1 |         1 |   128 |       1 | (64, 12)    | (127, 23)   |
		| test    | test       | PRIMARY     |        1 |         2 |   192 |       1 | (128, 24)   | (191, 35)   |
		| test    | test       | PRIMARY     |        1 |         3 |   256 |       1 | (192, 36)   | (255, 47)   |
		| test    | test       | PRIMARY     |        1 |         4 |   320 |       1 | (256, 48)   | (319, 59)   |
	*/

	statsRows := sqlmock.NewRows([]string{"Db_name", "Table_name", "Column_name", "Is_index", "Bucket_id", "Count", "Repeats", "Lower_Bound", "Upper_Bound"})
	for i := 0; i < 5; i++ {
		statsRows.AddRow("test", "test", "PRIMARY", 1, (i+1)*64, (i+1)*64, 1, fmt.Sprintf("(%d, %d)", i*64, i*12), fmt.Sprintf("(%d, %d)", (i+1)*64-1, (i+1)*12-1))
	}
	mock.ExpectQuery("SHOW STATS_BUCKETS").WillReturnRows(statsRows)

	return
}
