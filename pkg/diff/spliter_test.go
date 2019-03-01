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
	//"context"
	//"fmt"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb-tools/pkg/dbutil"
)

var _ = Suite(&testSpliterSuite{})

type testSpliterSuite struct{}

func (s *testSpliterSuite) TestSpliter(c *C) {
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

	/*
	countRows := sqlmock.NewRows([]string{"cnt"}).AddRow(10)
	mock.ExpectQuery("SELECT COUNT").WillReturnRows(countRows)

	minMaxRows := sqlmock.NewRows([]string{"MIN", "MAX"}).AddRow(0, 10)
	mock.ExpectQuery("MIN, MAX").WillReturnRows(minMaxRows)

	randomRows := sqlmock.NewRows([]string{"a", "count"}).AddRow(1, 1).AddRow(2, 1).AddRow(3, 1).AddRow(4, 1)
	mock.ExpectQuery("ORDER BY RAND()").WillReturnRows(randomRows)
	*/

	//count, err := dbutil.GetRowCount(context.Background(), db, "test", "test", "")
	//c.Assert(err, IsNil)
	//c.Assert(count, Equals, int64(10))
	testCases := []struct{
		count int
		aMin int
		aMax int
		bMin string
		bMax string
		aRandomValues []int
		bRandomValues []string
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
	}

	expectResult := [][]struct{
		chunkStr string
		args     []string
	}{
		{
			{"`a` < ?", []string{"1"}},
			{"`a` >= ? AND `a` < ?", []string{"1", "2"}},
			{"`a` >= ? AND `a` < ?", []string{"2", "3"}},
			{"`a` >= ? AND `a` < ?", []string{"3", "4"}},
			{"`a` >= ?", []string{"4"}},
		},
		{
			{"`a` < ?", []string{"1"}},
			{"`a` >= ? AND `a` < ?", []string{"1", "2"}},
			{"`a` = ? AND `b` < ?", []string{"2", "y"}},
			{"`a` = ? AND `b` >= ?", []string{"2", "y"}},
			{"`a` > ? AND `a` < ?", []string{"2", "3"}},
			{"`a` >= ? AND `a` < ?", []string{"3", "4"}},
			{"`a` >= ?", []string{"4"}},
		},
		{
			{"`a` < ?", []string{"1"}},
			{"`a` >= ? AND `a` < ?", []string{"1", "2"}},
			{"`a` >= ? AND `a` < ?", []string{"2", "3"}},
			{"`a` >= ? AND `a` < ?", []string{"3", "10"}},
			{"`a` = ? AND `b` < ?", []string{"10", "y"}},
			{"`a` = ? AND `b` >= ?", []string{"10", "y"}},
			{"`a` > ?", []string{"10"}},
		},
		{
			{"`a` < ?", []string{"0"}},
			{"`a` = ? AND `b` < ?", []string{"0", "y"}},
			{"`a` = ? AND `b` >= ?", []string{"0", "y"}},
			{"`a` > ? AND `a` < ?", []string{"0", "2"}},
			{"`a` >= ? AND `a` < ?", []string{"2", "3"}},
			{"`a` >= ? AND `a` <= ?", []string{"3", "4"}},
			{"`a` > ?", []string{"4"}},
		},
	}

	for i, t := range testCases {
		
		createFakeResult(mock, t.count, t.aMin, t.aMax, t.bMin, t.bMax, t.aRandomValues, t.bRandomValues, t.aRandomValueCounts)

		rSpliter := new(randomSpliter)
		chunks, err := rSpliter.split(tableInstance, tableInfo.Columns, 2, "TRUE", "")
		c.Assert(err, IsNil)

		for j, chunk := range chunks {
			chunkStr, args := chunk.toString(normalMode, "")
			c.Assert(chunkStr, Equals, expectResult[i][j].chunkStr)
			c.Assert(args, DeepEquals, expectResult[i][j].args)
		}
	}
	
	
	/*
	for _, chunk := range chunks {
		chunkStr, args := chunk.toString(normalMode, "")
		fmt.Println(chunkStr)
		fmt.Println(args)
	}
	*/
	//c.Assert(chunks, HasLen, 1)
}

func createFakeResult(mock sqlmock.Sqlmock, count, aMin, aMax int, bMin, bMax string, aRandomValues []int, bRandomValues []string, aRandomValueCounts []int) {
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

	// generate fake result for get min and max value for column b 
	bMinMaxRows := sqlmock.NewRows([]string{"MIN", "MAX"}).AddRow(bMin, bMax)
	mock.ExpectQuery("SELECT .* MIN(.+b.+) as MIN, .*").WillReturnRows(bMinMaxRows)

	// generate fake result for get random value for column b
	bRandomRows := sqlmock.NewRows([]string{"b", "count"})
	for _, randomValue := range bRandomValues {
		bRandomRows.AddRow(randomValue, 1)
	}
	mock.ExpectQuery("SELECT b, COUNT.*").WillReturnRows(bRandomRows)

	return
}
