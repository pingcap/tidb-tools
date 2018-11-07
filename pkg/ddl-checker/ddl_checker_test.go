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

package ddl_checker

import (
	"container/list"
	. "github.com/pingcap/check"
	"reflect"
	"testing"
)

var _ = Suite(&testSuite{})

type testSuite struct {
	ec       *ExecutableChecker
	testData *list.List
}

type parseTestData struct {
	sql                 string
	parseSucceeded      bool
	tableNeededExist    []string
	tableNeededNonExist []string
	executeSucceeded    bool
}

func TestT(t *testing.T) {
	TestingT(t)
}
func (s *testSuite) SetUpSuite(c *C) {
	var err error
	s.ec, err = NewExecutableChecker()
	c.Assert(err, IsNil)
	s.setUpTestData()
}

func (s *testSuite) setUpTestData() {
	s.testData = list.New()

	s.testData.PushBack(parseTestData{sql: "drop table if exists t1,t2,t3,t4,t5;", parseSucceeded: true, tableNeededExist: []string{"t1", "t2", "t3", "t4", "t5"}, tableNeededNonExist: []string{}, executeSucceeded: true})
	s.testData.PushBack(parseTestData{sql: "drop database if exists mysqltest;", parseSucceeded: true, tableNeededExist: []string{}, tableNeededNonExist: []string{}, executeSucceeded: true})
	s.testData.PushBack(parseTestData{sql: "create table t1 (b char(0));", parseSucceeded: true, tableNeededExist: []string{}, tableNeededNonExist: []string{"t1"}, executeSucceeded: true})
	s.testData.PushBack(parseTestData{sql: "insert into t1 values (''),(null);", parseSucceeded: true, tableNeededExist: []string{}, tableNeededNonExist: []string{}, executeSucceeded: true})
	s.testData.PushBack(parseTestData{sql: "select * from t1;", parseSucceeded: true, tableNeededExist: []string{}, tableNeededNonExist: []string{}, executeSucceeded: true})
	s.testData.PushBack(parseTestData{sql: "drop table if exists t1;", parseSucceeded: true, tableNeededExist: []string{"t1"}, tableNeededNonExist: []string{}, executeSucceeded: true})
	s.testData.PushBack(parseTestData{sql: "create table t1 (b char(0) not null);", parseSucceeded: true, tableNeededExist: []string{}, tableNeededNonExist: []string{"t1"}, executeSucceeded: true})
	s.testData.PushBack(parseTestData{sql: "create table if not exists t1 (b char(0) not null);", parseSucceeded: true, tableNeededExist: []string{}, tableNeededNonExist: []string{"t1"}, executeSucceeded: true})
	s.testData.PushBack(parseTestData{sql: "insert into t1 values (''),(null);", parseSucceeded: true, tableNeededExist: []string{}, tableNeededNonExist: []string{}, executeSucceeded: false})
	s.testData.PushBack(parseTestData{sql: "select * from t1;", parseSucceeded: true, tableNeededExist: []string{}, tableNeededNonExist: []string{}, executeSucceeded: true})
	s.testData.PushBack(parseTestData{sql: "drop table t1;", parseSucceeded: true, tableNeededExist: []string{"t1"}, tableNeededNonExist: []string{}, executeSucceeded: true})

}

func (s *testSuite) TearDownSuite(c *C) {
	s.ec.Close()
}
func (s *testSuite) TestParse(c *C) {
	for e := s.testData.Front(); e != nil; e = e.Next() {
		data := e.Value.(parseTestData)
		stmt, err := s.ec.Parse(data.sql)
		if err != nil {
			c.Assert(data.parseSucceeded, IsFalse)
			continue
		}
		tableNeededExist := GetTableNeededExist(stmt)
		tableNeededNonExist := GetTableNeededNonExist(stmt)
		c.Assert(data.parseSucceeded, IsTrue)
		c.Assert(reflect.DeepEqual(data.tableNeededExist, tableNeededExist), IsTrue)
		c.Assert(reflect.DeepEqual(tableNeededNonExist, tableNeededNonExist), IsTrue)
	}
}

func (s *testSuite) TestExecute(c *C) {
	err := s.ec.Execute("use test;")
	c.Assert(err, IsNil)
	for e := s.testData.Front(); e != nil; e = e.Next() {
		data := e.Value.(parseTestData)
		err := s.ec.Execute(data.sql)
		c.Assert(err == nil, Equals, data.executeSucceeded)
	}
}

