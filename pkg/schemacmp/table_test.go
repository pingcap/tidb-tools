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

package schemacmp_test

import (
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/util/mock"

	// initialize expression.EvalAsAst
	_ "github.com/pingcap/tidb/planner"

	. "github.com/pingcap/tidb-tools/pkg/schemacmp"
)

func Test(t *testing.T) {
	TestingT(t)
}

type tableSchema struct {
	parser *parser.Parser
	sctx   *mock.Context
}

var _ = Suite(&tableSchema{})

func (s *tableSchema) SetUpSuite(c *C) {
	s.parser = parser.New()
	s.sctx = mock.NewContext()
}

func (s *tableSchema) toTableInfo(createTableStmt string) (*model.TableInfo, error) {
	node, err := s.parser.ParseOneStmt(createTableStmt, "", "")
	if err != nil {
		return nil, err
	}
	createStmtNode, ok := node.(*ast.CreateTableStmt)
	if !ok {
		return nil, errors.New("not a create table statement")
	}
	return ddl.MockTableInfo(s.sctx, createStmtNode, 1)
}

func (s *tableSchema) TestJoinSchemas(c *C) {
	testCases := []struct {
		name    string
		a       string
		b       string
		cmp     int
		cmpErr  string
		join    string
		joinErr string
	}{
		{
			name: "DM_002/1",
			a:    "CREATE TABLE tb1 (col1 INT)",
			b:    "CREATE TABLE tb2 (col1 INT, new_col1 INT)",
			cmp:  -1,
			join: "CREATE TABLE tb3 (col1 INT, new_col1 INT)",
		},
		{
			name: "DM_002/1/unordered",
			a:    "CREATE TABLE tb1 (col1 INT)",
			b:    "CREATE TABLE tb2 (new_col1 INT, col1 INT)",
			cmp:  -1,
			join: "CREATE TABLE tb3 (new_col1 INT, col1 INT)",
		},
		{
			name: "DM_002/2",
			a:    "CREATE TABLE tb1 (col1 INT, new_col1 INT)",
			b:    "CREATE TABLE tb2 (col1 INT, new_col1 INT)",
			cmp:  0,
			join: "CREATE TABLE tb3 (col1 INT, new_col1 INT)",
		},
		{
			name: "DM_002/2/unordered",
			a:    "CREATE TABLE tb1 (col1 INT, new_col1 INT)",
			b:    "CREATE TABLE tb2 (new_col1 INT, col1 INT)",
			cmp:  0,
			join: "CREATE TABLE tb3 (col1 INT, new_col1 INT)",
		},
		{
			name: "DM_010",
			a:    "CREATE TABLE tb1 (a INT, b VARCHAR(10), new_col1 INT, new_col2 INT)",
			b:    "CREATE TABLE tb2 (a INT, b VARCHAR(10))",
			cmp:  1,
			join: "CREATE TABLE tb3 (a INT, b VARCHAR(10), new_col1 INT, new_col2 INT)",
		},
		{
			name:    "DM_011",
			a:       "CREATE TABLE tb1 (a INT, b VARCHAR(10), new_col1 INT, new_col2 INT)",
			b:       "CREATE TABLE tb2 (a INT, b VARCHAR(10), new_col1 FLOAT)",
			cmpErr:  `.*"new_col1".*distinct singletons.*`,
			joinErr: `.*"new_col1".*distinct singletons.*`,
		},
		{
			name:   "DM_014",
			a:      "CREATE TABLE tb1 (a INT, b VARCHAR(10), new_col1 INT)",
			b:      "CREATE TABLE tb2 (a INT, b VARCHAR(10), new_col2 INT)",
			cmpErr: `.*combining contradicting orders.*`,
			join:   "CREATE TABLE tb3 (a INT, b VARCHAR(10), new_col1 INT, new_col2 INT)",
		},
		{
			name:    "DM_031/VARCHAR",
			a:       "CREATE TABLE tb1 (a INT, b VARCHAR(10), new_col1 INT)",
			b:       "CREATE TABLE tb2 (a INT, b VARCHAR(10), new_col1 VARCHAR(10))",
			cmpErr:  `.*"new_col1".*distinct singletons.*`,
			joinErr: `.*"new_col1".*distinct singletons.*`,
		},
		{
			name:    "DM_031/TEXT",
			a:       "CREATE TABLE tb1 (a INT, b VARCHAR(10), new_col1 INT)",
			b:       "CREATE TABLE tb2 (a INT, b VARCHAR(10), new_col1 TEXT)",
			cmpErr:  `.*"new_col1".*distinct singletons.*`,
			joinErr: `.*"new_col1".*distinct singletons.*`,
		},
		{
			name:    "DM_031/JSON",
			a:       "CREATE TABLE tb1 (a INT, b VARCHAR(10), new_col1 INT)",
			b:       "CREATE TABLE tb2 (a INT, b VARCHAR(10), new_col1 JSON)",
			cmpErr:  `.*"new_col1".*distinct singletons.*`,
			joinErr: `.*"new_col1".*distinct singletons.*`,
		},
		{
			name:   "DM_033",
			a:      "CREATE TABLE tb1 (a INT, b VARCHAR(10), c FLOAT NOT NULL)",
			b:      "CREATE TABLE tb2 (a INT, b VARCHAR(10))",
			cmpErr: `.*"c": column with no default value cannot be missing`,
			join:   "CREATE TABLE tb3 (a INT, b VARCHAR(10), c FLOAT NOT NULL DEFAULT 0)",
		},
		{
			name:    "DM_034",
			a:       "CREATE TABLE tb1 (a INT, b VARCHAR(10), new_col1 INT UNIQUE AUTO_INCREMENT)",
			b:       "CREATE TABLE tb2 (a INT, b VARCHAR(10))",
			cmpErr:  `.*combining contradicting orders.*`,
			joinErr: `.*"new_col1".*auto type but not defined as a key`,
		},
		{
			name: "DM_035",
			a:    "CREATE TABLE tb1 (a INT, b VARCHAR(10), col1 INT, col2 INT)",
			b:    "CREATE TABLE tb2 (a INT, b VARCHAR(10), col2 INT, col1 INT)",
			cmp:  0,
			join: "CREATE TABLE tb3 (a INT, b VARCHAR(10), col1 INT, col2 INT)",
		},
		{
			name:    "DM_037",
			a:       "CREATE TABLE tb1 (a INT, b VARCHAR(10), col1 INT DEFAULT 0)",
			b:       "CREATE TABLE tb2 (a INT, b VARCHAR(10), col1 INT DEFAULT -1)",
			cmpErr:  `.*"col1".*distinct singletons.*`,
			joinErr: `.*"col1".*distinct singletons.*`,
		},
		{
			name: "DM_039/1",
			a:    "CREATE TABLE tb1 (a INT, b VARCHAR(10), col1 VARCHAR(10) CHARSET utf8 COLLATE utf8_bin)",
			b:    "CREATE TABLE tb2 (a INT, b VARCHAR(10))",
			cmp:  1,
			join: "CREATE TABLE tb3 (a INT, b VARCHAR(10), col1 VARCHAR(10) CHARSET utf8 COLLATE utf8_bin)",
		},
		{
			name: "DM_039/2",
			a:    "CREATE TABLE tb1 (a INT, b VARCHAR(10), col1 VARCHAR(10) CHARSET utf8 COLLATE utf8_bin)",
			b:    "CREATE TABLE tb2 (a INT, b VARCHAR(10), col1 VARCHAR(10) CHARSET utf8 COLLATE utf8_bin)",
			cmp:  0,
			join: "CREATE TABLE tb3 (a INT, b VARCHAR(10), col1 VARCHAR(10) CHARSET utf8 COLLATE utf8_bin)",
		},
		{
			name:    "DM_040",
			a:       "CREATE TABLE tb1 (a INT, b VARCHAR(10), col1 VARCHAR(10) CHARSET utf8 COLLATE utf8_bin)",
			b:       "CREATE TABLE tb2 (a INT, b VARCHAR(10), col1 VARCHAR(10) CHARSET utf8mb4 COLLATE utf8mb4_bin)",
			cmpErr:  `.*"col1".*distinct singletons.*`,
			joinErr: `.*"col1".*distinct singletons.*`,
		},
		{
			name: "DM_041/1",
			a:    "CREATE TABLE tb1 (a INT, b VARCHAR(10), new_col1 INT AS (a + 1))",
			b:    "CREATE TABLE tb2 (a INT, b VARCHAR(10))",
			cmp:  1,
			join: "CREATE TABLE tb1 (a INT, b VARCHAR(10), new_col1 INT AS (a + 1))",
		},
		{
			name: "DM_041/2",
			a:    "CREATE TABLE tb1 (a INT, b VARCHAR(10), new_col1 INT AS (a + 1))",
			b:    "CREATE TABLE tb2 (a INT, b VARCHAR(10), new_col1 INT AS (a + 1))",
			cmp:  0,
			join: "CREATE TABLE tb1 (a INT, b VARCHAR(10), new_col1 INT AS (a + 1))",
		},
		{
			name: "DM_042",
			a:    "CREATE TABLE tb1 (a INT, b VARCHAR(10), new_col1 INT AS (a + 1) STORED)",
			b:    "CREATE TABLE tb2 (a INT, b VARCHAR(10))",
			cmp:  1,
			join: "CREATE TABLE tb1 (a INT, b VARCHAR(10), new_col1 INT AS (a + 1) STORED)",
		},
		{
			name:    "DM_043",
			a:       "CREATE TABLE tb1 (a INT, b VARCHAR(10), new_col1 INT AS (a + 1))",
			b:       "CREATE TABLE tb2 (a INT, b VARCHAR(10), new_col1 INT AS (a + 2))",
			cmpErr:  `.*"new_col1".*distinct singletons.*`,
			joinErr: `.*"new_col1".*distinct singletons.*`,
		},
		{
			name:    "DM_044",
			a:       "CREATE TABLE tb1 (a INT, b VARCHAR(10), new_col1 INT AS (a + 1) VIRTUAL)",
			b:       "CREATE TABLE tb2 (a INT, b VARCHAR(10), new_col1 INT AS (a + 1) STORED)",
			cmpErr:  `.*"new_col1".*distinct singletons.*`,
			joinErr: `.*"new_col1".*distinct singletons.*`,
		},
		{
			name:   "DM_052",
			a:      "CREATE TABLE tb1 (a INT, b VARCHAR(10))",
			b:      "CREATE TABLE tb2 (c BIGINT, b VARCHAR(10))",
			cmpErr: `.*combining contradicting orders.*`,
			join:   `CREATE TABLE tb3 (a INT, b VARCHAR(10), c BIGINT)`,
		},
		{
			name:    "DM_053",
			a:       "CREATE TABLE tb1 (c BIGINT, b VARCHAR(10))",
			b:       "CREATE TABLE tb2 (c DOUBLE, b VARCHAR(10))",
			cmpErr:  `.*"c".*distinct singletons.*`,
			joinErr: `.*"c".*distinct singletons.*`,
		},
		// { // TODO: re-enable after we've implemented merging of compatible types.
		// 	name: "DM_055",
		// 	a:    "CREATE TABLE tb1 (a INT, b VARCHAR(10))",
		// 	b:    "CREATE TABLE tb2 (a BIGINT, b VARCHAR(10))",
		// 	join: "CREATE TABLE tb2 (a BIGINT, b VARCHAR(10))",
		// },
		{
			name:   "DM_057",
			a:      "CREATE TABLE tb1 (a INT, b VARCHAR(10))",
			b:      "CREATE TABLE tb2 (c INT DEFAULT 1, b VARCHAR(10))",
			cmpErr: `.*combining contradicting orders.*`,
			join:   "CREATE TABLE tb3 (a INT, b VARCHAR(10), c INT DEFAULT 1)",
		},
		{
			name:    "DM_061",
			a:       "CREATE TABLE tb1 (a INT, b VARCHAR(10))",
			b:       "CREATE TABLE tb2 (a INT, b VARCHAR(10) CHARSET utf8)",
			cmpErr:  `.*"b".*distinct singletons.*`,
			joinErr: `.*"b".*distinct singletons.*`,
		},
		{
			name: "DM_066",
			a:    "CREATE TABLE tb1 (a INT, b VARCHAR(10))",
			b:    "CREATE TABLE tb2 (a INT DEFAULT 1, b VARCHAR(10))",
			cmp:  -1,
			join: "CREATE TABLE tb3 (a INT DEFAULT 1, b VARCHAR(10))",
		},
		// { // these table options are somehow ignored by the parser.
		// 	name:    "DM_074",
		// 	a:       "CREATE TABLE tbl1 (a INT, b VARCHAR(10)) CHARSET utf8 COLLATE utf8_bin",
		// 	b:       "CREATE TABLE tbl2 (a INT, b VARCHAR(10)) CHARSET utf8mb4 COLLATE utf8mb4_bin",
		// 	joinErr: `.*distinct singletons.*`,
		// },
		{
			name: "DM_078",
			a:    "CREATE TABLE tb1 (a INT, b VARCHAR(10))",
			b:    "CREATE TABLE tb2 (a INT PRIMARY KEY, b VARCHAR(10))",
			cmp:  1,
			join: "CREATE TABLE tb3 (a INT, b VARCHAR(10))",
		},
		{
			name: "DM_080/1",
			a:    "CREATE TABLE tb1 (a INT, b VARCHAR(10), UNIQUE KEY idx_a(a), UNIQUE KEY idx_b(b), UNIQUE KEY idx_ab(a, b))",
			b:    "CREATE TABLE tb2 (a INT, b VARCHAR(10))",
			cmp:  -1,
			join: "CREATE TABLE tb3 (a INT, b VARCHAR(10))",
		},
		{
			name: "DM_080/2",
			a:    "CREATE TABLE tb1 (a INT, b VARCHAR(10), UNIQUE KEY idx_a(a), UNIQUE KEY idx_b(b), UNIQUE KEY idx_ab(a, b))",
			b:    "CREATE TABLE tb2 (a INT, b VARCHAR(10), UNIQUE KEY idx_a(a), UNIQUE KEY idx_b(b))",
			cmp:  -1,
			join: "CREATE TABLE tb3 (a INT, b VARCHAR(10), UNIQUE KEY idx_a(a), UNIQUE KEY idx_b(b))",
		},
		// { // index visibility is not visible in IndexInfo yet.
		// 	name:    "DM_086",
		// 	a:       "CREATE TABLE tb1 (a INT, b VARCHAR(10), UNIQUE KEY a(a) VISIBLE)",
		// 	b:       "CREATE TABLE tb2 (a INT, b VARCHAR(10), UNIQUE KEY a(a) INVISIBLE)",
		// 	joinErr: `.*distinct singletons.*`,
		// },
		{
			name:   "Different index components",
			a:      "CREATE TABLE tbl1 (a INT, b INT, KEY i(a))",
			b:      "CREATE TABLE tbl2 (a INT, b INT, KEY i(b))",
			cmpErr: `.*combining contradicting orders.*`,
			join:   "CREATE TABLE tbl3 (a INT, b INT)",
		},
		{
			name:   "Different index order",
			a:      "CREATE TABLE tbl1 (a INT, b INT, KEY i(a, b))",
			b:      "CREATE TABLE tbl2 (a INT, b INT, KEY i(b, a))",
			cmpErr: `.*combining contradicting orders.*`,
			join:   "CREATE TABLE tbl3 (a INT, b INT)",
		},
		{
			name:   "Different index length",
			a:      "CREATE TABLE tbl1 (a TEXT, KEY i(a(14)))",
			b:      "CREATE TABLE tbl2 (a TEXT, KEY i(a(15)))",
			cmpErr: `.*distinct singletons.*`,
			join:   "CREATE TABLE tbl3 (a TEXT)",
		},
		{
			name:    "Cannot drop key tied to AUTO_INC column",
			a:       "CREATE TABLE tbl1(a INT AUTO_INCREMENT, b INT, KEY i(a))",
			b:       "CREATE TABLE tbl2(a INT AUTO_INCREMENT, b INT, KEY i(a, b))",
			cmpErr:  `.*distinct singletons.*`,
			joinErr: `.*"a".*auto type but not defined as a key`,
		},
		{
			name: "not-null column with special types",
			a: `CREATE TABLE tbl1(
				a1 INT NOT NULL,
				b1 DECIMAL NOT NULL,
				c1 VARCHAR(20) NOT NULL,
				d1 DATETIME(3) NOT NULL,
				e1 ENUM('abc', 'def') NOT NULL
			)`,
			b: `CREATE TABLE tbl2(
				a2 TIME NOT NULL,
				b2 DATE NOT NULL,
				c2 BINARY(50) NOT NULL,
				d2 YEAR(4) NOT NULL,
				e2 SET('abc', 'def') NOT NULL
			)`,
			cmpErr: `.*column with no default value cannot be missing`,
			join: `CREATE TABLE tbl3(
				a1 INT NOT NULL DEFAULT 0,
				b1 DECIMAL NOT NULL DEFAULT 0,
				c1 VARCHAR(20) NOT NULL DEFAULT '',
				d1 DATETIME(3) NOT NULL DEFAULT '0000-00-00 00:00:00',
				e1 ENUM('abc', 'def') NOT NULL DEFAULT 'abc',
				a2 TIME NOT NULL DEFAULT '00:00:00',
				b2 DATE NOT NULL DEFAULT '0000-00-00',
				c2 BINARY(50) NOT NULL DEFAULT '',
				d2 YEAR(4) NOT NULL DEFAULT '0000',
				e2 SET('abc', 'def') NOT NULL DEFAULT ''
			)`,
		},
	}

	for _, tc := range testCases {
		assert := func(obtained interface{}, checker Checker, args ...interface{}) {
			args = append(args, Commentf("test case = %+v", tc))
			c.Assert(obtained, checker, args...)
		}

		tia, err := s.toTableInfo(tc.a)
		assert(err, IsNil)
		tib, err := s.toTableInfo(tc.b)
		assert(err, IsNil)

		a := Encode(tia)
		b := Encode(tib)
		var j Table
		if len(tc.joinErr) == 0 {
			tij, err := s.toTableInfo(tc.join)
			assert(err, IsNil)
			j = Encode(tij)
		}

		cmp, err := a.Compare(b)
		if len(tc.cmpErr) != 0 {
			assert(err, ErrorMatches, tc.cmpErr)
		} else {
			assert(err, IsNil)
			assert(cmp, Equals, tc.cmp)
		}

		cmp, err = b.Compare(a)
		if len(tc.cmpErr) != 0 {
			assert(err, ErrorMatches, tc.cmpErr)
		} else {
			assert(err, IsNil)
			assert(cmp, Equals, -tc.cmp)
		}

		joined, err := a.Join(b)
		if len(tc.joinErr) != 0 {
			if err == nil {
				c.Log("a = ", a)
				c.Log("b = ", b)
				c.Log("j = ", joined)
			}
			assert(err, ErrorMatches, tc.joinErr)
		} else {
			assert(err, IsNil)
			assert(joined, DeepEquals, j)
			assert(joined.String(), Equals, j.String())
		}

		joined, err = b.Join(a)
		if len(tc.joinErr) != 0 {
			assert(err, ErrorMatches, tc.joinErr)
		} else {
			assert(err, IsNil)
			assert(joined, DeepEquals, j)

			cmp, err = joined.Compare(a)
			assert(err, IsNil)
			assert(cmp, GreaterEqual, 0)

			cmp, err = joined.Compare(b)
			assert(err, IsNil)
			assert(cmp, GreaterEqual, 0)
		}
	}
}
