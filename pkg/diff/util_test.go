/*************************************************************************
 *
 * PingCAP CONFIDENTIAL
 * __________________
 *
 *  [2015] - [2018] PingCAP Incorporated
 *  All Rights Reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of PingCAP Incorporated and its suppliers,
 * if any.  The intellectual and technical concepts contained
 * herein are proprietary to PingCAP Incorporated
 * and its suppliers and may be covered by P.R.China and Foreign Patents,
 * patents in process, and are protected by trade secret or copyright law.
 * Dissemination of this information or reproduction of this material
 * is strictly forbidden unless prior written permission is obtained
 * from PingCAP Incorporated.
 */

package diff

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb-tools/pkg/dbutil"
)

var _ = Suite(&testUtilSuite{})

type testUtilSuite struct{}

func (s *testUtilSuite) TestRemoveColumns(c *C) {
	createTableSQL1 := "CREATE TABLE `test`.`atest` (`a` int, `b` int, `c` int, `d` int, primary key(`a`))"
	tableInfo1, err := dbutil.GetTableInfoBySQL(createTableSQL1)
	c.Assert(err, IsNil)
	tbInfo := removeColumns(tableInfo1, []string{"a"})
	c.Assert(len(tbInfo.Columns), Equals, 3)
	c.Assert(len(tbInfo.Indices), Equals, 0)

	createTableSQL2 := "CREATE TABLE `test`.`atest` (`a` int, `b` int, `c` int, `d` int, primary key(`a`), index idx(`b`, `c`))"
	tableInfo2, err := dbutil.GetTableInfoBySQL(createTableSQL2)
	c.Assert(err, IsNil)
	tbInfo = removeColumns(tableInfo2, []string{"a", "b"})
	c.Assert(len(tbInfo.Columns), Equals, 2)
	c.Assert(len(tbInfo.Indices), Equals, 1)

	createTableSQL3 := "CREATE TABLE `test`.`atest` (`a` int, `b` int, `c` int, `d` int, primary key(`a`), index idx(`b`, `c`))"
	tableInfo3, err := dbutil.GetTableInfoBySQL(createTableSQL3)
	c.Assert(err, IsNil)
	tbInfo = removeColumns(tableInfo3, []string{"b", "c"})
	c.Assert(len(tbInfo.Columns), Equals, 2)
	c.Assert(len(tbInfo.Indices), Equals, 1)
}
