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
	"container/heap"
	"strconv"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb-tools/pkg/dbutil"
)

var _ = Suite(&testMergerSuite{})

type testMergerSuite struct{}

func (s *testMergerSuite) TestMerge(c *C) {
	createTableSQL := "create table test.test(id int(24), name varchar(24), age int(24), primary key(id, name));"
	tableInfo, err := dbutil.GetTableInfoBySQL(createTableSQL)
	c.Assert(err, IsNil)

	_, orderKeyCols := dbutil.SelectUniqueOrderKey(tableInfo)
	ids := []int{3, 2, 2, 4, 1}
	names := []string{"d", "b", "c", "b", "a"}
	ages := []int{1, 2, 3, 4, 5}
	null := map[string]bool{
		"id":   false,
		"name": false,
		"age":  false,
	}

	expectIDs := []int64{1, 2, 2, 3, 4}
	expectNames := []string{"a", "b", "c", "d", "b"}

	rowDatas := &RowDatas{
		Rows:         make([]RowData, 0, len(ids)),
		OrderKeyCols: orderKeyCols,
	}

	heap.Init(rowDatas)
	for i, id := range ids {
		data := map[string][]byte{
			"id":   []byte(strconv.Itoa(id)),
			"name": []byte(names[i]),
			"age":  []byte(strconv.Itoa(ages[i])),
		}
		heap.Push(rowDatas, RowData{
			Data: data,
			Null: null,
		})
	}

	for i := 0; i < len(ids); i++ {
		rowData := heap.Pop(rowDatas).(RowData)

		id, err := strconv.ParseInt(string(rowData.Data["id"]), 10, 64)
		c.Assert(err, IsNil)
		name := string(rowData.Data["name"])
		c.Assert(id, Equals, expectIDs[i])
		c.Assert(name, Equals, expectNames[i])
	}
}
