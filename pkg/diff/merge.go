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
	"strconv"

	"github.com/ngaut/log"
	"github.com/pingcap/tidb/model"
)

// RowData is the struct of rows selected from mysql/tidb
type RowData struct {
	Data   map[string][]byte
	Null   map[string]bool
	Source string
}

// RowDatas is a heap of MergeItems.
type RowDatas struct {
	Rows         []RowData
	OrderKeyCols []*model.ColumnInfo
}

func (r RowDatas) Len() int { return len(r.Rows) }
func (r RowDatas) Less(i, j int) bool {
	var data1, data2 []byte

	for _, col := range r.OrderKeyCols {
		data1 = r.Rows[i].Data[col.Name.O]
		data2 = r.Rows[j].Data[col.Name.O]
		if needQuotes(col.FieldType) {
			if string(data1) > string(data2) {
				return false
			} else if string(data1) < string(data2) {
				return true
			} else {
				// `NULL` is less than ""
				if r.Rows[i].Null[col.Name.O] {
					return true
				}
				if r.Rows[j].Null[col.Name.O] {
					return false
				}
				continue
			}
		} else {
			num1, err1 := strconv.ParseFloat(string(data1), 64)
			num2, err2 := strconv.ParseFloat(string(data2), 64)
			if err1 != nil || err2 != nil {
				log.Fatalf("convert %s, %s to float failed, err1: %v, err2: %v", string(data1), string(data2), err1, err2)
			}
			if num1 > num2 {
				return false
			} else if num1 < num2 {
				return true
			} else {
				continue
			}
		}
	}

	return true
}
func (r RowDatas) Swap(i, j int) { r.Rows[i], r.Rows[j] = r.Rows[j], r.Rows[i] }

// Push implements heap.Interface's Push function
func (r *RowDatas) Push(x interface{}) {
	r.Rows = append(r.Rows, x.(RowData))
}

// Pop implements heap.Interface's Pop function
func (r *RowDatas) Pop() interface{} {
	old := r.Rows
	n := len(old)
	x := old[n-1]
	r.Rows = old[0 : n-1]
	return x
}
