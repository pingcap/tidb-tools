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
	"strconv"

	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb-tools/pkg/dbutil"
	log "github.com/sirupsen/logrus"
)

// RowData is the struct of rows selected from mysql/tidb
type RowData struct {
	Data   map[string]*dbutil.ColumnData
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
		data1 = r.Rows[i].Data[col.Name.O].Data
		data2 = r.Rows[j].Data[col.Name.O].Data
		if needQuotes(col.FieldType) {
			strData1 := string(data1)
			strData2 := string(data2)

			if strData1 == strData2 {
				// `NULL` is less than ""
				if r.Rows[i].Data[col.Name.O].IsNull {
					return true
				}
				if r.Rows[j].Data[col.Name.O].IsNull {
					return false
				}
				continue
			}
			if strData1 > strData2 {
				return false
			}
			return true
		}
		num1, err1 := strconv.ParseFloat(string(data1), 64)
		num2, err2 := strconv.ParseFloat(string(data2), 64)
		if err1 != nil || err2 != nil {
			log.Fatalf("convert %s, %s to float failed, err1: %v, err2: %v", string(data1), string(data2), err1, err2)
		}

		if num1 == num2 {
			continue
		}
		if num1 > num2 {
			return false
		}
		return true

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
