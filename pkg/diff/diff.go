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
	"container/heap"
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb-tools/pkg/dbutil"
)

// TableInstance record a table instance
type TableInstance struct {
	Conn   *sql.DB
	Schema string
	Table  string
	info   *model.TableInfo
}

// TableDiff saves config for diff table
type TableDiff struct {
	// source tables
	SourceTables []*TableInstance
	// target table
	TargetTable *TableInstance

	// columns be ignored
	IgnoreColumns []string

	// columns be removed
	RemoveColumns []string

	// field should be the primary key, unique key or field with index
	Field string

	// select range, for example: "age > 10 AND age < 20"
	Range string

	// for example, the whole data is [1...100]
	// we can split these data to [1...10], [11...20], ..., [91...100]
	// the [1...10] is a chunk, and it's chunk size is 10
	// size of the split chunk
	ChunkSize int

	// sampling check percent, for example 10 means only check 10% data
	Sample int

	// how many goroutines are created to check data
	CheckThreadCount int

	// set true if target-db and source-db all support tidb implicit column "_tidb_rowid"
	UseRowID bool

	// set false if want to comapre the data directly
	UseChecksum bool

	// collation config in mysql/tidb, should corresponding to charset.
	Collation string

	// ignore check table's struct
	IgnoreStructCheck bool

	// ignore check table's data
	IgnoreDataCheck bool

	sqlCh chan string

	wg sync.WaitGroup
}

// Equal tests whether two database have same data and schema.
func (t *TableDiff) Equal(ctx context.Context, writeFixSQL func(string) error) (bool, bool, error) {
	t.adjustConfig()

	t.sqlCh = make(chan string)
	t.wg.Add(1)
	go func() {
		t.WriteSqls(ctx, writeFixSQL)
		t.wg.Done()
	}()

	structEqual := true
	dataEqual := true
	var err error

	if !t.IgnoreStructCheck {
		structEqual, err = t.CheckTableStruct(ctx)
		if err != nil {
			return false, false, errors.Trace(err)
		}
	}

	if !t.IgnoreDataCheck {
		dataEqual, err = t.CheckTableData(ctx)
		if err != nil {
			return false, false, errors.Trace(err)
		}
	}

	t.sqlCh <- "end"
	t.wg.Wait()
	return structEqual, dataEqual, nil
}

// CheckTableStruct checks table's struct
func (t *TableDiff) CheckTableStruct(ctx context.Context) (bool, error) {
	tableInfo, err := dbutil.GetTableInfoWithRowID(ctx, t.TargetTable.Conn, t.TargetTable.Schema, t.TargetTable.Table, t.UseRowID)
	if err != nil {
		return false, errors.Trace(err)
	}
	t.TargetTable.info = removeColumns(tableInfo, t.RemoveColumns)

	for _, sourceTable := range t.SourceTables {
		tableInfo, err := dbutil.GetTableInfoWithRowID(ctx, sourceTable.Conn, sourceTable.Schema, sourceTable.Table, t.UseRowID)
		if err != nil {
			return false, errors.Trace(err)
		}
		sourceTable.info = removeColumns(tableInfo, t.RemoveColumns)
		eq := dbutil.EqualTableInfo(sourceTable.info, t.TargetTable.info)
		if !eq {
			return false, nil
		}
	}

	return true, nil
}

func (t *TableDiff) adjustConfig() {
	if t.ChunkSize <= 0 {
		t.ChunkSize = 100
	}

	if len(t.Range) == 0 {
		t.Range = "true"
	}
	if t.Sample <= 0 {
		t.Sample = 100
	}

	if t.CheckThreadCount <= 0 {
		t.CheckThreadCount = 4
	}
}

// CheckTableData checks table's data
func (t *TableDiff) CheckTableData(ctx context.Context) (bool, error) {
	return t.EqualTableData(ctx)
}

// EqualTableData checks data is equal or not.
func (t *TableDiff) EqualTableData(ctx context.Context) (bool, error) {
	allJobs, err := GenerateCheckJob(t.TargetTable, t.Field, t.Range, t.ChunkSize, t.Sample, t.Collation)
	if err != nil {
		return false, errors.Trace(err)
	}

	checkNums := len(allJobs) * t.Sample / 100
	checkNumArr := getRandomN(len(allJobs), checkNums)
	log.Infof("total has %d check jobs, check %d of them", len(allJobs), len(checkNumArr))
	if checkNums == 0 {
		return true, nil
	}

	checkResultCh := make(chan bool, t.CheckThreadCount)
	defer close(checkResultCh)

	for i := 0; i < t.CheckThreadCount; i++ {
		checkJobs := make([]*CheckJob, 0, len(checkNumArr))
		for j := len(checkNumArr) * i / t.CheckThreadCount; j < len(checkNumArr)*(i+1)/t.CheckThreadCount && j < len(checkNumArr); j++ {
			checkJobs = append(checkJobs, allJobs[checkNumArr[j]])
		}
		go func(checkJobs []*CheckJob) {
			eq, err := t.checkChunkDataEqual(ctx, checkJobs)
			if err != nil {
				log.Errorf("check chunk data equal failed, error %v", errors.ErrorStack(err))
			}
			checkResultCh <- eq
		}(checkJobs)
	}

	num := 0
	equal := true

CheckResult:
	for {
		select {
		case eq := <-checkResultCh:
			num++
			if !eq {
				equal = false
			}
			if num == t.CheckThreadCount {
				break CheckResult
			}
		case <-ctx.Done():
			return equal, nil
		}
	}
	return equal, nil
}

func (t *TableDiff) getSourceTableChecksum(ctx context.Context, job *CheckJob) (int64, error) {
	var checksum int64

	for _, sourceTable := range t.SourceTables {
		checksumTmp, err := dbutil.GetCRC32Checksum(ctx, sourceTable.Conn, sourceTable.Schema, sourceTable.Table, t.TargetTable.info, job.Where, job.Args, SliceToMap(t.IgnoreColumns))
		if err != nil {
			return -1, errors.Trace(err)
		}

		checksum ^= checksumTmp
	}
	return checksum, nil
}

func (t *TableDiff) checkChunkDataEqual(ctx context.Context, checkJobs []*CheckJob) (bool, error) {
	equal := true
	if len(checkJobs) == 0 {
		return true, nil
	}

	for _, job := range checkJobs {
		if t.UseChecksum {
			// first check the checksum is equal or not
			sourceChecksum, err := t.getSourceTableChecksum(ctx, job)
			if err != nil {
				return false, errors.Trace(err)
			}

			targetChecksum, err := dbutil.GetCRC32Checksum(ctx, t.TargetTable.Conn, t.TargetTable.Schema, t.TargetTable.Table, t.TargetTable.info, job.Where, job.Args, SliceToMap(t.IgnoreColumns))
			if err != nil {
				return false, errors.Trace(err)
			}
			if sourceChecksum == targetChecksum {
				log.Infof("table: %s, range: %s, args: %v, checksum is equal, checksum: %d", job.Table, job.Where, job.Args, sourceChecksum)
				continue
			}

			log.Errorf("table: %s, range: %s, args: %v, checksum is not equal, one is %d, another is %d", job.Table, job.Where, job.Args, sourceChecksum, targetChecksum)
		}

		// if checksum is not equal or don't need compare checksum, compare the data
		sourceRows := make(map[string]*sql.Rows)
		for i, sourceTable := range t.SourceTables {
			rows, _, err := getChunkRows(ctx, sourceTable.Conn, sourceTable.Schema, sourceTable.Table, sourceTable.info, job.Where, job.Args, SliceToMap(t.IgnoreColumns), t.Collation)
			if err != nil {
				return false, errors.Trace(err)
			}
			sourceRows[fmt.Sprintf("source-%d", i)] = rows
		}

		targetRows, orderKeyCols, err := getChunkRows(ctx, t.TargetTable.Conn, t.TargetTable.Schema, t.TargetTable.Table, t.TargetTable.info, job.Where, job.Args, SliceToMap(t.IgnoreColumns), t.Collation)
		if err != nil {
			return false, errors.Trace(err)
		}

		eq, err := t.compareRows(sourceRows, targetRows, orderKeyCols)
		if err != nil {
			return false, errors.Trace(err)
		}

		// if equal is false, we continue check data, we should find all the different data just run once
		if !eq {
			equal = false
		}
	}

	return equal, nil
}

func (t *TableDiff) compareRows(sourceRows map[string]*sql.Rows, targetRows *sql.Rows, orderKeyCols []*model.ColumnInfo) (bool, error) {
	var (
		equal     = true
		rowsData1 = make([]map[string][]byte, 0, 100)
		rowsData2 = make([]map[string][]byte, 0, 100)
		rowsNull1 = make([]map[string]bool, 0, 100)
		rowsNull2 = make([]map[string]bool, 0, 100)
	)

	rowDatas := &RowDatas{
		Rows:         make([]RowData, 0, len(sourceRows)),
		OrderKeyCols: orderKeyCols,
	}
	heap.Init(rowDatas)
	sourceMap := make(map[string]interface{})
	for {
		for source, rows := range sourceRows {
			if _, ok := sourceMap[source]; ok {
				continue
			}

			if rows.Next() {
				data, null, err := dbutil.ScanRow(rows)
				if err != nil {
					return false, errors.Trace(err)
				}
				heap.Push(rowDatas, RowData{
					Data:   data,
					Null:   null,
					Source: source,
				})
				sourceMap[source] = struct{}{}
			} else {
				rows.Close()
				delete(sourceRows, source)
			}
		}

		if rowDatas.Len() == 0 {
			break
		}

		rowData := heap.Pop(rowDatas).(RowData)
		rowsData1 = append(rowsData1, rowData.Data)
		rowsNull1 = append(rowsNull1, rowData.Null)
		delete(sourceMap, rowData.Source)
	}

	for targetRows.Next() {
		data2, null2, err := dbutil.ScanRow(targetRows)
		if err != nil {
			return false, errors.Trace(err)
		}
		rowsData2 = append(rowsData2, data2)
		rowsNull2 = append(rowsNull2, null2)
	}
	targetRows.Close()

	var index1, index2 int
	for {
		if index1 == len(rowsData1) {
			// all the rowsData2's data should be deleted
			for ; index2 < len(rowsData2); index2++ {
				sql := generateDML("delete", rowsData2[index2], rowsNull2[index2], orderKeyCols, t.TargetTable.info, t.TargetTable.Schema)
				log.Infof("[delete] sql: %v", sql)
				t.wg.Add(1)
				t.sqlCh <- sql
				equal = false
			}
			break
		}
		if index2 == len(rowsData2) {
			// rowsData2 lack some data, should insert them
			for ; index1 < len(rowsData1); index1++ {
				sql := generateDML("replace", rowsData1[index1], rowsNull1[index1], orderKeyCols, t.TargetTable.info, t.TargetTable.Schema)
				log.Infof("[insert] sql: %v", sql)
				t.wg.Add(1)
				t.sqlCh <- sql
				equal = false
			}
			break
		}
		eq, cmp, err := compareData(rowsData1[index1], rowsData2[index2], rowsNull1[index1], rowsNull2[index2], orderKeyCols)
		if err != nil {
			return false, errors.Trace(err)
		}
		if eq {
			index1++
			index2++
			continue
		}
		equal = false
		switch cmp {
		case 1:
			// delete
			sql := generateDML("delete", rowsData2[index2], rowsNull2[index2], orderKeyCols, t.TargetTable.info, t.TargetTable.Schema)
			log.Infof("[delete] sql: %s", sql)
			t.wg.Add(1)
			t.sqlCh <- sql
			index2++
		case -1:
			// insert
			sql := generateDML("replace", rowsData1[index1], rowsNull1[index1], orderKeyCols, t.TargetTable.info, t.TargetTable.Schema)
			log.Infof("[insert] sql: %s", sql)
			t.wg.Add(1)
			t.sqlCh <- sql
			index1++
		case 0:
			// update
			sql := generateDML("replace", rowsData1[index1], rowsNull1[index1], orderKeyCols, t.TargetTable.info, t.TargetTable.Schema)
			log.Infof("[update] sql: %s", sql)
			t.wg.Add(1)
			t.sqlCh <- sql
			index1++
			index2++
		}
	}

	return equal, nil
}

// WriteSqls write sqls to file
func (t *TableDiff) WriteSqls(ctx context.Context, writeFixSQL func(string) error) {
	for {
		select {
		case dml, ok := <-t.sqlCh:
			if !ok || dml == "end" {
				return
			}

			err := writeFixSQL(fmt.Sprintf("%s\n", dml))
			if err != nil {
				log.Errorf("write sql: %s failed, error: %v", dml, err)
			}
			t.wg.Done()
		case <-ctx.Done():
			return
		}
	}
}

func generateDML(tp string, data map[string][]byte, null map[string]bool, keys []*model.ColumnInfo, table *model.TableInfo, schema string) (sql string) {
	switch tp {
	case "replace":
		colNames := make([]string, 0, len(table.Columns))
		values := make([]string, 0, len(table.Columns))
		for _, col := range table.Columns {
			colNames = append(colNames, fmt.Sprintf("`%s`", col.Name.O))
			if null[col.Name.O] {
				values = append(values, "NULL")
				continue
			}

			if needQuotes(col.FieldType) {
				values = append(values, fmt.Sprintf("'%s'", string(data[col.Name.O])))
			} else {
				values = append(values, string(data[col.Name.O]))
			}
		}

		sql = fmt.Sprintf("REPLACE INTO `%s`.`%s`(%s) VALUES (%s);", schema, table.Name, strings.Join(colNames, ","), strings.Join(values, ","))
	case "delete":
		kvs := make([]string, 0, len(keys))
		for _, col := range keys {
			if null[col.Name.O] {
				kvs = append(kvs, fmt.Sprintf("`%s` is NULL", col.Name.O))
				continue
			}

			if needQuotes(col.FieldType) {
				kvs = append(kvs, fmt.Sprintf("`%s` = '%s'", col.Name.O, string(data[col.Name.O])))
			} else {
				kvs = append(kvs, fmt.Sprintf("`%s` = %s", col.Name.O, string(data[col.Name.O])))
			}
		}
		sql = fmt.Sprintf("DELETE FROM `%s`.`%s` WHERE %s;", schema, table.Name, strings.Join(kvs, " AND "))
	default:
		log.Errorf("unknown sql type %s", tp)
	}

	return
}

func compareData(map1, map2 map[string][]byte, null1, null2 map[string]bool, orderKeyCols []*model.ColumnInfo) (bool, int32, error) {
	var (
		equal        = true
		data1, data2 []byte
		key          string
		ok           bool
		cmp          int32
	)

	for key, data1 = range map1 {
		if data2, ok = map2[key]; !ok {
			return false, 0, errors.Errorf("don't have key %s", key)
		}
		if (string(data1) == string(data2)) && (null1[key] == null2[key]) {
			continue
		}
		equal = false
		if null1[key] == null2[key] {
			log.Errorf("find difference data in column %s, data1: %s, data2: %s", key, map1, map2)
		} else {
			log.Errorf("find difference data in column %s, one of them is NULL, data1: %s, data2: %s", key, map1, map2)
		}
		break
	}
	if equal {
		return true, 0, nil
	}

	for _, col := range orderKeyCols {
		if data1, ok = map1[col.Name.O]; !ok {
			return false, 0, errors.Errorf("don't have key %s", col.Name.O)
		}
		if data2, ok = map2[col.Name.O]; !ok {
			return false, 0, errors.Errorf("don't have key %s", col.Name.O)
		}
		if needQuotes(col.FieldType) {
			if string(data1) > string(data2) {
				cmp = 1
				break
			} else if string(data1) < string(data2) {
				cmp = -1
				break
			} else {
				continue
			}
		} else {
			num1, err1 := strconv.ParseFloat(string(data1), 64)
			num2, err2 := strconv.ParseFloat(string(data2), 64)
			if err1 != nil || err2 != nil {
				return false, 0, errors.Errorf("convert %s, %s to float failed, err1: %v, err2: %v", string(data1), string(data2), err1, err2)
			}
			if num1 > num2 {
				cmp = 1
				break
			} else if num1 < num2 {
				cmp = -1
				break
			} else {
				continue
			}
		}
	}

	return false, cmp, nil
}

func getChunkRows(ctx context.Context, db *sql.DB, schema, table string, tableInfo *model.TableInfo, where string,
	args []interface{}, ignoreColumns map[string]interface{}, collation string) (*sql.Rows, []*model.ColumnInfo, error) {
	orderKeys, orderKeyCols := dbutil.SelectUniqueOrderKey(tableInfo)
	columns := "*"

	if len(ignoreColumns) != 0 {
		columnNames := make([]string, 0, len(tableInfo.Columns))
		for _, col := range tableInfo.Columns {
			if _, ok := ignoreColumns[col.Name.O]; ok {
				continue
			}
			columnNames = append(columnNames, col.Name.O)
		}
		columns = strings.Join(columnNames, ", ")
	}

	if orderKeys[0] == dbutil.ImplicitColName {
		columns = fmt.Sprintf("%s, %s", columns, dbutil.ImplicitColName)
	}

	if collation != "" {
		collation = fmt.Sprintf(" COLLATE \"%s\"", collation)
	}

	query := fmt.Sprintf("SELECT /*!40001 SQL_NO_CACHE */ %s FROM `%s`.`%s` WHERE %s ORDER BY %s%s",
		columns, schema, table, where, strings.Join(orderKeys, ","), collation)

	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	return rows, orderKeyCols, nil
}
