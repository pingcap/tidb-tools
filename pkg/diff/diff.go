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

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb-tools/pkg/dbutil"
	"github.com/pingcap/tidb-tools/pkg/utils"
	"go.uber.org/zap"
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
	Fields string

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

	// get tidb statistics information from which table instance. if is nil, will split chunk by random.
	TiDBStatsSource *TableInstance

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

	err := t.getTableInfo(ctx)
	if err != nil {
		return false, false, errors.Trace(err)
	}

	structEqual := true
	dataEqual := true

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
	for _, sourceTable := range t.SourceTables {
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
		t.Range = "TRUE"
	}
	if t.Sample <= 0 {
		t.Sample = 100
	}

	if t.CheckThreadCount <= 0 {
		t.CheckThreadCount = 4
	}
}

func (t *TableDiff) getTableInfo(ctx context.Context) error {
	tableInfo, err := dbutil.GetTableInfoWithRowID(ctx, t.TargetTable.Conn, t.TargetTable.Schema, t.TargetTable.Table, t.UseRowID)
	if err != nil {
		return errors.Trace(err)
	}
	t.TargetTable.info = removeColumns(tableInfo, t.RemoveColumns)

	for _, sourceTable := range t.SourceTables {
		tableInfo, err := dbutil.GetTableInfoWithRowID(ctx, sourceTable.Conn, sourceTable.Schema, sourceTable.Table, t.UseRowID)
		if err != nil {
			return errors.Trace(err)
		}
		sourceTable.info = removeColumns(tableInfo, t.RemoveColumns)
	}

	return nil
}

// CheckTableData checks table's data
func (t *TableDiff) CheckTableData(ctx context.Context) (bool, error) {
	return t.EqualTableData(ctx)
}

// EqualTableData checks data is equal or not.
func (t *TableDiff) EqualTableData(ctx context.Context) (equal bool, err error) {
	var allJobs []*CheckJob

	table := t.TargetTable
	useTiDB := false

	if t.TiDBStatsSource != nil {
		table = t.TiDBStatsSource
		useTiDB = true
	}

	allJobs, err = GenerateCheckJob(table, t.Fields, t.Range, t.ChunkSize, t.Collation, useTiDB)

	if err != nil {
		return false, errors.Trace(err)
	}

	checkNums := len(allJobs) * t.Sample / 100
	checkNumArr := getRandomN(len(allJobs), checkNums)
	log.Info("check jobs", zap.Int("total job num", len(allJobs)), zap.Int("check job num", len(checkNumArr)))
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
				log.Error("check chunk data equal failed", zap.Error(err))
			}
			checkResultCh <- eq
		}(checkJobs)
	}

	num := 0
	equal = true

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
		checksumTmp, err := dbutil.GetCRC32Checksum(ctx, sourceTable.Conn, sourceTable.Schema, sourceTable.Table, t.TargetTable.info, job.Where, utils.StringsToInterfaces(job.Args), utils.SliceToMap(t.IgnoreColumns))
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

			targetChecksum, err := dbutil.GetCRC32Checksum(ctx, t.TargetTable.Conn, t.TargetTable.Schema, t.TargetTable.Table, t.TargetTable.info, job.Where, utils.StringsToInterfaces(job.Args), utils.SliceToMap(t.IgnoreColumns))
			if err != nil {
				return false, errors.Trace(err)
			}
			if sourceChecksum == targetChecksum {
				log.Info("checksum is equal", zap.String("table", dbutil.TableName(job.Schema, job.Table)), zap.String("where", job.Where), zap.Reflect("args", job.Args), zap.Int64("checksum", sourceChecksum))
				continue
			}

			log.Warn("checksum is not equal", zap.String("table", dbutil.TableName(job.Schema, job.Table)), zap.String("where", job.Where), zap.Reflect("args", job.Args), zap.Int64("source checksum", sourceChecksum), zap.Int64("target checksum", targetChecksum))
		}

		// if checksum is not equal or don't need compare checksum, compare the data
		log.Info("select data and then check data", zap.String("table", dbutil.TableName(job.Schema, job.Table)), zap.String("where", job.Where), zap.Reflect("args", job.Args))
		sourceRows := make(map[string][]map[string]*dbutil.ColumnData)
		for i, sourceTable := range t.SourceTables {
			rows, _, err := getChunkRows(ctx, sourceTable.Conn, sourceTable.Schema, sourceTable.Table, sourceTable.info, job.Where, utils.StringsToInterfaces(job.Args), utils.SliceToMap(t.IgnoreColumns), t.Collation)
			if err != nil {
				return false, errors.Trace(err)
			}
			sourceRows[fmt.Sprintf("source-%d", i)] = rows
		}

		targetRows, orderKeyCols, err := getChunkRows(ctx, t.TargetTable.Conn, t.TargetTable.Schema, t.TargetTable.Table, t.TargetTable.info, job.Where, utils.StringsToInterfaces(job.Args), utils.SliceToMap(t.IgnoreColumns), t.Collation)
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

func (t *TableDiff) compareRows(sourceRows map[string][]map[string]*dbutil.ColumnData, targetRows []map[string]*dbutil.ColumnData, orderKeyCols []*model.ColumnInfo) (bool, error) {
	var (
		equal     = true
		rowsData1 = make([]map[string]*dbutil.ColumnData, 0, 100)
		rowsData2 = make([]map[string]*dbutil.ColumnData, 0, 100)
	)

	rowDatas := &RowDatas{
		Rows:         make([]RowData, 0, len(sourceRows)),
		OrderKeyCols: orderKeyCols,
	}
	heap.Init(rowDatas)
	sourceMap := make(map[string]interface{})
	sourceOffset := make(map[string]int)
	for {
		for source, rows := range sourceRows {
			if _, ok := sourceMap[source]; ok {
				continue
			}
			if sourceOffset[source] == len(rows) {
				delete(sourceRows, source)
				continue
			}

			data := rows[sourceOffset[source]]
			heap.Push(rowDatas, RowData{
				Data:   data,
				Source: source,
			})
			sourceMap[source] = struct{}{}
			sourceOffset[source]++
		}

		if rowDatas.Len() == 0 {
			break
		}

		rowData := heap.Pop(rowDatas).(RowData)
		rowsData1 = append(rowsData1, rowData.Data)
		delete(sourceMap, rowData.Source)
	}

	rowsData2 = targetRows

	var index1, index2 int
	for {
		if index1 == len(rowsData1) {
			// all the rowsData2's data should be deleted
			for ; index2 < len(rowsData2); index2++ {
				sql := generateDML("delete", rowsData2[index2], orderKeyCols, t.TargetTable.info, t.TargetTable.Schema)
				log.Info("[delete]", zap.String("sql", sql))
				t.wg.Add(1)
				t.sqlCh <- sql
				equal = false
			}
			break
		}
		if index2 == len(rowsData2) {
			// rowsData2 lack some data, should insert them
			for ; index1 < len(rowsData1); index1++ {
				sql := generateDML("replace", rowsData1[index1], orderKeyCols, t.TargetTable.info, t.TargetTable.Schema)
				log.Info("[insert]", zap.String("sql", sql))
				t.wg.Add(1)
				t.sqlCh <- sql
				equal = false
			}
			break
		}
		eq, cmp, err := compareData(rowsData1[index1], rowsData2[index2], orderKeyCols)
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
			sql := generateDML("delete", rowsData2[index2], orderKeyCols, t.TargetTable.info, t.TargetTable.Schema)
			log.Info("[delete]", zap.String("sql", sql))
			t.wg.Add(1)
			t.sqlCh <- sql
			index2++
		case -1:
			// insert
			sql := generateDML("replace", rowsData1[index1], orderKeyCols, t.TargetTable.info, t.TargetTable.Schema)
			log.Info("[insert]", zap.String("sql", sql))
			t.wg.Add(1)
			t.sqlCh <- sql
			index1++
		case 0:
			// update
			sql := generateDML("replace", rowsData1[index1], orderKeyCols, t.TargetTable.info, t.TargetTable.Schema)
			log.Info("[update]", zap.String("sql", sql))
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
				log.Error("write sql failed", zap.String("sql", dml), zap.Error(err))
			}
			t.wg.Done()
		case <-ctx.Done():
			return
		}
	}
}

func generateDML(tp string, data map[string]*dbutil.ColumnData, keys []*model.ColumnInfo, table *model.TableInfo, schema string) (sql string) {
	switch tp {
	case "replace":
		colNames := make([]string, 0, len(table.Columns))
		values := make([]string, 0, len(table.Columns))
		for _, col := range table.Columns {
			colNames = append(colNames, fmt.Sprintf("`%s`", col.Name.O))
			if data[col.Name.O].IsNull {
				values = append(values, "NULL")
				continue
			}

			if needQuotes(col.FieldType) {
				values = append(values, fmt.Sprintf("'%s'", string(data[col.Name.O].Data)))
			} else {
				values = append(values, string(data[col.Name.O].Data))
			}
		}

		sql = fmt.Sprintf("REPLACE INTO `%s`.`%s`(%s) VALUES (%s);", schema, table.Name, strings.Join(colNames, ","), strings.Join(values, ","))
	case "delete":
		kvs := make([]string, 0, len(keys))
		for _, col := range keys {
			if data[col.Name.O].IsNull {
				kvs = append(kvs, fmt.Sprintf("`%s` is NULL", col.Name.O))
				continue
			}

			if needQuotes(col.FieldType) {
				kvs = append(kvs, fmt.Sprintf("`%s` = '%s'", col.Name.O, string(data[col.Name.O].Data)))
			} else {
				kvs = append(kvs, fmt.Sprintf("`%s` = %s", col.Name.O, string(data[col.Name.O].Data)))
			}
		}
		sql = fmt.Sprintf("DELETE FROM `%s`.`%s` WHERE %s;", schema, table.Name, strings.Join(kvs, " AND "))
	default:
		log.Error("unknown sql type", zap.String("type", tp))
	}

	return
}

func compareData(map1, map2 map[string]*dbutil.ColumnData, orderKeyCols []*model.ColumnInfo) (bool, int32, error) {
	var (
		equal        = true
		data1, data2 *dbutil.ColumnData
		key          string
		ok           bool
		cmp          int32
	)

	for key, data1 = range map1 {
		if data2, ok = map2[key]; !ok {
			return false, 0, errors.Errorf("don't have key %s", key)
		}
		if (string(data1.Data) == string(data2.Data)) && (data1.IsNull == data2.IsNull) {
			continue
		}
		equal = false
		if data1.IsNull == data2.IsNull {
			log.Error("find difference data", zap.String("column", key), zap.Reflect("data1", map1), zap.Reflect("data2", map2))
		} else {
			log.Error("find difference data, one of them is NULL", zap.String("column", key), zap.Reflect("data1", map1), zap.Reflect("data2", map2))
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
			strData1 := string(data1.Data)
			strData2 := string(data2.Data)

			if len(strData1) == len(strData2) && strData1 == strData2 {
				continue
			}

			if strData1 < strData2 {
				cmp = -1
			} else if strData1 > strData2 {
				cmp = 1
			}
			break

		} else {
			num1, err1 := strconv.ParseFloat(string(data1.Data), 64)
			num2, err2 := strconv.ParseFloat(string(data2.Data), 64)
			if err1 != nil || err2 != nil {
				return false, 0, errors.Errorf("convert %s, %s to float failed, err1: %v, err2: %v", string(data1.Data), string(data2.Data), err1, err2)
			}

			if num1 == num2 {
				continue
			}

			if num1 < num2 {
				cmp = -1
			} else if num1 > num2 {
				cmp = 1
			}
			break
		}
	}

	return false, cmp, nil
}

func getChunkRows(ctx context.Context, db *sql.DB, schema, table string, tableInfo *model.TableInfo, where string,
	args []interface{}, ignoreColumns map[string]interface{}, collation string) ([]map[string]*dbutil.ColumnData, []*model.ColumnInfo, error) {
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

	log.Debug("select data", zap.String("sql", query), zap.Reflect("args", args))
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	defer rows.Close()

	datas := make([]map[string]*dbutil.ColumnData, 0, 100)
	for rows.Next() {
		data, err := dbutil.ScanRow(rows)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		datas = append(datas, data)
	}

	return datas, orderKeyCols, errors.Trace(rows.Err())
}
