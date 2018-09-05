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

package main

import (
	"container/heap"
	"context"
	"database/sql"
	"fmt"
	"math/rand"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb-tools/pkg/dbutil"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/types"
)

// Diff contains two sql DB, used for comparing.
type Diff struct {
	sourceDBs        map[string]DBConfig
	targetDB         DBConfig
	chunkSize        int
	sample           int
	checkThreadCount int
	useRowID         bool
	useChecksum      bool
	tables           map[string]map[string]*TableConfig
	fixSQLFile       *os.File
	sqlCh            chan string
	wg               sync.WaitGroup
	report           *Report

	ctx context.Context
}

// NewDiff returns a Diff instance.
func NewDiff(ctx context.Context, cfg *Config) (diff *Diff, err error) {
	diff = &Diff{
		sourceDBs:        make(map[string]DBConfig),
		chunkSize:        cfg.ChunkSize,
		sample:           cfg.Sample,
		checkThreadCount: cfg.CheckThreadCount,
		useRowID:         cfg.UseRowID,
		useChecksum:      cfg.UseChecksum,
		tables:           make(map[string]map[string]*TableConfig),
		sqlCh:            make(chan string),
		report:           NewReport(),
		ctx:              ctx,
	}

	if err = diff.init(cfg); err != nil {
		diff.Close()
		return nil, errors.Trace(err)
	}

	return diff, nil
}

func (df *Diff) init(cfg *Config) (err error) {
	// create connection for source.
	for _, source := range cfg.SourceDBCfg {
		source.Conn, err = dbutil.OpenDB(source.DBConfig)
		if err != nil {
			return errors.Errorf("create source db %+v error %v", cfg.SourceDBCfg, err)
		}
		source.Conn.SetMaxOpenConns(cfg.CheckThreadCount)
		source.Conn.SetMaxIdleConns(cfg.CheckThreadCount)

		if source.Snapshot != "" {
			err = dbutil.SetSnapshot(df.ctx, source.Conn, source.Snapshot)
			if err != nil {
				return errors.Errorf("set history snapshot %s for source db %+v error %v", source.Snapshot, cfg.SourceDBCfg, err)
			}
		}
		df.sourceDBs[source.Label] = source
	}

	// create connection for target.
	cfg.TargetDBCfg.Conn, err = dbutil.OpenDB(cfg.TargetDBCfg.DBConfig)
	if err != nil {
		return errors.Errorf("create target db %+v error %v", cfg.TargetDBCfg, err)
	}
	cfg.TargetDBCfg.Conn.SetMaxOpenConns(cfg.CheckThreadCount)
	cfg.TargetDBCfg.Conn.SetMaxIdleConns(cfg.CheckThreadCount)

	if cfg.TargetDBCfg.Snapshot != "" {
		err = dbutil.SetSnapshot(df.ctx, cfg.TargetDBCfg.Conn, cfg.TargetDBCfg.Snapshot)
		if err != nil {
			return errors.Errorf("set history snapshot %s for target db %+v error %v", cfg.TargetDBCfg.Snapshot, cfg.TargetDBCfg, err)
		}
	}
	df.targetDB = cfg.TargetDBCfg

	// fill the table information.
	// will add default source information, don't worry, we will use table config's info replace this later.
	for _, schemaTables := range cfg.Tables {
		df.tables[schemaTables.Schema] = make(map[string]*TableConfig)
		allTables, err := dbutil.GetTables(df.ctx, df.targetDB.Conn, schemaTables.Schema)
		if err != nil {
			return errors.Errorf("get tables from %s.%s error %v", df.targetDB.Label, schemaTables.Schema, errors.Trace(err))
		}

		for _, table := range schemaTables.Tables {
			if table[0] == '~' {
				tableRegex := regexp.MustCompile(fmt.Sprintf("(?i)%s", table[1:]))
				for _, tableName := range allTables {
					if !tableRegex.MatchString(tableName) {
						continue
					}
					tableInfo, err := dbutil.GetTableInfoWithRowID(df.ctx, df.targetDB.Conn, schemaTables.Schema, tableName, cfg.UseRowID)
					if err != nil {
						return errors.Errorf("get table %s.%s's inforamtion error %v", schemaTables.Schema, tableName, errors.Trace(err))
					}
					df.tables[schemaTables.Schema][tableName] = &TableConfig{
						TableInstance: TableInstance{
							Schema: schemaTables.Schema,
							Table:  tableName,
						},
						Info:  tableInfo,
						Range: "TRUE",
						SourceTables: []TableInstance{{
							DBLabel: cfg.SourceDBCfg[0].Label,
							Schema:  schemaTables.Schema,
							Table:   tableName,
						}},
					}
				}

			} else {
				tableInfo, err := dbutil.GetTableInfoWithRowID(df.ctx, df.targetDB.Conn, schemaTables.Schema, table, cfg.UseRowID)
				if err != nil {
					return errors.Errorf("get table %s.%s's inforamtion error %v", schemaTables.Schema, table, errors.Trace(err))
				}
				df.tables[schemaTables.Schema][table] = &TableConfig{
					TableInstance: TableInstance{
						Schema: schemaTables.Schema,
						Table:  table,
					},
					Info:  tableInfo,
					Range: "TRUE",
					SourceTables: []TableInstance{{
						DBLabel: cfg.SourceDBCfg[0].Label,
						Schema:  schemaTables.Schema,
						Table:   table,
					}},
				}
			}
		}
	}

	for _, table := range cfg.TableCfgs {
		if _, ok := df.tables[table.Schema]; !ok {
			return errors.Errorf("schema %s not found in check tables", table.Schema)
		}
		if _, ok := df.tables[table.Schema][table.Table]; !ok {
			return errors.Errorf("table %s.%s not found in check tables", table.Schema, table.Table)
		}

		sourceTables := make([]TableInstance, 0, len(table.SourceTables))
		for _, sourceTable := range table.SourceTables {
			if sourceTable.Table[0] == '~' {
				allTables, err := dbutil.GetTables(df.ctx, df.sourceDBs[sourceTable.DBLabel].Conn, sourceTable.Schema)
				if err != nil {
					return errors.Errorf("get tables from %s.%s error %v", df.targetDB.Label, sourceTable.Schema, errors.Trace(err))
				}

				tableRegex := regexp.MustCompile(fmt.Sprintf("(?i)%s", sourceTable.Table[1:]))

				for _, tableName := range allTables {
					if !tableRegex.MatchString(tableName) {
						continue
					}

					sourceTables = append(sourceTables, TableInstance{
						DBLabel: sourceTable.DBLabel,
						Schema:  sourceTable.Schema,
						Table:   tableName,
					})
				}
			} else {
				sourceTables = append(sourceTables, TableInstance{
					DBLabel: sourceTable.DBLabel,
					Schema:  sourceTable.Schema,
					Table:   sourceTable.Table,
				})
			}
		}

		if len(sourceTables) != 0 {
			df.tables[table.Schema][table.Table].SourceTables = sourceTables
		}
		if table.Range != "" {
			df.tables[table.Schema][table.Table].Range = table.Range
		}
		df.tables[table.Schema][table.Table].Field = table.Field
	}

	df.fixSQLFile, err = os.Create(cfg.FixSQLFile)
	if err != nil {
		return errors.Trace(err)
	}

	return nil
}

// Close closes file and database connection.
func (df *Diff) Close() {
	if df.fixSQLFile != nil {
		df.fixSQLFile.Close()
	}

	for _, db := range df.sourceDBs {
		if db.Conn != nil {
			db.Conn.Close()
		}
	}

	if df.targetDB.Conn != nil {
		df.targetDB.Conn.Close()
	}
}

// Equal tests whether two database have same data and schema.
func (df *Diff) Equal() (err error) {
	defer df.Close()

	df.wg.Add(1)
	go func() {
		df.WriteSqls()
		df.wg.Done()
	}()

	reportResult := func(structEqual, dataEqual bool) {
		if structEqual && dataEqual {
			df.report.PassNum++
		} else {
			df.report.FailedNum++
		}
	}

	for _, schema := range df.tables {
		for _, table := range schema {
			structEqual, err := df.CheckTableStruct(table)
			if err != nil {
				return errors.Trace(err)
			}
			df.report.SetTableStructCheckResult(table.Table, structEqual)
			if !structEqual {
				log.Errorf("table have different struct: %s\n", table.Table)

				// if table struct not equal, we skip check data.
				reportResult(false, false)
				continue
			}

			dataEqual, err := df.EqualTableData(table)
			if err != nil {
				log.Errorf("equal table error %v", err)
				return errors.Trace(err)
			}
			df.report.SetTableDataCheckResult(table.Table, dataEqual)
			if !dataEqual {
				log.Errorf("table %s's data is not equal", table.Table)
			}

			reportResult(structEqual, dataEqual)
		}
	}

	df.sqlCh <- "end"
	df.wg.Wait()
	return
}

// CheckTableStruct checks table's struct is equal or not.
func (df *Diff) CheckTableStruct(table *TableConfig) (bool, error) {
	structEqual := true
	targetTableInfo := table.Info

	for _, sourceTable := range table.SourceTables {
		conn := df.sourceDBs[sourceTable.DBLabel].Conn
		sourceTableInfo, err := dbutil.GetTableInfoWithRowID(df.ctx, conn, sourceTable.Schema, sourceTable.Table, df.useRowID)
		if err != nil {
			return false, errors.Trace(err)
		}
		eq, err := df.EqualTableStruct(sourceTableInfo, targetTableInfo)
		if err != nil {
			return false, errors.Trace(err)
		}

		if !eq {
			structEqual = false
		}
	}

	return structEqual, nil
}

// EqualTableStruct tests whether two table's struct are same.
func (df *Diff) EqualTableStruct(tableInfo1, tableInfo2 *model.TableInfo) (bool, error) {
	// check columns
	if len(tableInfo1.Columns) != len(tableInfo2.Columns) {
		return false, nil
	}

	for j, col := range tableInfo1.Columns {
		if col.Name.O != tableInfo2.Columns[j].Name.O {
			return false, nil
		}
		if col.Tp != tableInfo2.Columns[j].Tp {
			return false, nil
		}
	}

	// check index
	if len(tableInfo1.Indices) != len(tableInfo2.Indices) {
		return false, nil
	}

	for i, index := range tableInfo1.Indices {
		index2 := tableInfo2.Indices[i]
		if index.Name.O != index2.Name.O {
			return false, nil
		}
		if len(index.Columns) != len(index2.Columns) {
			return false, nil
		}
		for j, col := range index.Columns {
			if col.Name.O != index2.Columns[j].Name.O {
				return false, nil
			}
		}
	}

	return true, nil
}

// EqualTableData checks data is equal or not.
func (df *Diff) EqualTableData(table *TableConfig) (bool, error) {
	allJobs, err := GenerateCheckJob(df.targetDB, table, df.chunkSize, df.sample, df.useRowID)
	if err != nil {
		return false, errors.Trace(err)
	}

	checkNums := len(allJobs) * df.sample / 100
	checkNumArr := getRandomN(len(allJobs), checkNums)
	log.Infof("total has %d check jobs, check %d of them", len(allJobs), len(checkNumArr))

	checkResultCh := make(chan bool, df.checkThreadCount)
	defer close(checkResultCh)

	for i := 0; i < df.checkThreadCount; i++ {
		checkJobs := make([]*CheckJob, 0, len(checkNumArr))
		for j := len(checkNumArr) * i / df.checkThreadCount; j < len(checkNumArr)*(i+1)/df.checkThreadCount && j < len(checkNumArr); j++ {
			checkJobs = append(checkJobs, allJobs[checkNumArr[j]])
		}
		go func() {
			eq, err := df.checkChunkDataEqual(checkJobs, table)
			if err != nil {
				log.Errorf("check chunk data equal failed, error %v", errors.ErrorStack(err))
			}
			checkResultCh <- eq
		}()
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
			if num == df.checkThreadCount {
				break CheckResult
			}
		case <-df.ctx.Done():
			return equal, nil
		}
	}
	return equal, nil
}

func (df *Diff) getSourceTableChecksum(table *TableConfig, job *CheckJob) (int64, error) {
	var checksum int64 = -1

	for _, sourceTable := range table.SourceTables {
		source := df.sourceDBs[sourceTable.DBLabel]
		checksumTmp, err := dbutil.GetCRC32Checksum(df.ctx, source.Conn, sourceTable.Schema, sourceTable.Table, table.Info, job.Where, job.Args)
		if err != nil {
			return -1, errors.Trace(err)
		}
		if checksum == -1 {
			checksum = checksumTmp
		} else {
			checksum ^= checksumTmp
		}
	}
	return checksum, nil
}

func (df *Diff) checkChunkDataEqual(checkJobs []*CheckJob, table *TableConfig) (bool, error) {
	equal := true
	if len(checkJobs) == 0 {
		return true, nil
	}

	for _, job := range checkJobs {
		if df.useChecksum {
			// first check the checksum is equal or not
			sourceChecksum, err := df.getSourceTableChecksum(table, job)
			if err != nil {
				return false, errors.Trace(err)
			}

			targetChecksum, err := dbutil.GetCRC32Checksum(df.ctx, df.targetDB.Conn, table.Schema, table.Table, table.Info, job.Where, job.Args)
			if err != nil {
				return false, errors.Trace(err)
			}
			if sourceChecksum == targetChecksum {
				log.Infof("table: %s, range: %s, args: %v, checksum is equal, checksum: %d", job.Table, job.Where, job.Args, sourceChecksum)
				continue
			}

			log.Errorf("table: %s, range: %s, args: %v, checksum is not equal, one is %d, another is %d", job.Table, job.Args, job.Where, sourceChecksum, targetChecksum)
		}

		// if checksum is not equal or don't need compare checksum, compare the data
		sourceRows := make([]*sql.Rows, 0, len(table.SourceTables))
		for _, sourceTable := range table.SourceTables {
			source := df.sourceDBs[sourceTable.DBLabel]
			rows, _, err := getChunkRows(df.ctx, source.Conn, sourceTable.Schema, sourceTable.Table, table.Info, job.Where, job.Args, df.useRowID)
			if err != nil {
				return false, errors.Trace(err)
			}
			sourceRows = append(sourceRows, rows)
		}

		targetRows, orderKeyCols, err := getChunkRows(df.ctx, df.targetDB.Conn, table.Schema, table.Table, table.Info, job.Where, job.Args, df.useRowID)
		if err != nil {
			return false, errors.Trace(err)
		}

		eq, err := df.compareRows(sourceRows, targetRows, orderKeyCols, table)
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

func (df *Diff) compareRows(sourceRows []*sql.Rows, targetRows *sql.Rows, orderKeyCols []*model.ColumnInfo, table *TableConfig) (bool, error) {
	var (
		equal     = true
		rowsData1 = make([]map[string][]byte, 0, 100)
		rowsData2 = make([]map[string][]byte, 0, 100)
		rowsNull1 = make([]map[string]bool, 0, 100)
		rowsNull2 = make([]map[string]bool, 0, 100)
	)

	rowDatas := new(RowDatas)
	heap.Init(rowDatas)
	for _, rows := range sourceRows {
		for rows.Next() {
			data, null, err := dbutil.ScanRow(rows)
			if err != nil {
				return false, errors.Trace(err)
			}
			heap.Push(rowDatas, RowData{
				Data:         data,
				Null:         null,
				OrderKeyCols: orderKeyCols,
			})
		}
		rows.Close()
	}
	for {
		if rowDatas.Len() == 0 {
			break
		}
		rowData := heap.Pop(rowDatas).(RowData)
		rowsData1 = append(rowsData1, rowData.Data)
		rowsNull1 = append(rowsNull1, rowData.Null)
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
				sql := generateDML("delete", rowsData2[index2], rowsNull2[index2], orderKeyCols, table.Info, table.Schema)
				log.Infof("[delete] sql: %v", sql)
				df.wg.Add(1)
				df.sqlCh <- sql
				equal = false
			}
			break
		}
		if index2 == len(rowsData2) {
			// rowsData2 lack some data, should insert them
			for ; index1 < len(rowsData1); index1++ {
				sql := generateDML("replace", rowsData1[index1], rowsNull1[index1], orderKeyCols, table.Info, table.Schema)
				log.Infof("[insert] sql: %v", sql)
				df.wg.Add(1)
				df.sqlCh <- sql
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
			sql := generateDML("delete", rowsData2[index2], rowsNull2[index2], orderKeyCols, table.Info, table.Schema)
			log.Infof("[delete] sql: %s", sql)
			df.wg.Add(1)
			df.sqlCh <- sql
			index2++
		case -1:
			// insert
			sql := generateDML("replace", rowsData1[index1], rowsNull1[index1], orderKeyCols, table.Info, table.Schema)
			log.Infof("[insert] sql: %s", sql)
			df.wg.Add(1)
			df.sqlCh <- sql
			index1++
		case 0:
			// update
			sql := generateDML("replace", rowsData1[index1], rowsNull1[index1], orderKeyCols, table.Info, table.Schema)
			log.Infof("[update] sql: %s", sql)
			df.wg.Add(1)
			df.sqlCh <- sql
			index1++
			index2++
		}
	}

	return equal, nil
}

// WriteSqls write sqls to file
func (df *Diff) WriteSqls() {
	for {
		select {
		case dml, ok := <-df.sqlCh:
			if !ok || dml == "end" {
				return
			}

			_, err := df.fixSQLFile.WriteString(fmt.Sprintf("%s\n", dml))
			if err != nil {
				log.Errorf("write sql: %s failed, error: %v", dml, err)
			}
			df.wg.Done()
		case <-df.ctx.Done():
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
				values = append(values, fmt.Sprintf("\"%s\"", string(data[col.Name.O])))
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
				kvs = append(kvs, fmt.Sprintf("`%s` = \"%s\"", col.Name.O, string(data[col.Name.O])))
			} else {
				kvs = append(kvs, fmt.Sprintf("`%s` = %s", col.Name.O, string(data[col.Name.O])))
			}
		}
		sql = fmt.Sprintf("DELETE FROM `%s`.`%s` WHERE %s;", schema, table.Name, strings.Join(kvs, " AND "))
	default:
		log.Errorf("unknow sql type %s", tp)
	}

	return
}

func needQuotes(ft types.FieldType) bool {
	return !(dbutil.IsNumberType(ft.Tp) || dbutil.IsFloatType(ft.Tp))
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
	args []interface{}, useRowID bool) (*sql.Rows, []*model.ColumnInfo, error) {
	orderKeys, orderKeyCols := dbutil.SelectUniqueOrderKey(tableInfo)
	columns := "*"
	if orderKeys[0] == dbutil.ImplicitColName {
		columns = fmt.Sprintf("*, %s", dbutil.ImplicitColName)
	}
	query := fmt.Sprintf("SELECT /*!40001 SQL_NO_CACHE */ %s FROM `%s`.`%s` WHERE %s ORDER BY %s",
		columns, schema, table, where, strings.Join(orderKeys, ","))

	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	return rows, orderKeyCols, nil
}

func equalStrings(str1, str2 []string) bool {
	if len(str1) != len(str2) {
		return false
	}
	for i := 0; i < len(str1); i++ {
		if str1[i] != str2[i] {
			return false
		}
	}
	return true
}

func getRandomN(total, num int) []int {
	if num > total {
		log.Warnf("the num %d is greater than total %d", num, total)
		num = total
	}

	totalArray := make([]int, 0, total)
	for i := 0; i < total; i++ {
		totalArray = append(totalArray, i)
	}

	for j := 0; j < num; j++ {
		r := j + rand.Intn(total-j)
		totalArray[j], totalArray[r] = totalArray[r], totalArray[j]
	}

	return totalArray[:num]
}
