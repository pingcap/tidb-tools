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
	"database/sql"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb-tools/pkg/db"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/types"
)

// Diff contains two sql DB, used for comparing.
type Diff struct {
	db1              *sql.DB
	db2              *sql.DB
	schema           string
	chunkSize        int
	sample           int
	checkThreadCount int
	useRowID         bool
	tables           []*TableCheckCfg
	fixSQLFile       *os.File
	sqlCh            chan string
	wg               sync.WaitGroup
}

// NewDiff returns a Diff instance.
func NewDiff(db1, db2 *sql.DB, cfg *Config) (diff *Diff, err error) {
	diff = &Diff{
		db1:              db1,
		db2:              db2,
		schema:           cfg.SourceDBCfg.Schema,
		chunkSize:        cfg.ChunkSize,
		sample:           cfg.Sample,
		checkThreadCount: cfg.CheckThreadCount,
		useRowID:         cfg.UseRowID,
		tables:           cfg.Tables,
		sqlCh:            make(chan string),
	}
	for _, table := range diff.tables {
		table.Info, err = pkgdb.GetTableInfoWithRowID(diff.db1, diff.schema, table.Name, cfg.UseRowID)
		if err != nil {
			return nil, errors.Trace(err)
		}
		table.Schema = diff.schema
	}
	diff.fixSQLFile, err = os.Create(cfg.FixSQLFile)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return diff, nil
}

// Equal tests whether two database have same data and schema.
func (df *Diff) Equal() (equal bool, err error) {
	defer func() {
		df.fixSQLFile.Close()
		df.db1.Close()
		df.db2.Close()
	}()

	df.wg.Add(1)
	go func() {
		df.WriteSqls()
		df.wg.Done()
	}()

	equal = true
	tbls1, err := pkgdb.GetTables(df.db1, df.schema)
	if err != nil {
		err = errors.Trace(err)
		return
	}

	tbls2, err := pkgdb.GetTables(df.db2, df.schema)
	if err != nil {
		err = errors.Trace(err)
		return
	}

	eq := equalStrings(tbls1, tbls2)
	// len(df.tables) == 0 means check all tables
	if !eq && len(df.tables) == 0 {
		log.Errorf("show tables get different table. [source db tables] %v [target db tables] %v", tbls1, tbls2)
		equal = false
	}

	if len(df.tables) == 0 {
		df.tables = make([]*TableCheckCfg, 0, len(tbls1))
		for _, name := range tbls1 {
			table := &TableCheckCfg{Name: name, Schema: df.schema}
			table.Info, err = pkgdb.GetTableInfoWithRowID(df.db1, df.schema, name, df.useRowID)
			if err != nil {
				return false, errors.Trace(err)
			}
			table.Schema = df.schema
			df.tables = append(df.tables, table)
		}
	}

	for _, table := range df.tables {
		tableInfo1 := table.Info
		tableInfo2, err := pkgdb.GetTableInfoWithRowID(df.db2, df.schema, table.Name, df.useRowID)
		if err != nil {
			return false, errors.Trace(err)
		}
		eq, err = df.EqualTableStruct(tableInfo1, tableInfo2)
		if err != nil {
			return equal, errors.Trace(err)
		}
		if !eq {
			log.Errorf("table have different index: %s\n", table.Name)
			equal = false
		}

		eq, err = df.EqualTableData(table)
		if err != nil {
			log.Errorf("equal table error %v", err)
		}
		if !eq {
			equal = false
			log.Errorf("table %s's data is not equal", table.Name)
		}
	}

	df.sqlCh <- "end"
	df.wg.Wait()
	return
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
func (df *Diff) EqualTableData(table *TableCheckCfg) (bool, error) {
	// TODO: now only check data between source data's min and max, need check data less than min and greater than max.
	allJobs, err := GenerateCheckJob(df.db1, df.schema, table.Info, table.Field, table.Range, df.chunkSize, df.sample, df.useRowID)
	if err != nil {
		return false, errors.Trace(err)
	}

	checkNums := len(allJobs) * df.sample / 100
	checkNumArr := getRandomN(len(allJobs), checkNums)
	log.Infof("total has %d check jobs, check %+v", len(allJobs), checkNumArr)

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
				log.Errorf("check chunk data equal failed, error %v", err)
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
		}
	}
	return equal, nil
}

func (df *Diff) checkChunkDataEqual(checkJobs []*CheckJob, table *TableCheckCfg) (bool, error) {
	equal := true
	if len(checkJobs) == 0 {
		return true, nil
	}

	for _, job := range checkJobs {
		// first check the checksum is equal or not
		orderKeys, _ := pkgdb.GetOrderKey(table.Info)
		checksum1, err := pkgdb.GetCRC32Checksum(df.db1, df.schema, table.Info, orderKeys, job.Where)
		if err != nil {
			return false, errors.Trace(err)
		}

		checksum2, err := pkgdb.GetCRC32Checksum(df.db2, df.schema, table.Info, orderKeys, job.Where)
		if err != nil {
			return false, errors.Trace(err)
		}
		if checksum1 == checksum2 {
			log.Infof("table: %s, range: %s checksum is equal, checksum: %s", job.Table, job.Where, checksum1)
			continue
		}

		// if checksum is not equal, compare the data
		log.Errorf("table: %s, range: %s checksum is not equal", job.Table, job.Where)
		rows1, orderKeyCols, err := getChunkRows(df.db1, df.schema, table, job.Where, df.useRowID)
		if err != nil {
			return false, errors.Trace(err)
		}
		defer rows1.Close()

		rows2, _, err := getChunkRows(df.db2, df.schema, table, job.Where, df.useRowID)
		if err != nil {
			return false, errors.Trace(err)
		}
		defer rows2.Close()

		eq, err := df.compareRows(rows1, rows2, orderKeyCols, table)
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

func (df *Diff) compareRows(rows1, rows2 *sql.Rows, orderKeyCols []*model.ColumnInfo, table *TableCheckCfg) (bool, error) {
	equal := true
	rowsData1 := make([]map[string][]byte, 0, 100)
	rowsData2 := make([]map[string][]byte, 0, 100)

	for rows1.Next() {
		data1, err := pkgdb.ScanRow(rows1)
		if err != nil {
			return false, errors.Trace(err)
		}
		rowsData1 = append(rowsData1, data1)
	}
	for rows2.Next() {
		data2, err := pkgdb.ScanRow(rows2)
		if err != nil {
			return false, errors.Trace(err)
		}
		rowsData2 = append(rowsData2, data2)
	}

	var index1, index2 int
	for {
		if index1 == len(rowsData1) {
			// all the rowsData2's data should be deleted
			for ; index2 < len(rowsData2); index2++ {
				sql := generateDML("delete", rowsData2[index2], orderKeyCols, table.Info, table.Schema)
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
				sql := generateDML("replace", rowsData1[index1], orderKeyCols, table.Info, table.Schema)
				log.Infof("[insert] sql: %v", sql)
				df.wg.Add(1)
				df.sqlCh <- sql
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
			sql := generateDML("delete", rowsData2[index2], orderKeyCols, table.Info, table.Schema)
			log.Infof("[delete] sql: %s", sql)
			df.wg.Add(1)
			df.sqlCh <- sql
			index2++
		case -1:
			// insert
			sql := generateDML("replace", rowsData1[index1], orderKeyCols, table.Info, table.Schema)
			log.Infof("[insert] sql: %s", sql)
			df.wg.Add(1)
			df.sqlCh <- sql
			index1++
		case 0:
			// update
			sql := generateDML("replace", rowsData1[index1], orderKeyCols, table.Info, table.Schema)
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
		}
	}
}

func generateDML(tp string, data map[string][]byte, keys []*model.ColumnInfo, table *model.TableInfo, schema string) (sql string) {
	// TODO: can't distinguish NULL between ""
	switch tp {
	case "replace":
		colNames := make([]string, 0, len(table.Columns))
		values := make([]string, 0, len(table.Columns))
		for _, col := range table.Columns {
			colNames = append(colNames, col.Name.O)
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
			if needQuotes(col.FieldType) {
				kvs = append(kvs, fmt.Sprintf("%s = \"%s\"", col.Name.O, string(data[col.Name.O])))
			} else {
				kvs = append(kvs, fmt.Sprintf("%s = %s", col.Name.O, string(data[col.Name.O])))
			}
		}
		sql = fmt.Sprintf("DELETE FROM `%s`.`%s` where %s;", schema, table.Name, strings.Join(kvs, " AND "))
	default:
		log.Errorf("unknow sql type %s", tp)
	}

	return
}

func needQuotes(ft types.FieldType) bool {
	return !(pkgdb.IsNumberType(ft.Tp) || pkgdb.IsFloatType(ft.Tp))
}

func compareData(map1 map[string][]byte, map2 map[string][]byte, orderKeyCols []*model.ColumnInfo) (bool, int32, error) {
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
		if string(data1) == string(data2) {
			continue
		}
		equal = false
		log.Errorf("find difference data, data1: %s, data2: %s", map1, map2)
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

func getChunkRows(db *sql.DB, schema string, table *TableCheckCfg, where string, useRowID bool) (*sql.Rows, []*model.ColumnInfo, error) {
	orderKeys, orderKeyCols := pkgdb.GetOrderKey(table.Info)
	columnNames := make([]string, 0, len(table.Info.Columns))
	for _, col := range table.Info.Columns {
		if pkgdb.IsBitType(col.Tp) {
			columnNames = append(columnNames, fmt.Sprintf("`%s`+0 AS `%s`", col.Name.O, col.Name.O))
		} else {
			columnNames = append(columnNames, fmt.Sprintf("`%s`", col.Name.O))
		}
	}
	if orderKeys[0] == pkgdb.ImplicitColName {
		columnNames = append(columnNames, pkgdb.ImplicitColName)
	}
	query := fmt.Sprintf("SELECT /*!40001 SQL_NO_CACHE */ %s FROM `%s`.`%s` WHERE %s ORDER BY %s",
		strings.Join(columnNames, ", "), schema, table.Name, where, strings.Join(orderKeys, ","))

	rows, err := pkgdb.QuerySQL(db, query)
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
