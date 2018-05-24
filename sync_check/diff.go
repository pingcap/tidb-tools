// Copyright 2016 PingCAP, Inc.
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
	"bytes"
	"database/sql"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb-tools/pkg/db"
	"github.com/pingcap/tidb-tools/sync_check/util"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/types"
)

// Diff contains two sql DB, used for comparing.
type Diff struct {
	db1          *sql.DB
	db2          *sql.DB
	dbName       string
	chunkSize    int
	sample       int
	checkThCount int
	useRowID     bool
	tables       []*TableCheckCfg
	fixSqlFile   *os.File
	sqlCh        chan string
	wg           sync.WaitGroup
}

// NewDiff returns a Diff instance.
func NewDiff(db1, db2 *sql.DB, dbName string, chunkSize, sample, checkThCount int,
	useRowID bool, tables []*TableCheckCfg, filename, snapshot string) (diff *Diff, err error) {
	diff = &Diff{
		db1:          db1,
		db2:          db2,
		dbName:       dbName,
		chunkSize:    chunkSize,
		sample:       sample,
		checkThCount: checkThCount,
		useRowID:     useRowID,
		tables:       tables,
		sqlCh:        make(chan string),
	}
	for _, table := range diff.tables {
		table.Info, err = pkgdb.GetSchemaTable(diff.db1, diff.dbName, table.Name)
		if err != nil {
			return nil, errors.Trace(err)
		}
		table.Schema = diff.dbName
	}
	diff.fixSqlFile, err = os.Create(filename)
	if err != nil {
		return nil, errors.Trace(err)
	}

	if snapshot != "" {
		err = pkgdb.SetSnapshot(db1, snapshot)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}

	return diff, nil
}

// Equal tests whether two database have same data and schema.
func (df *Diff) Equal() (equal bool, err error) {
	defer func() {
		df.fixSqlFile.Close()
		df.db1.Close()
		df.db2.Close()
	}()

	df.wg.Add(1)
	go df.WriteSqls()

	equal = true
	tbls1, err := pkgdb.GetTables(df.db1, df.dbName)
	if err != nil {
		err = errors.Trace(err)
		return
	}

	tbls2, err := pkgdb.GetTables(df.db2, df.dbName)
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
			table := &TableCheckCfg{Name: name, Schema: df.dbName}
			table.Info, err = pkgdb.GetSchemaTable(df.db1, df.dbName, name)
			if err != nil {
				return false, errors.Trace(err)
			}

			df.tables = append(df.tables, table)
		}
	}

	for _, table := range df.tables {
		eq, err = df.EqualIndex(table.Name)
		if err != nil {
			err = errors.Trace(err)
			return
		}
		if !eq {
			log.Errorf("table have different index: %s\n", table.Name)
			equal = false
		}

		eq, err = df.EqualTable(table)
		if err != nil || !eq {
			err = errors.Trace(err)
			equal = false
			return
		}
	}

	for {
		if len(df.sqlCh) == 0 {
			close(df.sqlCh)
			break
		}
		time.Sleep(time.Second)
	}

	df.wg.Wait()
	return
}

// EqualTable tests whether two database table have same data and schema.
func (df *Diff) EqualTable(table *TableCheckCfg) (bool, error) {
	eq, err := df.equalCreateTable(table.Name)
	if err != nil {
		return eq, errors.Trace(err)
	}
	if !eq {
		log.Errorf("table have different structure: %s\n", table.Name)
		return eq, err
	}

	eq, err = df.equalTableData(table)
	if err != nil {
		return eq, errors.Trace(err)
	}
	if !eq {
		log.Errorf("table data different: %s\n", table.Name)
	}
	return eq, err
}

// EqualIndex tests whether two database index are same.
func (df *Diff) EqualIndex(tblName string) (bool, error) {
	index1, err := pkgdb.ShowIndex(df.db1, df.dbName, tblName)
	if err != nil {
		return false, errors.Trace(err)
	}

	index2, err := pkgdb.ShowIndex(df.db2, df.dbName, tblName)
	if err != nil {
		return false, errors.Trace(err)
	}

	eq := true
	for i, index := range index1 {
		keyName1 := string(index["Key_name"])
		keyName2 := string(index2[i]["Key_name"])
		columnName1 := string(index["Column_name"])
		columnName2 := string(index2[i]["Column_name"])
		if keyName1 != keyName2 || columnName1 != columnName2 {
			eq = false
			break
		}
	}

	return eq, nil
}

func (df *Diff) equalCreateTable(tblName string) (bool, error) {
	_, err1 := pkgdb.GetCreateTable(df.db1, df.dbName, tblName)
	_, err2 := pkgdb.GetCreateTable(df.db2, df.dbName, tblName)

	if errors.IsNotFound(err1) && errors.IsNotFound(err2) {
		return true, nil
	}
	if err1 != nil {
		return false, errors.Trace(err1)
	}
	if err2 != nil {
		return false, errors.Trace(err2)
	}

	// TODO ignore table schema currently
	// return table1 == table2, nil
	return true, nil
}

func (df *Diff) equalTableData(table *TableCheckCfg) (bool, error) {
	dumpJobs, err := util.GenerateDumpJob(df.db1, df.dbName, table.Name, table.Field, table.Range, df.chunkSize, df.sample, df.useRowID)
	if err != nil {
		return false, errors.Trace(err)
	}

	checkNums := len(dumpJobs) * df.sample / 100
	if checkNums == 0 {
		checkNums = 1
	}
	checkNumArr := getRandomN(len(dumpJobs), checkNums)
	log.Infof("total has %d check jobs, check %+v", len(dumpJobs), checkNumArr)

	checkResultCh := make(chan bool, df.checkThCount)
	defer close(checkResultCh)

	for i := 0; i < df.checkThCount; i++ {
		checkJobs := make([]*util.DumpJob, 0, checkNums)
		for j := checkNums * i / df.checkThCount; j < checkNums*(i+1)/df.checkThCount; j++ {
			checkJobs = append(checkJobs, dumpJobs[j])
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
			if num == df.checkThCount {
				break CheckResult
			}
		}
	}
	return equal, nil
}

func (df *Diff) checkChunkDataEqual(dumpJobs []*util.DumpJob, table *TableCheckCfg) (bool, error) {
	if len(dumpJobs) == 0 {
		return true, nil
	}

	for _, job := range dumpJobs {
		log.Infof("check table: %s, range: %s", job.Table, job.Where)

		rows1, orderKeyCols, err := getChunkRows(df.db1, df.dbName, table, job.Where, df.useRowID)
		if err != nil {
			return false, errors.Trace(err)
		}
		defer rows1.Close()

		rows2, _, err := getChunkRows(df.db2, df.dbName, table, job.Where, df.useRowID)
		if err != nil {
			return false, errors.Trace(err)
		}
		defer rows2.Close()

		cols1, err := rows1.Columns()
		if err != nil {
			return false, errors.Trace(err)
		}
		cols2, err := rows2.Columns()
		if err != nil {
			return false, errors.Trace(err)
		}
		if len(cols1) != len(cols2) {
			return false, errors.Trace(err)
		}

		eq, err := df.compareRows(rows1, rows2, orderKeyCols, table)
		if err != nil {
			return false, errors.Trace(err)
		}
		if !eq {
			return false, nil
		}
	}

	return true, nil
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
			// the all rowsData2's data should be deleted
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
			// rosData2 lack some data, should insert them
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
			log.Infof("[delete] sql: %v", sql)
			df.wg.Add(1)
			df.sqlCh <- sql
			index2++
		case -1:
			// insert
			sql := generateDML("replace", rowsData1[index1], orderKeyCols, table.Info, table.Schema)
			log.Infof("[insert] sql: %v", sql)
			df.wg.Add(1)
			df.sqlCh <- sql
			index1++
		case 0:
			// update
			deleteSql := generateDML("delete", rowsData2[index2], orderKeyCols, table.Info, table.Schema)
			replaceSql := generateDML("replace", rowsData1[index1], orderKeyCols, table.Info, table.Schema)
			log.Infof("[update] delete: %s,\n replace: %s", deleteSql, replaceSql)
			df.wg.Add(2)
			df.sqlCh <- deleteSql
			df.sqlCh <- replaceSql
			index1++
			index2++
		}
	}

	return equal, nil
}

func (df *Diff) WriteSqls() {
	for {
		select {
		case dml, ok := <-df.sqlCh:
			if !ok {
				df.wg.Done()
				return
			}
			_, err := df.fixSqlFile.WriteString(fmt.Sprintf("%s\n", dml))
			if err != nil {
				log.Errorf("write sql: %s failed, error: %v", dml, err)
			}
			df.wg.Done()
		default:
		}
	}
}

func generateDML(tp string, data map[string][]byte, keys []*model.ColumnInfo, table *model.TableInfo, dbName string) (sql string) {
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

		sql = fmt.Sprintf("REPLACE INTO `%s`.`%s`(%s) VALUES (%s);", dbName, table.Name, strings.Join(colNames, ","), strings.Join(values, ","))
	case "delete":
		kvs := make([]string, 0, len(keys))
		for _, col := range keys {
			if needQuotes(col.FieldType) {
				kvs = append(kvs, fmt.Sprintf("%s = \"%s\"", col.Name.O, string(data[col.Name.O])))
			} else {
				kvs = append(kvs, fmt.Sprintf("%s = %s", col.Name.O, string(data[col.Name.O])))
			}
		}
		sql = fmt.Sprintf("DELETE FROM `%s`.`%s` where %s;", dbName, table.Name, strings.Join(kvs, " AND "))
	default:
		log.Errorf("unknow sql type %s", tp)
	}

	return
}

func needQuotes(ft types.FieldType) bool {
	switch ft.Tp {
	case mysql.TypeTimestamp, mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24,
		mysql.TypeLong, mysql.TypeLonglong, mysql.TypeFloat, mysql.TypeDouble:
		return false
	default:
		return true
	}

	return false
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
	if cmp == 1 {
		return false, 1, nil
	} else if cmp == -1 {
		return false, -1, nil
	} else {
		return false, 0, nil
	}
}

func equalRows(rows1, rows2 *sql.Rows, row1, row2 comparableSQLRow, pks []string) (bool, error) {
	dataEqual := true
	for rows1.Next() {
		if !rows2.Next() {
			// rows2 count less than rows1
			log.Error("rows count different")
			return false, nil
		}

		eq, err := equalOneRow(rows1, rows2, row1, row2)
		if err != nil {
			return false, errors.Trace(err)
		}
		if !eq {
			dataEqual = false
		}
	}
	if rows2.Next() {
		// rows1 count less than rows2
		log.Error("rows count different")
		return false, nil
	}
	if !dataEqual {
		log.Error("rows count equal, but not every row equal!")
		return false, nil
	}
	return true, nil
}

func equalOneRow(rows1, rows2 *sql.Rows, row1, row2 comparableSQLRow) (bool, error) {
	err := row1.Scan(rows1)
	if err != nil {
		return false, errors.Trace(err)
	}

	row2.Scan(rows2)
	if err != nil {
		return false, errors.Trace(err)
	}
	if row1.Equal(row2) {
		return true, nil
	}

	log.Errorf("row1 %s not equal row2 %s!", row1, row2)
	return false, nil
}

func getChunkRows(db *sql.DB, dbName string, table *TableCheckCfg, where string, useRowID bool) (*sql.Rows, []*model.ColumnInfo, error) {
	orderKeys, orderKeyCols := pkgdb.GetOrderKey(table.Info, useRowID)

	query := fmt.Sprintf("SELECT %s * FROM `%s`.`%s` WHERE %s ORDER BY %s",
		"/*!40001 SQL_NO_CACHE */", dbName, table.Name, where, strings.Join(orderKeys, ","))

	rows, err := pkgdb.QuerySQL(db, query)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	return rows, orderKeyCols, nil
}

type comparableSQLRow interface {
	sqlRow
	comparable
}

type sqlRow interface {
	Scan(*sql.Rows) error
}

type comparable interface {
	Equal(comparable) bool
}

type rawBytesRow []sql.RawBytes

func (r rawBytesRow) Len() int {
	return len([]sql.RawBytes(r))
}

func (r rawBytesRow) Scan(rows *sql.Rows) error {
	args := make([]interface{}, len(r))
	for i := 0; i < len(args); i++ {
		args[i] = &r[i]
	}

	err := rows.Scan(args...)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (r rawBytesRow) Equal(data comparable) bool {
	r2, ok := data.(rawBytesRow)
	if !ok {
		return false
	}
	if r.Len() != r2.Len() {
		return false
	}
	for i := 0; i < r.Len(); i++ {
		if bytes.Compare(r[i], r2[i]) != 0 {
			return false
		}
	}
	return true
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
		return nil
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
