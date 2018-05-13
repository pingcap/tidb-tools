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
	"strings"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb-tools/sync_check/util"
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
}

// NewDiff returns a Diff instance.
func NewDiff(db1, db2 *sql.DB, dbName string, chunkSize, sample, checkThCount int,
	useRowID bool, tables []*TableCheckCfg) *Diff {
	return &Diff{
		db1:          db1,
		db2:          db2,
		dbName:       dbName,
		chunkSize:    chunkSize,
		sample:       sample,
		checkThCount: checkThCount,
		useRowID:     useRowID,
		tables:       tables,
	}
}

// Equal tests whether two database have same data and schema.
func (df *Diff) Equal() (equal bool, err error) {
	equal = true
	tbls1, err := getTables(df.db1)
	if err != nil {
		err = errors.Trace(err)
		return
	}

	tbls2, err := getTables(df.db2)
	if err != nil {
		err = errors.Trace(err)
		return
	}

	eq := equalStrings(tbls1, tbls2)
	// len(df.tables) == 0 means check all tables
	if !eq && len(df.tables) == 0 {
		log.Infof("show tables get different table. [source db tables] %v [target db tables] %v", tbls1, tbls2)
		equal = false
	}

	if len(df.tables) == 0 {
		df.tables = make([]*TableCheckCfg, 0, len(tbls1))
		for _, name := range tbls1 {
			df.tables = append(df.tables, &TableCheckCfg{Name: name})
		}
	}

	for _, table := range df.tables {
		eq, err = df.EqualIndex(table.Name)
		if err != nil {
			err = errors.Trace(err)
			return
		}
		if !eq {
			log.Infof("table have different index: %s\n", table.Name)
			equal = false
		}

		eq, err = df.EqualTable(table)
		if err != nil || !eq {
			err = errors.Trace(err)
			equal = false
			return
		}
	}

	return
}

// EqualTable tests whether two database table have same data and schema.
func (df *Diff) EqualTable(table *TableCheckCfg) (bool, error) {
	eq, err := df.equalCreateTable(table.Name)
	if err != nil {
		return eq, errors.Trace(err)
	}
	if !eq {
		log.Infof("table have different schema: %s\n", table.Name)
		return eq, err
	}

	eq, err = df.equalTableData(table)
	if err != nil {
		return eq, errors.Trace(err)
	}
	if !eq {
		log.Infof("table data different: %s\n", table.Name)
	}
	return eq, err
}

// EqualIndex tests whether two database index are same.
func (df *Diff) EqualIndex(tblName string) (bool, error) {
	index1, err := getTableIndex(df.db1, tblName)
	if err != nil {
		return false, errors.Trace(err)
	}
	defer index1.Close()
	index2, err := getTableIndex(df.db2, tblName)
	if err != nil {
		return false, errors.Trace(err)
	}
	defer index2.Close()

	eq, err := equalRows(index1, index2, &showIndex{}, &showIndex{}, nil)
	if err != nil || !eq {
		return eq, errors.Trace(err)
	}
	return eq, nil
}

func (df *Diff) equalCreateTable(tblName string) (bool, error) {
	_, err1 := getCreateTable(df.db1, tblName)
	_, err2 := getCreateTable(df.db2, tblName)

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
			eq, err := df.checkChunkDataEqual(checkJobs, table.Name)
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

func (df *Diff) checkChunkDataEqual(dumpJobs []*util.DumpJob, tblName string) (bool, error) {
	if len(dumpJobs) == 0 {
		return true, nil
	}

	for _, job := range dumpJobs {
		log.Infof("table: %s, where: %s", job.Table, job.Where)

		rows1, pks, err := getChunkRows(df.db1, df.dbName, tblName, job.Where)
		if err != nil {
			return false, errors.Trace(err)
		}
		defer rows1.Close()

		rows2, _, err := getChunkRows(df.db2, df.dbName, tblName, job.Where)
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

		eq, err := compareRows(rows1, rows2, pks)
		if err != nil {
			return false, errors.Trace(err)
		}
		if !eq {
			return false, nil
		}
	}

	return true, nil
}

func compareRows(rows1, rows2 *sql.Rows, pks []string) (bool, error) {
	equal := true
	rowsData1 := make([]map[string][]byte, 0, 100)
	rowsData2 := make([]map[string][]byte, 0, 100)

	for rows1.Next() {
		data1, err := util.ScanRow(rows1)
		if err != nil {
			return false, errors.Trace(err)
		}
		rowsData1 = append(rowsData1, data1)
	}
	for rows2.Next() {
		data2, err := util.ScanRow(rows2)
		if err != nil {
			return false, errors.Trace(err)
		}
		rowsData2 = append(rowsData2, data2)
	}
	index1 := 0
	index2 := 0
	for {
		if index1 == len(rowsData1) || index2 == len(rowsData2) {
			break
		}
		eq, cmp, err := compareData(rowsData1[index1], rowsData2[index2], pks)
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
			log.Infof("delete %v", rowsData2[index2])
			index2++
			// delete
		case -1:
			log.Infof("insert %v", rowsData1[index1])
			index1++
			// insert
		case 0:
			log.Infof("replace %v", rowsData1[index1])
			index1++
			index2++
			// replace into
		}
	}
	return equal, nil
}

func compareData(map1 map[string][]byte, map2 map[string][]byte, pks []string) (bool, int32, error) {
	var (
		equal               = true
		data1, data2        []byte
		key, pkStr1, pkStr2 string
		ok                  bool
	)

	for key, data1 = range map1 {
		if data2, ok = map2[key]; !ok {
			return false, 0, errors.Errorf("don't have key %s", key)
		}
		if string(data1) == string(data2) {
			continue
		}
		equal = false
		break
	}
	if equal {
		return true, 0, nil
	}

	for _, pk := range pks {
		if data1, ok = map1[pk]; !ok {
			return false, 0, errors.Errorf("don't have key %s", pk)
		}
		if data2, ok = map2[pk]; !ok {
			return false, 0, errors.Errorf("don't have key %s", pk)
		}
		pkStr1 += string(data1)
		pkStr2 += string(data2)
	}
	if pkStr1 > pkStr2 {
		return false, 1, nil
	} else if pkStr1 < pkStr2 {
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

func getChunkRows(db *sql.DB, dbName, tblName string, where string) (*sql.Rows, []string, error) {
	descs, err := getTableSchema(db, tblName)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	pks := orderbyKey(descs)

	query := fmt.Sprintf("SELECT %s * FROM `%s`.`%s` WHERE %s ORDER BY %s",
		"/*!40001 SQL_NO_CACHE */", dbName, tblName, where, strings.Join(pks, ","))

	rows, err := querySQL(db, query)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	return rows, pks, nil
}

func getTableIndex(db *sql.DB, tblName string) (*sql.Rows, error) {
	rows, err := querySQL(db, fmt.Sprintf("show index from %s;", tblName))
	if err != nil {
		return nil, errors.Trace(err)
	}
	return rows, nil
}

func getTables(db *sql.DB) ([]string, error) {
	rs, err := querySQL(db, "show tables;")
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer rs.Close()

	var tbls []string
	for rs.Next() {
		var name string
		err := rs.Scan(&name)
		if err != nil {
			return nil, errors.Trace(err)
		}
		tbls = append(tbls, name)
	}
	return tbls, nil
}

func getCreateTable(db *sql.DB, tn string) (string, error) {
	stmt := fmt.Sprintf("show create table %s;", tn)
	rs, err := querySQL(db, stmt)
	if err != nil {
		return "", errors.Trace(err)
	}
	defer rs.Close()

	if rs.Next() {
		var (
			name string
			cs   string
		)
		err := rs.Scan(&name, &cs)
		return cs, errors.Trace(err)
	}
	return "", errors.NewNotFound(nil, "table not exist")
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

type showIndex struct {
	Table        sql.RawBytes
	NonUnique    sql.RawBytes
	KeyName      sql.RawBytes
	SeqInIndex   sql.RawBytes
	ColumnName   sql.RawBytes
	Collation    sql.RawBytes
	Cardinality  sql.RawBytes
	SubPart      sql.RawBytes
	Packed       sql.RawBytes
	Null         sql.RawBytes
	IndexType    sql.RawBytes
	Comment      sql.RawBytes
	IndexComment sql.RawBytes
}

func (si *showIndex) Scan(rows *sql.Rows) error {
	err := rows.Scan(&si.Table,
		&si.NonUnique,
		&si.KeyName,
		&si.SeqInIndex,
		&si.ColumnName,
		&si.Collation,
		&si.Cardinality,
		&si.SubPart,
		&si.Packed,
		&si.Null,
		&si.IndexType,
		&si.Comment,
		&si.IndexComment)
	return errors.Trace(err)
}

func (si *showIndex) Equal(data comparable) bool {
	si1, ok := data.(*showIndex)
	if !ok {
		return false
	}
	return bytes.Compare(si.Table, si1.Table) == 0 &&
		bytes.Compare(si.NonUnique, si1.NonUnique) == 0 &&
		bytes.Compare(si.KeyName, si1.KeyName) == 0 &&
		bytes.Compare(si.SeqInIndex, si1.SeqInIndex) == 0 &&
		bytes.Compare(si.ColumnName, si1.ColumnName) == 0 &&
		bytes.Compare(si.SubPart, si1.SubPart) == 0 &&
		bytes.Compare(si.Packed, si1.Packed) == 0
}

type describeTable struct {
	Field   string
	Type    string
	Null    string
	Key     string
	Default interface{}
	Extra   interface{}
}

func (desc *describeTable) Scan(rows *sql.Rows) error {
	err := rows.Scan(&desc.Field, &desc.Type, &desc.Null, &desc.Key, &desc.Default, &desc.Extra)
	return errors.Trace(err)
}

func getTableSchema(db *sql.DB, tblName string) ([]describeTable, error) {
	stmt := fmt.Sprintf("describe %s;", tblName)
	rows, err := querySQL(db, stmt)
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer rows.Close()

	var descs []describeTable
	for rows.Next() {
		var desc describeTable
		err1 := desc.Scan(rows)
		if err1 != nil {
			return nil, errors.Trace(err1)
		}
		descs = append(descs, desc)
	}
	return descs, err
}

func orderbyKey(descs []describeTable) []string {
	// TODO can't get the real primary key
	keys := make([]string, 0, 2)
	for _, desc := range descs {
		if desc.Key == "PRI" {
			keys = append(keys, desc.Field)
		}
	}
	if len(keys) == 0 {
		// if no primary key found, use all fields as order by key
		for _, desc := range descs {
			keys = append(keys, desc.Field)
		}
	}
	return keys
}

func querySQL(db *sql.DB, query string) (*sql.Rows, error) {
	var (
		err  error
		rows *sql.Rows
	)

	log.Debugf("[query][sql]%s", query)

	rows, err = db.Query(query)

	if err != nil {
		log.Errorf("query sql[%s] failed %v", query, errors.ErrorStack(err))
		return nil, errors.Trace(err)
	}
	return rows, nil

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

// ShowDatabases returns a database lists.
func ShowDatabases(db *sql.DB) ([]string, error) {
	var ret []string
	rows, err := querySQL(db, "show databases;")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var dbName string
		err := rows.Scan(&dbName)
		if err != nil {
			return nil, errors.Trace(err)
		}
		ret = append(ret, dbName)
	}
	return ret, nil
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
