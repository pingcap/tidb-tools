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

package pkgdb

import (
	"database/sql"
	"fmt"
	"strconv"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
)

const implicitColName = "_tidb_rowid"

// CloseDB close the mysql fd
func CloseDB(db *sql.DB) error {
	return errors.Trace(db.Close())
}

// GetCount get count rows of the table for specific field.
func GetCount(db *sql.DB, dbname string, table string, where string) (int64, error) {
	query := fmt.Sprintf("SELECT count(1) cnt from `%s`.`%s` where %s", dbname, table, where)
	rows, err := db.Query(query)
	if err != nil {
		return 0, errors.Trace(err)
	}
	defer rows.Close()

	var fields map[string][]byte
	if rows.Next() {
		fields, err = ScanRow(rows)
	}
	if err != nil {
		return 0, errors.Trace(err)
	}
	cntStr, ok := fields["cnt"]
	if !ok {
		return 0, errors.New("[dumper] `cnt` field not found in select count sql result")
	}
	cnt, err := strconv.ParseInt(string(cntStr), 10, 64)
	return cnt, errors.Trace(err)
}

// ShowIndex returns result of execute `show index`
func ShowIndex(db *sql.DB, dbname string, table string) ([]map[string][]byte, error) {
	/*
		show index example result:
		mysql> show index from test;
		+-------+------------+----------+--------------+-------------+-----------+-------------+----------+--------+------+------------+---------+---------------+
		| Table | Non_unique | Key_name | Seq_in_index | Column_name | Collation | Cardinality | Sub_part | Packed | Null | Index_type | Comment | Index_comment |
		+-------+------------+----------+--------------+-------------+-----------+-------------+----------+--------+------+------------+---------+---------------+
		| test  | 0          | PRIMARY  | 1            | id          | A         | 0           | NULL     | NULL   |      | BTREE      |         |               |
		| test  | 0          | aid      | 1            | aid         | A         | 0           | NULL     | NULL   | YES  | BTREE      |         |               |
		+-------+------------+----------+--------------+-------------+-----------+-------------+----------+--------+------+------------+---------+---------------+
	*/

	query := fmt.Sprintf("SHOW INDEX FROM `%s`.`%s`", dbname, table)
	rows, err := db.Query(query)
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer rows.Close()

	var rowsData []map[string][]byte
	for rows.Next() {
		fields, err1 := ScanRow(rows)
		if err1 != nil {
			return nil, errors.Trace(err1)
		}
		rowsData = append(rowsData, fields)
	}

	return rowsData, nil
}

func FindSuitableIndex(db *sql.DB, dbName string, table string, useRowID bool) (*model.ColumnInfo, error) {
	rowsData, err := ShowIndex(db, dbName, table)
	if err != nil {
		return nil, errors.Trace(err)
	}

	tableInfo, err := GetSchemaTable(db, dbName, table)
	if err != nil {
		return nil, errors.Trace(err)
	}

	// seek pk
	for _, fields := range rowsData {
		if string(fields["Key_name"]) == "PRIMARY" && string(fields["Seq_in_index"]) == "1" {
			column, valid := GetColumnByName(tableInfo, string(fields["Column_name"]))
			if !valid {
				return nil, errors.New(fmt.Sprintf("can't find column %s in %s.%s", string(fields["Column_name"]), dbName, table))
			}
			return column, nil
		}
	}

	// no pk found, seek unique index
	for _, fields := range rowsData {
		if string(fields["Non_unique"]) == "0" && string(fields["Seq_in_index"]) == "1" {
			column, valid := GetColumnByName(tableInfo, string(fields["Column_name"]))
			if !valid {
				return nil, errors.New(fmt.Sprintf("can't find column %s in %s.%s", string(fields["Column_name"]), dbName, table))
			}
			return column, nil
		}
	}

	if useRowID {
		newColumn := &model.ColumnInfo{
			Name: model.NewCIStr(implicitColName),
		}
		newColumn.Tp = mysql.TypeInt24
		return newColumn, nil
	}

	// no unique index found, seek index with max cardinality
	var c *model.ColumnInfo
	var maxCardinality int
	for _, fields := range rowsData {
		if string(fields["Seq_in_index"]) == "1" {
			cardinality, err := strconv.Atoi(string(fields["Cardinality"]))
			if err != nil {
				return nil, errors.Trace(err)
			}
			if cardinality > maxCardinality {
				column, valid := GetColumnByName(tableInfo, string(fields["Column_name"]))
				if !valid {
					return nil, errors.New(fmt.Sprintf("can't find column %s in %s.%s", string(fields["Column_name"]), dbName, table))
				}
				maxCardinality = cardinality
				c = column
			}
		}
	}

	return c, nil
}

// GetFirstColumn returns the first column in the table
func GetFirstColumn(db *sql.DB, dbname string, table string) (*model.ColumnInfo, error) {
	tableInfo, err := GetSchemaTable(db, dbname, table)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return tableInfo.Columns[0], nil
}

// GetRandomValues returns some random value of a table
func GetRandomValues(db *sql.DB, dbname string, table string, field string, num int64, min, max interface{}, timeRange string) ([]string, error) {
	randomValue := make([]string, 0, num)
	query := fmt.Sprintf("SELECT `%s` FROM (SELECT `%s` FROM `%s`.`%s` WHERE `%s` > \"%v\" AND `%s` < \"%v\" AND %s ORDER BY RAND() LIMIT %d)rand_tmp ORDER BY `%s`",
		field, field, dbname, table, field, min, field, max, timeRange, num, field)
	log.Infof("GetRandomValues sql: %s", query)
	rows, err := db.Query(query)
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer rows.Close()

	for rows.Next() {
		var value string
		err = rows.Scan(&value)
		if err != nil {
			return nil, errors.Trace(err)
		}
		randomValue = append(randomValue, value)
	}

	return randomValue, nil
}

func GetColumnByName(table *model.TableInfo, name string) (*model.ColumnInfo, bool) {
	var c *model.ColumnInfo
	for _, column := range table.Columns {
		if column.Name.O == name {
			c = column
			break
		}
	}

	if c != nil {
		return c, true
	}

	return nil, false
}

// ScanRowsToInterfaces scans rows to interfaces.
func ScanRowsToInterfaces(rows *sql.Rows) ([][]interface{}, error) {
	var rowsData [][]interface{}
	cols, err := rows.Columns()
	if err != nil {
		return nil, errors.Trace(err)
	}

	for rows.Next() {
		colVals := make([]interface{}, len(cols))
		colValsPtr := make([]interface{}, len(cols))
		for i := range colVals {
			colValsPtr[i] = &colVals[i]
		}

		err = rows.Scan(colValsPtr...)
		if err != nil {
			return nil, errors.Trace(err)
		}
		rowsData = append(rowsData, colVals)
	}

	return rowsData, nil
}

// ScanRow scans rows into a map.
func ScanRow(rows *sql.Rows) (map[string][]byte, error) {
	cols, err := rows.Columns()
	if err != nil {
		return nil, errors.Trace(err)
	}

	colVals := make([][]byte, len(cols))
	colValsI := make([]interface{}, len(colVals))
	for i := range colValsI {
		colValsI[i] = &colVals[i]
	}

	err = rows.Scan(colValsI...)
	if err != nil {
		return nil, errors.Trace(err)
	}

	result := make(map[string][]byte)
	for i := range colVals {
		result[cols[i]] = colVals[i]
	}

	return result, nil
}

func SetSnapshot(db *sql.DB, snapshot string) error {
	sql := fmt.Sprintf("set @@tidb_snapshot=\"%s\"", snapshot)
	log.Infof("set snapshot: %s", sql)
	result, err := db.Exec(sql)
	if err != nil {
		return errors.Trace(err)
	}
	log.Infof("set snapshot result: %v", result)

	return nil
}

func IsNumberType(tp byte) bool {
	switch tp {
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeLong, mysql.TypeLonglong, mysql.TypeInt24:
		return true
	}

	return false
}

func IsFloatType(tp byte) bool {
	switch tp {
	case mysql.TypeFloat, mysql.TypeDouble, mysql.TypeNewDecimal:
		return true
	}

	return false
}
