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

package pkgdb

import (
	"database/sql"
	"fmt"
	"strconv"
	"strings"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
)

// ImplicitColName is tidb's implicit column's name
const ImplicitColName = "_tidb_rowid"

// ImplicitColID is tidb's implicit column's id
const ImplicitColID = -1

// CloseDB closes the mysql fd
func CloseDB(db *sql.DB) error {
	return errors.Trace(db.Close())
}

// GetCreateTableSQL gets the create table sql.
func GetCreateTableSQL(db *sql.DB, schemaName string, tableName string) (string, error) {
	query := fmt.Sprintf("SHOW CREATE TABLE `%s`.`%s`", schemaName, tableName)
	row := db.QueryRow(query)

	var tbl, createTable sql.NullString
	err := row.Scan(&tbl, &createTable)
	if err != nil {
		return "", errors.Trace(err)
	}
	if !tbl.Valid || !createTable.Valid {
		return "", errors.NewNotFound(nil, fmt.Sprintf("table %s not exist", tableName))
	}
	return createTable.String, nil
}

// GetRowCount returns row count of the table by given where condition.
func GetRowCount(db *sql.DB, dbname string, table string, where string) (int64, error) {
	/*
		select count example result:
		mysql> SELECT count(1) cnt from `test`.`itest` where id > 0;
		+------+
		| cnt  |
		+------+
		|  100 |
		+------+
	*/

	query := fmt.Sprintf("SELECT count(1) cnt from `%s`.`%s` where %s", dbname, table, where)
	rows, err := db.Query(query)
	if err != nil {
		return 0, errors.Trace(err)
	}
	defer rows.Close()

	var fields map[string][]byte
	if rows.Next() {
		fields, err = ScanRow(rows)
		if err != nil {
			return 0, errors.Trace(err)
		}
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

// FindSuitableIndex returns a suitableIndex
func FindSuitableIndex(db *sql.DB, dbName string, table string) (*model.ColumnInfo, error) {
	rowsData, err := ShowIndex(db, dbName, table)
	if err != nil {
		return nil, errors.Trace(err)
	}

	tableInfo, err := GetTableInfo(db, dbName, table)
	if err != nil {
		return nil, errors.Trace(err)
	}

	// seek pk
	for _, fields := range rowsData {
		if string(fields["Key_name"]) == "PRIMARY" && string(fields["Seq_in_index"]) == "1" {
			column, valid := GetColumnByName(tableInfo, string(fields["Column_name"]))
			if !valid {
				return nil, errors.Errorf("can't find column %s in %s.%s", string(fields["Column_name"]), dbName, table)
			}
			return column, nil
		}
	}

	// no pk found, seek unique index
	for _, fields := range rowsData {
		if string(fields["Non_unique"]) == "0" && string(fields["Seq_in_index"]) == "1" {
			column, valid := GetColumnByName(tableInfo, string(fields["Column_name"]))
			if !valid {
				return nil, errors.Errorf("can't find column %s in %s.%s", string(fields["Column_name"]), dbName, table)
			}
			return column, nil
		}
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
					return nil, errors.Errorf("can't find column %s in %s.%s", string(fields["Column_name"]), dbName, table)
				}
				maxCardinality = cardinality
				c = column
			}
		}
	}

	return c, nil
}

// GetOrderKey return some columns for order
func GetOrderKey(tbInfo *model.TableInfo) ([]string, []*model.ColumnInfo) {
	keys := make([]string, 0, 2)
	keyCols := make([]*model.ColumnInfo, 0, 2)

	for _, index := range tbInfo.Indices {
		if index.Primary {
			for _, indexCol := range index.Columns {
				keys = append(keys, indexCol.Name.O)
			}
		}
	}

	if len(keys) == 0 {
		// no primary key found, use all fields as order by key
		for _, col := range tbInfo.Columns {
			keys = append(keys, col.Name.O)
			keyCols = append(keyCols, col)
		}
	} else {
		for _, col := range tbInfo.Columns {
			for _, key := range keys {
				if col.Name.O == key {
					keyCols = append(keyCols, col)
				}
			}
		}
	}
	return keys, keyCols
}

// GetFirstColumn returns the first column in the table
func GetFirstColumn(db *sql.DB, dbname string, table string) (*model.ColumnInfo, error) {
	tableInfo, err := GetTableInfo(db, dbname, table)
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
	log.Infof("get random values sql: %s", query)
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

// GetColumnByName returns a column by column name
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

// GetTables gets all table in the schema
func GetTables(db *sql.DB, dbName string) ([]string, error) {
	rs, err := QuerySQL(db, fmt.Sprintf("show tables in `%s`;", dbName))
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

// DescribeTable is the struct for describe table
type DescribeTable struct {
	Field   string
	Type    string
	Null    string
	Key     string
	Default interface{}
	Extra   interface{}
}

// Scan scan rows for desctibe table
func (desc *DescribeTable) Scan(rows *sql.Rows) error {
	err := rows.Scan(&desc.Field, &desc.Type, &desc.Null, &desc.Key, &desc.Default, &desc.Extra)
	return errors.Trace(err)
}

// GetTableSchema returns DescribeTable
func GetTableSchema(db *sql.DB, tblName string) ([]DescribeTable, error) {
	stmt := fmt.Sprintf("describe %s;", tblName)
	rows, err := QuerySQL(db, stmt)
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer rows.Close()

	var descs []DescribeTable
	for rows.Next() {
		var desc DescribeTable
		err1 := desc.Scan(rows)
		if err1 != nil {
			return nil, errors.Trace(err1)
		}
		descs = append(descs, desc)
	}
	return descs, err
}

// GetCRC32Checksum returns the checksum of some data
func GetCRC32Checksum(db *sql.DB, schemaName string, tbInfo *model.TableInfo, limitRange string) (string, error) {
	columnNames := make([]string, 0, len(tbInfo.Columns))
	for _, col := range tbInfo.Columns {
		columnNames = append(columnNames, col.Name.O)
	}

	query := fmt.Sprintf("SELECT CRC32(GROUP_CONCAT(CONCAT_WS(',', %s) SEPARATOR  ' + ')) AS checksum FROM `%s`.`%s` WHERE %s;", strings.Join(columnNames, ", "), schemaName, tbInfo.Name.O, limitRange)
	rows, err := db.Query(query)
	if err != nil {
		return "", errors.Trace(err)
	}
	defer rows.Close()

	for rows.Next() {
		var checksum sql.NullString
		err = rows.Scan(&checksum)
		if err != nil {
			return "", errors.Trace(err)
		}
		return checksum.String, nil
	}

	return "", nil
}

// QuerySQL querys sql, and returns some row
func QuerySQL(db *sql.DB, query string) (*sql.Rows, error) {
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

// SetSnapshot set the snapshot variable for tidb
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

// IsNumberType returns true if is number type
func IsNumberType(tp byte) bool {
	switch tp {
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeLong, mysql.TypeLonglong, mysql.TypeInt24:
		return true
	}

	return false
}

// IsFloatType returns true if is float type
func IsFloatType(tp byte) bool {
	switch tp {
	case mysql.TypeFloat, mysql.TypeDouble, mysql.TypeNewDecimal:
		return true
	}

	return false
}
