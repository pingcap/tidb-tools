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

const (
	// ImplicitColName is tidb's implicit column's name
	ImplicitColName = "_tidb_rowid"

	// ImplicitColID is tidb's implicit column's id
	ImplicitColID = -1
)

// DBConfig is the DB configuration.
type DBConfig struct {
	Host string `toml:"host" json:"host"`

	Port int `toml:"port" json:"port"`

	User string `toml:"user" json:"user"`

	Password string `toml:"password" json:"password"`

	Schema string `toml:"schema" json:"schema"`
}

// String returns string of database config
func (c *DBConfig) String() string {
	if c == nil {
		return "<nil>"
	}
	return fmt.Sprintf("DBConfig(%+v)", *c)
}

// CreateDB create a mysql fd
func CreateDB(cfg DBConfig, snapshot string) (*sql.DB, error) {
	dbDSN := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8", cfg.User, cfg.Password, cfg.Host, cfg.Port, cfg.Schema)
	dbConn, err := sql.Open("mysql", dbDSN)
	if err != nil {
		return nil, errors.Trace(err)
	}

	if snapshot != "" {
		err = SetSnapshot(dbConn, snapshot)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}

	return dbConn, nil
}

// CloseDB closes the mysql fd
func CloseDB(db *sql.DB) error {
	return errors.Trace(db.Close())
}

// GetCreateTableSQL gets the create table sql.
func GetCreateTableSQL(db *sql.DB, schemaName string, tableName string) (string, error) {
	/*
		show create table example result:
		mysql> SHOW CREATE TABLE `test`.`itest`;
		+-------+--------------------------------------------------------------------+
		| Table | Create Table                                                                                                                              |
		+-------+--------------------------------------------------------------------+
		| itest | CREATE TABLE `itest` (
			`id` int(11) DEFAULT NULL,
		  	`name` varchar(24) DEFAULT NULL
			) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin |
		+-------+--------------------------------------------------------------------+
	*/
	query := fmt.Sprintf("SHOW CREATE TABLE `%s`.`%s`", schemaName, tableName)
	row := db.QueryRow(query)

	var tbl, createTable sql.NullString
	err := row.Scan(&tbl, &createTable)
	if err != nil {
		return "", errors.Trace(err)
	}
	if !tbl.Valid || !createTable.Valid {
		return "", errors.NotFoundf("table %s not exist", tableName)
	}
	return createTable.String, nil
}

// GetRowCount returns row count of the table by given where condition.
func GetRowCount(db *sql.DB, dbName string, table string, where string) (int64, error) {
	/*
		select count example result:
		mysql> SELECT count(1) cnt from `test`.`itest` where id > 0;
		+------+
		| cnt  |
		+------+
		|  100 |
		+------+
	*/

	query := fmt.Sprintf("SELECT COUNT(1) cnt FROM `%s`.`%s` WHERE %s", dbName, table, where)
	rows, err := QuerySQL(db, query)
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
		return 0, errors.NotFoundf("`cnt` field in `%s`.`%s`'s count result", dbName, table)
	}
	cnt, err := strconv.ParseInt(string(cntStr), 10, 64)
	return cnt, errors.Trace(err)
}

// IndexInfo saves index's information.
type IndexInfo struct {
	Table       string
	NoneUnique  bool
	KeyName     string
	SeqInIndex  int
	ColumnName  string
	Cardinality int
}

// ShowIndex returns result of executing `show index`
func ShowIndex(db *sql.DB, dbName string, table string) ([]*IndexInfo, error) {
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
	indices := make([]*IndexInfo, 0, 3)
	query := fmt.Sprintf("SHOW INDEX FROM `%s`.`%s`", dbName, table)
	rows, err := QuerySQL(db, query)
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer rows.Close()

	for rows.Next() {
		fields, err1 := ScanRow(rows)
		if err1 != nil {
			return nil, errors.Trace(err1)
		}
		seqInINdex, err1 := strconv.Atoi(string(fields["Seq_in_index"]))
		if err != nil {
			return nil, errors.Trace(err1)
		}
		cardinality, err1 := strconv.Atoi(string(fields["Cardinality"]))
		if err != nil {
			return nil, errors.Trace(err1)
		}
		index := &IndexInfo{
			Table:       string(fields["Table"]),
			NoneUnique:  string(fields["Non_unique"]) == "1",
			KeyName:     string(fields["Key_name"]),
			ColumnName:  string(fields["Column_name"]),
			SeqInIndex:  seqInINdex,
			Cardinality: cardinality,
		}
		indices = append(indices, index)
	}

	return indices, nil
}

// FindSuitableIndex returns a suitable index.
func FindSuitableIndex(db *sql.DB, dbName string, tableInfo *model.TableInfo) (*model.ColumnInfo, error) {
	// find primary key
	for _, index := range tableInfo.Indices {
		if index.Primary {
			return GetColumnByName(tableInfo, index.Columns[0].Name.O), nil
		}
	}

	// no primary key found, seek unique index
	for _, index := range tableInfo.Indices {
		if index.Unique {
			return GetColumnByName(tableInfo, index.Columns[0].Name.O), nil
		}
	}

	// no unique index found, seek index with max cardinality
	indices, err := ShowIndex(db, dbName, tableInfo.Name.O)
	if err != nil {
		return nil, errors.Trace(err)
	}
	var c *model.ColumnInfo
	var maxCardinality int
	for _, indexInfo := range indices {
		// just use the first column in the index, otherwise can't hit the index when select
		if indexInfo.SeqInIndex != 1 {
			continue
		}

		if indexInfo.Cardinality > maxCardinality {
			column := GetColumnByName(tableInfo, indexInfo.ColumnName)
			if column == nil {
				return nil, errors.NotFoundf("column %s in %s.%s", indexInfo.ColumnName, dbName, tableInfo.Name.O)
			}
			maxCardinality = indexInfo.Cardinality
			c = column
		}
	}

	return c, nil
}

// GetOrderKey return some columns for order.
func GetOrderKey(tbInfo *model.TableInfo) ([]string, []*model.ColumnInfo) {
	keys := make([]string, 0, 2)
	keyCols := make([]*model.ColumnInfo, 0, 2)

	for _, index := range tbInfo.Indices {
		if index.Primary {
			for _, indexCol := range index.Columns {
				keys = append(keys, indexCol.Name.O)
				keyCols = append(keyCols, tbInfo.Columns[indexCol.Offset])
			}
		}
	}

	if len(keys) == 0 {
		// no primary key found, use all fields as order by key
		for _, col := range tbInfo.Columns {
			keys = append(keys, col.Name.O)
			keyCols = append(keyCols, col)
		}
	}

	return keys, keyCols
}

// GetRandomValues returns some random value of a column, not used for number type column.
func GetRandomValues(db *sql.DB, dbName string, table string, column string, num int64, min, max interface{}, limitRange string) ([]string, error) {
	if limitRange != "" {
		limitRange = "true"
	}

	randomValue := make([]string, 0, num)
	query := fmt.Sprintf("SELECT `%s` FROM (SELECT `%s` FROM `%s`.`%s` WHERE `%s` >= \"%v\" AND `%s` <= \"%v\" AND %s ORDER BY RAND() LIMIT %d)rand_tmp ORDER BY `%s`",
		column, column, dbName, table, column, min, column, max, limitRange, num, column)
	log.Debugf("get random values sql: %s", query)
	rows, err := QuerySQL(db, query)
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
func GetColumnByName(table *model.TableInfo, name string) *model.ColumnInfo {
	var c *model.ColumnInfo
	for _, column := range table.Columns {
		if column.Name.O == name {
			c = column
			break
		}
	}

	return c
}

// GetTables gets all table in the schema
func GetTables(db *sql.DB, dbName string) ([]string, error) {
	rs, err := QuerySQL(db, fmt.Sprintf("SHOW TABLES IN `%s`;", dbName))
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

// GetCRC32Checksum returns the checksum of some data
func GetCRC32Checksum(db *sql.DB, schemaName string, tbInfo *model.TableInfo, orderKeys []string, limitRange string) (string, error) {
	/*
		calculate CRC32 checksum example:
		mysql> SELECT CRC32(GROUP_CONCAT(CONCAT_WS(',', a, b, c, d) SEPARATOR  ' + ')) AS checksum
			> FROM (SELECT * FROM `test`.`test2` WHERE `a` >= 29100000 AND `a` < 29200000 AND true order by a) AS tmp;
		+------------+
		| checksum   |
		+------------+
		| 1171947116 |
		+------------+
	*/
	columnNames := make([]string, 0, len(tbInfo.Columns))
	for _, col := range tbInfo.Columns {
		columnNames = append(columnNames, col.Name.O)
	}

	query := fmt.Sprintf("SELECT CRC32(GROUP_CONCAT(CONCAT_WS(',', %s) SEPARATOR  ' + ')) AS checksum FROM (SELECT * FROM `%s`.`%s` WHERE %s ORDER BY %s) AS tmp;",
		strings.Join(columnNames, ", "), schemaName, tbInfo.Name.O, limitRange, strings.Join(orderKeys, ","))
	log.Debugf("checksum sql: %s", query)
	rows, err := QuerySQL(db, query)
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

// GetTidbPosition gets tidb's position.
func GetTidbPosition(db *sql.DB) (int64, error) {
	/*
		example in tidb:
		mysql> SHOW MASTER STATUS;
		+-------------+--------------------+--------------+------------------+-------------------+
		| File        | Position           | Binlog_Do_DB | Binlog_Ignore_DB | Executed_Gtid_Set |
		+-------------+--------------------+--------------+------------------+-------------------+
		| tidb-binlog | 400718757701615617 |              |                  |                   |
		+-------------+--------------------+--------------+------------------+-------------------+
	*/
	rows, err := db.Query("SHOW MASTER STATUS")
	if err != nil {
		return 0, errors.Trace(err)
	}
	defer rows.Close()

	for rows.Next() {
		fields, err1 := ScanRow(rows)
		if err1 != nil {
			return 0, errors.Trace(err1)
		}

		ts, err1 := strconv.ParseInt(string(fields["Position"]), 10, 64)
		if err1 != nil {
			return 0, errors.Trace(err1)
		}
		return ts, nil
	}
	return 0, errors.New("get slave cluster's ts failed")
}

// QuerySQL queries sql, and returns some row
func QuerySQL(db *sql.DB, query string) (*sql.Rows, error) {
	log.Debugf("[query][sql]%s", query)
	rows, err := db.Query(query)
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

		err = rows.Scan(colVals...)
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
	sql := fmt.Sprintf("SET @@tidb_snapshot='%s'", snapshot)
	log.Infof("set snapshot: %s", sql)
	_, err := db.Exec(sql)
	if err != nil {
		return errors.Trace(err)
	}

	return nil
}

// IsNumberType returns true if is number type
func IsNumberType(tp byte) bool {
	switch tp {
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeLong, mysql.TypeLonglong, mysql.TypeInt24, mysql.TypeYear:
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
