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

package dbutil

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb/model"
)

const (
	// ImplicitColName is name of implicit column in TiDB
	ImplicitColName = "_tidb_rowid"

	// ImplicitColID is ID implicit column in TiDB
	ImplicitColID = -1
)

// DBConfig is database configuration.
type DBConfig struct {
	Host string `toml:"host" json:"host"`

	Port int `toml:"port" json:"port"`

	User string `toml:"user" json:"user"`

	Password string `toml:"password" json:"password"`

	Schema string `toml:"schema" json:"schema"`
}

// String returns native format of database configuration
func (c *DBConfig) String() string {
	if c == nil {
		return "<nil>"
	}
	return fmt.Sprintf("DBConfig(%+v)", *c)
}

// OpenDB opens a mysql connection FD
func OpenDB(cfg DBConfig) (*sql.DB, error) {
	dbDSN := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8mb4", cfg.User, cfg.Password, cfg.Host, cfg.Port, cfg.Schema)
	dbConn, err := sql.Open("mysql", dbDSN)
	if err != nil {
		return nil, errors.Trace(err)
	}

	err = dbConn.Ping()
	return dbConn, errors.Trace(err)
}

// CloseDB closes the mysql fd
func CloseDB(db *sql.DB) error {
	if db == nil {
		return nil
	}

	return errors.Trace(db.Close())
}

// GetCreateTableSQL returns the create table statement.
func GetCreateTableSQL(ctx context.Context, db *sql.DB, schemaName string, tableName string) (string, error) {
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

	var tbl, createTable sql.NullString
	err := db.QueryRowContext(ctx, query).Scan(&tbl, &createTable)
	if err != nil {
		return "", errors.Trace(err)
	}
	if !tbl.Valid || !createTable.Valid {
		return "", errors.NotFoundf("table %s", tableName)
	}

	return createTable.String, nil
}

// GetRowCount returns row count of the table.
// if not specify where condition, return total row count of the table.
func GetRowCount(ctx context.Context, db *sql.DB, schemaName string, tableName string, where string) (int64, error) {
	/*
		select count example result:
		mysql> SELECT count(1) cnt from `test`.`itest` where id > 0;
		+------+
		| cnt  |
		+------+
		|  100 |
		+------+
	*/

	query := fmt.Sprintf("SELECT COUNT(1) cnt FROM `%s`.`%s`", schemaName, tableName)
	if len(where) > 0 {
		query += fmt.Sprintf(" WHERE %s", where)
	}

	var cnt sql.NullInt64
	err := db.QueryRowContext(ctx, query).Scan(&cnt)
	if err != nil {
		return 0, errors.Trace(err)
	}
	if !cnt.Valid {
		return 0, errors.NotFoundf("table `%s`.`%s`", schemaName, tableName)
	}

	return cnt.Int64, nil
}

// GetRandomValues returns some random value of a column.
func GetRandomValues(ctx context.Context, db *sql.DB, schemaName, table, column string, num int64, min, max interface{}, limitRange string) ([]interface{}, error) {
	if limitRange != "" {
		limitRange = "true"
	}

	randomValue := make([]interface{}, 0, num)
	query := fmt.Sprintf("SELECT `%s` FROM (SELECT `%s` FROM `%s`.`%s` WHERE `%s` >= ? AND `%s` <= ? AND %s ORDER BY RAND() LIMIT %d)rand_tmp ORDER BY `%s`",
		column, column, schemaName, table, column, column, limitRange, num, column)
	log.Debugf("get random values sql: %s, min: %v, max: %v", query, min, max)
	rows, err := db.QueryContext(ctx, query, min, max)
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer rows.Close()

	for rows.Next() {
		var value interface{}
		err = rows.Scan(&value)
		if err != nil {
			return nil, errors.Trace(err)
		}
		randomValue = append(randomValue, value)
	}

	return randomValue, nil
}

// GetTables returns name of all tables in the specified schema
func GetTables(ctx context.Context, db *sql.DB, schemaName string) ([]string, error) {
	rs, err := db.QueryContext(ctx, fmt.Sprintf("SHOW TABLES IN `%s`;", schemaName))
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

// GetCRC32Checksum returns checksum code of some data by given condition
func GetCRC32Checksum(ctx context.Context, db *sql.DB, schemaName string, tbInfo *model.TableInfo, orderKeys []string, limitRange string, args []interface{}) (string, error) {
	/*
		TODO: use same sql to calculate CRC32 checksum in TiDB and MySQL when TiDB support ORDER BY in GROUP_CONTACT.

		calculate CRC32 checksum example:

		in TiDB:
		mysql> SELECT CRC32(GROUP_CONCAT(row SEPARATOR ' + ')) AS checksum
			> FROM (SELECT CONCAT_WS(",",a,b,c) AS row
			> FROM (SELECT * FROM test.test WHERE `a` >= 0 AND `a` < 10 AND true ORDER BY a) AS tmp) AS rows ORDER BY row;
		+------------+
		| checksum   |
		+------------+
		| 1171947116 |
		+------------+

		in MySQL:
		mysql> SELECT CRC32(GROUP_CONCAT(CONCAT_WS(',', a, b, c ) ORDER BY a ASC SEPARATOR ' + ')) AS checksum
			> FROM test.test WHERE `a` >= 0 AND `a` < 10 AND true;
		+------------+
		| checksum   |
		+------------+
		| 1171947116 |
		+------------+

		Notice: in the older tidb version, tidb will get different checksum with mysql, can see this issue pingcap/tidb#7446
	*/
	isTiDB, err := IsTiDB(ctx, db)
	if err != nil {
		return "", errors.Trace(err)
	}

	columnNames := make([]string, 0, len(tbInfo.Columns))
	for _, col := range tbInfo.Columns {
		columnNames = append(columnNames, col.Name.O)
	}

	var query string
	if isTiDB {
		query = fmt.Sprintf("SELECT CRC32(GROUP_CONCAT(row SEPARATOR ' + ')) AS checksum FROM (SELECT CONCAT_WS(',',%s) AS row FROM (SELECT * FROM `%s`.`%s` WHERE %s ORDER BY %s) AS tmp) AS rows ORDER BY row;",
			strings.Join(columnNames, ", "), schemaName, tbInfo.Name.O, limitRange, strings.Join(orderKeys, ","))
	} else {
		query = fmt.Sprintf("SELECT CRC32(GROUP_CONCAT(CONCAT_WS(',',%s) ORDER BY %s ASC SEPARATOR ' + ')) AS checksum FROM `%s`.`%s` WHERE %s;",
			strings.Join(columnNames, ", "), strings.Join(orderKeys, ","), schemaName, tbInfo.Name.O, limitRange)
	}
	log.Debugf("checksum sql: %s, args: %v", query, args)

	var checksum sql.NullString
	err = db.QueryRowContext(ctx, query, args...).Scan(&checksum)
	if err != nil {
		return "", errors.Trace(err)
	}
	if !checksum.Valid {
		// if don't have any data, the checksum will be `NULL`
		log.Warnf("get empty checksum by query %s, args %v", query, args)
		return "", nil
	}

	return checksum.String, nil
}

// GetTidbLatestTSO returns tidb's current TSO.
func GetTidbLatestTSO(ctx context.Context, db *sql.DB) (int64, error) {
	/*
		example in tidb:
		mysql> SHOW MASTER STATUS;
		+-------------+--------------------+--------------+------------------+-------------------+
		| File        | Position           | Binlog_Do_DB | Binlog_Ignore_DB | Executed_Gtid_Set |
		+-------------+--------------------+--------------+------------------+-------------------+
		| tidb-binlog | 400718757701615617 |              |                  |                   |
		+-------------+--------------------+--------------+------------------+-------------------+
	*/
	rows, err := db.QueryContext(ctx, "SHOW MASTER STATUS")
	if err != nil {
		return 0, errors.Trace(err)
	}
	defer rows.Close()

	for rows.Next() {
		fields, _, err1 := ScanRow(rows)
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

// SetSnapshot set the snapshot variable for tidb
func SetSnapshot(ctx context.Context, db *sql.DB, snapshot string) error {
	sql := fmt.Sprintf("SET @@tidb_snapshot='%s'", snapshot)
	log.Infof("set history snapshot: %s", sql)
	_, err := db.ExecContext(ctx, sql)
	return errors.Trace(err)
}

// GetTiDBVersion returns the tidb's version
func GetTiDBVersion(ctx context.Context, db *sql.DB) (string, error) {
	/*
		example in tidb:
		mysql> SELECT tidb_version()\G
		*************************** 1. row ***************************
		tidb_version(): Release Version: v2.1.0-beta-260-gd4e0885
		Git Commit Hash: d4e08853e4d4d867676f0f17ec6e5b15d5a0bdf8
		Git Branch: master
		UTC Build Time: 2018-08-21 03:59:16
		GoVersion: go version go1.10.3 darwin/amd64
		Race Enabled: false
		TiKV Min Version: 2.1.0-alpha.1-ff3dd160846b7d1aed9079c389fc188f7f5ea13e
		Check Table Before Drop: false
		1 row in set (0.00 sec)
	*/
	query := "SELECT tidb_version()"
	result, err := db.QueryContext(ctx, query)
	if err != nil {
		return "", errors.Trace(err)
	}
	var version sql.NullString
	for result.Next() {
		err := result.Scan(&version)
		if err != nil {
			return "", errors.Trace(err)
		}
		break
	}

	if version.Valid {
		// TODO: need analyse the tidb version string
		return version.String, nil
	}

	return "", errors.New("can't get the tidb's version")
}

// IsTiDB returns true if this database is tidb
func IsTiDB(ctx context.Context, db *sql.DB) (bool, error) {
	_, err := GetTiDBVersion(ctx, db)
	if err != nil {
		if strings.Contains(err.Error(), "tidb_version does not exist") {
			return false, nil
		}

		log.Errorf("get tidb version meets error %v", err)
		return false, errors.Trace(err)
	}

	return true, nil
}
