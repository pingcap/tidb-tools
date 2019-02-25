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
	"os"
	"strconv"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb-tools/pkg/utils"
	"github.com/pingcap/tidb/types"
	log "github.com/sirupsen/logrus"
)

const (
	// ImplicitColName is name of implicit column in TiDB
	ImplicitColName = "_tidb_rowid"

	// ImplicitColID is ID implicit column in TiDB
	ImplicitColID = -1
)

var (
	// ErrVersionNotFound means can't get the database's version
	ErrVersionNotFound = errors.New("can't get the database's version")

	// ErrNoData means no data in table
	ErrNoData = errors.New("no data found")
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

// GetDBConfigFromEnv returns DBConfig from environment
func GetDBConfigFromEnv(schema string) DBConfig {
	host := os.Getenv("MYSQL_HOST")
	if host == "" {
		host = "127.0.0.1"
	}
	port, _ := strconv.Atoi(os.Getenv("MYSQL_PORT"))
	if port == 0 {
		port = 3306
	}
	user := os.Getenv("MYSQL_USER")
	if user == "" {
		user = "root"
	}
	pswd := os.Getenv("MYSQL_PSWD")

	return DBConfig{
		Host:     host,
		Port:     port,
		User:     user,
		Password: pswd,
		Schema:   schema,
	}
}

// OpenDB opens a mysql connection FD
func OpenDB(cfg DBConfig) (*sql.DB, error) {
	dbDSN := fmt.Sprintf("%s:%s@tcp(%s:%d)/?charset=utf8mb4", cfg.User, cfg.Password, cfg.Host, cfg.Port)
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

// GetRandomValues returns some random value and these value's count of a column, just like sampling. Tips: limitArgs is the value in limitRange.
func GetRandomValues(ctx context.Context, db *sql.DB, schemaName, table, column string, num int, limitRange string, limitArgs []interface{}, collation string) ([]string, []int, error) {
	/*
		example:
		mysql> SELECT `id`, COUNT(*) count FROM (SELECT `id` FROM `test`.`test`  WHERE `id` COLLATE "latin1_bin" > 0 AND `id` COLLATE "latin1_bin" < 100 ORDER BY RAND() LIMIT 5) rand_tmp GROUP BY `id` ORDER BY `id` COLLATE "latin1_bin";
		+------+-------+
		| id   | count |
		+------+-------+
		|    1 |     2 |
		|    2 |     2 |
		|    3 |     1 |
		+------+-------+

		FIXME: TiDB now don't return rand value when use `ORDER BY RAND()`
	*/

	if limitRange == "" {
		limitRange = "TRUE"
	}

	if collation != "" {
		collation = fmt.Sprintf(" COLLATE \"%s\"", collation)
	}

	randomValue := make([]string, 0, num)
	valueCount := make([]int, 0, num)

	query := fmt.Sprintf("SELECT %[1]s, COUNT(*) count FROM (SELECT %[1]s FROM %[2]s WHERE %[3]s ORDER BY RAND() LIMIT %[4]d)rand_tmp GROUP BY %[1]s ORDER BY %[1]s%[5]s",
		escapeName(column), TableName(schemaName, table), limitRange, num, collation)
	log.Debugf("get random values sql: %s, args: %v", query, limitArgs)

	rows, err := db.QueryContext(ctx, query, limitArgs...)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	defer rows.Close()

	for rows.Next() {
		var value string
		var count int
		err = rows.Scan(&value, &count)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		randomValue = append(randomValue, value)
		valueCount = append(valueCount, count)
	}

	return randomValue, valueCount, errors.Trace(rows.Err())
}

// GetMinMaxValue return min and max value of given column by specified limitRange condition.
func GetMinMaxValue(ctx context.Context, db *sql.DB, schema, table, column string, limitRange string, limitArgs []interface{}, collation string) (string, string, error) {
	/*
		example:
		mysql> SELECT MIN(`id`) as MIN, MAX(`id`) as MAX FROM `test`.`testa` WHERE id > 0 AND id < 10;
		+------+------+
		| MIN  | MAX  |
		+------+------+
		|    1 |    2 |
		+------+------+
	*/

	if limitRange == "" {
		limitRange = "TRUE"
	}

	if collation != "" {
		collation = fmt.Sprintf(" COLLATE \"%s\"", collation)
	}

	query := fmt.Sprintf("SELECT /*!40001 SQL_NO_CACHE */ MIN(`%s`%s) as MIN, MAX(`%s`%s) as MAX FROM `%s`.`%s` WHERE %s",
		column, collation, column, collation, schema, table, limitRange)
	log.Debugf("GetMinMaxValue query: %v, args: %v", query, limitArgs)

	var min, max sql.NullString
	rows, err := db.QueryContext(ctx, query, limitArgs...)
	if err != nil {
		return "", "", errors.Trace(err)
	}
	defer rows.Close()

	for rows.Next() {
		err = rows.Scan(&min, &max)
		if err != nil {
			return "", "", errors.Trace(err)
		}
	}

	if !min.Valid || !max.Valid {
		// don't have any data
		return "", "", ErrNoData
	}

	return min.String, max.String, errors.Trace(rows.Err())
}

// GetTables returns name of all tables in the specified schema
func GetTables(ctx context.Context, db *sql.DB, schemaName string) (tables []string, err error) {
	/*
		show tables without view: https://dev.mysql.com/doc/refman/5.7/en/show-tables.html

		example:
		mysql> show full tables in test where Table_Type != 'VIEW';
		+----------------+------------+
		| Tables_in_test | Table_type |
		+----------------+------------+
		| NTEST          | BASE TABLE |
		+----------------+------------+
	*/

	query := fmt.Sprintf("SHOW FULL TABLES IN `%s` WHERE Table_Type != 'VIEW';", schemaName)
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer rows.Close()

	tables = make([]string, 0, 8)
	for rows.Next() {
		var table, tType sql.NullString
		err = rows.Scan(&table, &tType)
		if err != nil {
			return nil, errors.Trace(err)
		}

		if !table.Valid || !tType.Valid {
			continue
		}

		tables = append(tables, table.String)
	}

	return tables, errors.Trace(rows.Err())
}

// GetSchemas returns name of all schemas
func GetSchemas(ctx context.Context, db *sql.DB) ([]string, error) {
	query := "SHOW DATABASES"
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer rows.Close()

	// show an example.
	/*
		mysql> SHOW DATABASES;
		+--------------------+
		| Database           |
		+--------------------+
		| information_schema |
		| mysql              |
		| performance_schema |
		| sys                |
		| test_db            |
		+--------------------+
	*/
	schemas := make([]string, 0, 10)
	for rows.Next() {
		var schema string
		err = rows.Scan(&schema)
		if err != nil {
			return nil, errors.Trace(err)
		}
		schemas = append(schemas, schema)
	}
	return schemas, errors.Trace(rows.Err())
}

// GetCRC32Checksum returns checksum code of some data by given condition
func GetCRC32Checksum(ctx context.Context, db *sql.DB, schemaName, tableName string, tbInfo *model.TableInfo, limitRange string, args []interface{}, ignoreColumns map[string]interface{}) (int64, error) {
	/*
		calculate CRC32 checksum example:
		mysql> SELECT BIT_XOR(CAST(CRC32(CONCAT_WS(',', id, name, age, CONCAT(ISNULL(id), ISNULL(name), ISNULL(age))))AS UNSIGNED)) AS checksum FROM test.test WHERE id > 0 AND id < 10;
		+------------+
		| checksum   |
		+------------+
		| 1466098199 |
		+------------+
	*/
	columnNames := make([]string, 0, len(tbInfo.Columns))
	columnIsNull := make([]string, 0, len(tbInfo.Columns))
	for _, col := range tbInfo.Columns {
		if _, ok := ignoreColumns[col.Name.O]; ok {
			continue
		}
		columnNames = append(columnNames, fmt.Sprintf("`%s`", col.Name.O))
		columnIsNull = append(columnIsNull, fmt.Sprintf("ISNULL(`%s`)", col.Name.O))
	}

	query := fmt.Sprintf("SELECT BIT_XOR(CAST(CRC32(CONCAT_WS(',', %s, CONCAT(%s)))AS UNSIGNED)) AS checksum FROM %s WHERE %s;",
		strings.Join(columnNames, ", "), strings.Join(columnIsNull, ", "), TableName(schemaName, tableName), limitRange)
	log.Debugf("checksum sql: %s, args: %v", query, args)

	var checksum sql.NullInt64
	err := db.QueryRowContext(ctx, query, args...).Scan(&checksum)
	if err != nil {
		return -1, errors.Trace(err)
	}
	if !checksum.Valid {
		// if don't have any data, the checksum will be `NULL`
		log.Warnf("get empty checksum by query %s, args %v", query, args)
		return 0, nil
	}

	return checksum.Int64, nil
}

// Bucket saves the bucket information from TiDB.
type Bucket struct {
	Count      int64
	LowerBound string
	UpperBound string
}

// GetBucketsInfo SHOW STATS_BUCKETS in TiDB.
func GetBucketsInfo(ctx context.Context, db *sql.DB, schema, table string, tableInfo *model.TableInfo) (map[string][]Bucket, error) {
	/*
		example in tidb:
		mysql> SHOW STATS_BUCKETS WHERE db_name= "test" AND table_name="testa";
		+---------+------------+----------------+-------------+----------+-----------+-------+---------+---------------------+---------------------+
		| Db_name | Table_name | Partition_name | Column_name | Is_index | Bucket_id | Count | Repeats | Lower_Bound         | Upper_Bound         |
		+---------+------------+----------------+-------------+----------+-----------+-------+---------+---------------------+---------------------+
		| test    | testa      |                | PRIMARY     |        1 |         0 |    64 |       1 | 1846693550524203008 | 1846838686059069440 |
		| test    | testa      |                | PRIMARY     |        1 |         1 |   128 |       1 | 1846840885082324992 | 1847056389361369088 |
		+---------+------------+----------------+-------------+----------+-----------+-------+---------+---------------------+---------------------+
	*/
	buckets := make(map[string][]Bucket)
	query := "SHOW STATS_BUCKETS WHERE db_name= ? AND table_name= ?;"
	log.Debugf("GetBucketsInfo query: %s", query)

	rows, err := db.QueryContext(ctx, query, schema, table)
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return nil, errors.Trace(err)
	}

	for rows.Next() {
		var dbName, tableName, partitionName, columnName, lowerBound, upperBound sql.NullString
		var isIndex, bucketID, count, repeats sql.NullInt64

		// add partiton_name in new version
		switch len(cols) {
		case 9:
			err = rows.Scan(&dbName, &tableName, &columnName, &isIndex, &bucketID, &count, &repeats, &lowerBound, &upperBound)
		case 10:
			err = rows.Scan(&dbName, &tableName, &partitionName, &columnName, &isIndex, &bucketID, &count, &repeats, &lowerBound, &upperBound)
		default:
			return nil, errors.New("Unknown struct for buckets info")
		}
		if err != nil {
			return nil, errors.Trace(err)
		}

		if _, ok := buckets[columnName.String]; !ok {
			buckets[columnName.String] = make([]Bucket, 0, 100)
		}
		buckets[columnName.String] = append(buckets[columnName.String], Bucket{
			Count:      count.Int64,
			LowerBound: lowerBound.String,
			UpperBound: upperBound.String,
		})
	}

	// when primary key is int type, the columnName will be column's name, not `PRIMARY`, check and transform here.
	indices := FindAllIndex(tableInfo)
	for _, index := range indices {
		if index.Name.O != "PRIMARY" {
			continue
		}
		_, ok := buckets[index.Name.O]
		if !ok && len(index.Columns) == 1 {
			if _, ok := buckets[index.Columns[0].Name.O]; !ok {
				return nil, errors.NotFoundf("primary key on %s in buckets info", index.Columns[0].Name.O)
			}
			buckets[index.Name.O] = buckets[index.Columns[0].Name.O]
			delete(buckets, index.Columns[0].Name.O)
		}
	}

	return buckets, errors.Trace(rows.Err())
}

// AnalyzeValuesFromBuckets analyze upperBound or lowerBound to string for each column.
// upperBound and lowerBound are looks like '(123, abc)' for multiple fields, or '123' for one field.
func AnalyzeValuesFromBuckets(valueString string, cols []*model.ColumnInfo) ([]string, error) {
	// FIXME: maybe some values contains '(', ')' or ', '
	vStr := strings.Trim(valueString, "()")
	values := strings.Split(vStr, ", ")
	if len(values) != len(cols) {
		return nil, errors.Errorf("analyze value %s failed", valueString)
	}

	for i, col := range cols {
		if IsTimeTypeAndNeedDecode(col.Tp) {
			value, err := DecodeTimeInBucket(values[i])
			if err != nil {
				return nil, errors.Trace(err)
			}

			values[i] = value
		}
	}

	return values, nil
}

// DecodeTimeInBucket decodes Time from a packed uint64 value.
func DecodeTimeInBucket(packedStr string) (string, error) {
	packed, err := strconv.ParseUint(packedStr, 10, 64)
	if err != nil {
		return "", err
	}

	if packed == 0 {
		return "", nil
	}

	t := new(types.Time)
	err = t.FromPackedUint(packed)
	if err != nil {
		return "", err
	}

	return t.String(), nil
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
		fields, err1 := ScanRow(rows)
		if err1 != nil {
			return 0, errors.Trace(err1)
		}

		ts, err1 := strconv.ParseInt(string(fields["Position"].Data), 10, 64)
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

// GetDBVersion returns the database's version
func GetDBVersion(ctx context.Context, db *sql.DB) (string, error) {
	/*
		example in TiDB:
		mysql> select version();
		+--------------------------------------+
		| version()                            |
		+--------------------------------------+
		| 5.7.10-TiDB-v2.1.0-beta-173-g7e48ab1 |
		+--------------------------------------+

		example in MySQL:
		mysql> select version();
		+-----------+
		| version() |
		+-----------+
		| 5.7.21    |
		+-----------+
	*/
	query := "SELECT version()"
	result, err := db.QueryContext(ctx, query)
	if err != nil {
		return "", errors.Trace(err)
	}
	defer result.Close()

	var version sql.NullString
	for result.Next() {
		err := result.Scan(&version)
		if err != nil {
			return "", errors.Trace(err)
		}
		break
	}

	if version.Valid {
		return version.String, nil
	}

	return "", ErrVersionNotFound
}

// IsTiDB returns true if this database is tidb
func IsTiDB(ctx context.Context, db *sql.DB) (bool, error) {
	version, err := GetDBVersion(ctx, db)
	if err != nil {
		log.Errorf("get database's version meets error %v", err)
		return false, errors.Trace(err)
	}

	return strings.Contains(strings.ToLower(version), "tidb"), nil
}

// TableName returns `schema`.`table`
func TableName(schema, table string) string {
	return fmt.Sprintf("`%s`.`%s`", escapeName(schema), escapeName(table))
}

func escapeName(name string) string {
	return strings.Replace(name, "`", "``", -1)
}

// ReplacePlaceholder will use args to replace '?', used for log.
// tips: make sure the num of "?" is same with len(args)
func ReplacePlaceholder(str string, args []string) string {
	/*
		for example:
		str is "a > ? AND a < ?", args is {'1', '2'},
		this function will return "a > '1' AND a < '2'"
	*/
	newStr := strings.Replace(str, "?", "'%s'", -1)
	return fmt.Sprintf(newStr, utils.StringsToInterfaces(args)...)
}
