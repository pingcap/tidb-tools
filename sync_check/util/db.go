package util

import (
	"database/sql"
	"fmt"
	"strconv"
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/siddontang/go-mysql/schema"
)

// DBConfig is the DB configuration.
type DBConfig struct {
	Host string `toml:"host" json:"host"`

	User string `toml:"user" json:"user"`

	Password string `toml:"password" json:"password"`

	Name string `toml:"name" json:"name"`

	Port int `toml:"port" json:"port"`
}

func (c *DBConfig) String() string {
	if c == nil {
		return "<nil>"
	}
	return fmt.Sprintf("DBConfig(%+v)", *c)
}

// CreateDB create a mysql fd
func CreateDB(cfg DBConfig) (*sql.DB, error) {
	dbName := cfg.Name
	createDBSql := fmt.Sprintf("create database if not exists %s", dbName)
	// dont't have database
	dbDSN := fmt.Sprintf("%s:%s@tcp(%s:%d)/mysql?charset=utf8", cfg.User, cfg.Password, cfg.Host, cfg.Port)
	db, err := sql.Open("mysql", dbDSN)
	if err != nil {
		return nil, errors.Trace(err)
	}

	_, err = db.Exec(createDBSql)
	if err != nil {
		return nil, errors.Trace(err)
	}

	dbDSN = fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8", cfg.User, cfg.Password, cfg.Host, cfg.Port, cfg.Name)
	db, err = sql.Open("mysql", dbDSN)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return db, nil
}

// CloseDB close the mysql fd
func CloseDB(db *sql.DB) error {
	return errors.Trace(db.Close())
}

// GetSchemaTables get some table information
func GetSchemaTables(db *sql.DB, schemaName string, tableNames []string) (tables []*schema.Table, err error) {
	tables = make([]*schema.Table, 0, len(tableNames))
	for _, tableName := range tableNames {
		table, err := GetSchemaTable(db, schemaName, tableName)
		if err != nil {
			return nil, errors.Trace(errors.Annotatef(err, "get table error. schema %s, table %s", schemaName, tableName))
		}
		tables = append(tables, table)
	}

	return tables, nil
}

// GetCount get count rows of the table for specific field.
func GetCount(db *sql.DB, dbname string, table string, timeRange string) (int64, error) {
	query := fmt.Sprintf("SELECT count(1) cnt from `%s`.`%s` where %s", dbname, table, timeRange)
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

// FindSuitableIndex find a suitable field for split data
func FindSuitableIndex(db *sql.DB, dbName string, table string, numberFirst bool) (*schema.TableColumn, error) {
	rowsData, err := ShowIndex(db, dbName, table)
	if err != nil {
		return nil, errors.Trace(err)
	}

	tableInfo, err := GetSchemaTable(db, dbName, table)
	if err != nil {
		return nil, errors.Trace(err)
	}

	var indexColumn *schema.TableColumn
	// seek pk
	for _, fields := range rowsData {
		if string(fields["Key_name"]) == "PRIMARY" && string(fields["Seq_in_index"]) == "1" {
			column, valid := GetColumnByName(tableInfo, string(fields["Column_name"]))
			if !valid {
				return nil, fmt.Errorf("can't find column %s in %s.%s", string(fields["Column_name"]), dbName, table)
			}
			if !numberFirst || column.Type == schema.TYPE_NUMBER {
				return column, nil
			}
			indexColumn = column
		}
	}

	// no pk found, seek unique index
	for _, fields := range rowsData {
		if string(fields["Non_unique"]) == "0" && string(fields["Seq_in_index"]) == "1" {
			column, valid := GetColumnByName(tableInfo, string(fields["Column_name"]))
			if !valid {
				return nil, fmt.Errorf("can't find column %s in %s.%s", string(fields["Column_name"]), dbName, table)
			}
			if !numberFirst || column.Type == schema.TYPE_NUMBER {
				return column, nil
			}
			if indexColumn == nil {
				indexColumn = column
			}
		}
	}

	// no unique index found, seek index with max cardinality
	var c *schema.TableColumn
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
					return nil, fmt.Errorf("can't find column %s in %s.%s", string(fields["Column_name"]), dbName, table)
				}
				if !numberFirst || column.Type == schema.TYPE_NUMBER {
					maxCardinality = cardinality
					c = column
				}
			}
		}
	}

	if c == nil {
		// if can't find number index, return the first index
		return indexColumn, nil
	}

	return c, nil
}

// FindNumberColumn find a number type column in the table
func FindNumberColumn(db *sql.DB, dbname string, table string) (*schema.TableColumn, error) {
	tableInfo, err := GetSchemaTable(db, dbname, table)
	if err != nil {
		return nil, errors.Trace(err)
	}
	var numberColumn *schema.TableColumn
	for _, column := range tableInfo.Columns {
		if column.Type == schema.TYPE_NUMBER {
			numberColumn = &column
			break
		}
	}

	return numberColumn, nil
}

// GetFirstColumn returns the first column in the table
func GetFirstColumn(db *sql.DB, dbname string, table string) (*schema.TableColumn, error) {
	tableInfo, err := GetSchemaTable(db, dbname, table)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &tableInfo.Columns[0], nil
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

// GetColumnByName get column by name
func GetColumnByName(table *schema.Table, name string) (*schema.TableColumn, bool) {
	var c *schema.TableColumn
	for _, column := range table.Columns {
		if column.Name == name {
			c = &column
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

// GetSchemaTable returns table information.
func GetSchemaTable(db *sql.DB, schemaName, tableName string) (table *schema.Table, err error) {
	maxRetry := 3
	for i := 0; i < maxRetry; i++ {
		table, err = schema.NewTableFromSqlDB(db, schemaName, tableName)
		if err != nil {
			log.Warnf("[syncer] get table error %s", errors.ErrorStack(err))
			time.Sleep(time.Millisecond * 200)
			continue
		}
		log.Debugf("[syncer] get table %+v", table)
		return table, nil
	}

	return nil, errors.Errorf("get table reached max retry %d and failed", maxRetry)
}
