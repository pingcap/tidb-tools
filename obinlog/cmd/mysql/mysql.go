package main

import (
	"database/sql"
	"flag"
	"fmt"
	"strings"

	"github.com/Shopify/sarama"
	_ "github.com/go-sql-driver/mysql"
	"github.com/ngaut/log"
	obinlog "github.com/pingcap/tidb-tools/obinlog/go-binlog"
	"github.com/pingcap/tidb-tools/obinlog/reader"
)

// a simple example to sync data to mysql

var (
	port     = flag.Int("P", 3306, "port")
	user     = flag.String("u", "root", "user")
	password = flag.String("p", "hello", "password")
	host     = flag.String("h", "localhost", "host")

	clusterID = flag.String("clusterID", "6561373978432450126", "clusterID")
	offset    = flag.Int64("offset", sarama.OffsetNewest, "offset")
	commitTS  = flag.Int64("commitTS", 0, "commitTS")
)

func getDB() (db *sql.DB, err error) {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8", *user, *password, *host, *port, "test")
	log.Debug(dsn)

	db, err = sql.Open("mysql", dsn)
	if err != nil {
		return
	}

	return
}

func main() {
	flag.Parse()

	cfg := &reader.Config{
		KafakaAddr: []string{"127.0.0.1:9092"},
		Offset:     *offset,
		CommitTS:   *commitTS,
		ClusterID:  *clusterID,
	}

	reader, err := reader.NewReader(cfg)
	if err != nil {
		panic(err)
	}

	db, err := getDB()
	if err != nil {
		panic(err)
	}

	for {
		select {
		case msg := <-reader.Messages():
			log.Debug("recv: ", msg.Binlog.String())
			binlog := msg.Binlog
			sqls, args := toSql(binlog)

			tx, err := db.Begin()
			if err != nil {
				log.Fatal(err)
			}

			for i := 0; i < len(sqls); i++ {
				log.Debug("exec: args: ", sqls[i], args[i])
				_, err := tx.Exec(sqls[i], args[i]...)
				if err != nil {
					log.Error(err)
				}

			}
			err = tx.Commit()
			if err != nil {
				log.Fatal(err)
			}

		}
	}

}

func columnToArg(c *obinlog.Column) (arg interface{}) {
	if c.GetIsNull() {
		return nil
	}

	if c.Int64Value != nil {
		return c.GetInt64Value()
	}

	if c.Uint64Value != nil {
		return c.GetUint64Value()
	}

	if c.DoubleValue != nil {
		return c.GetDoubleValue()
	}

	if c.BytesValue != nil {
		return c.GetBytesValue()
	}

	return c.GetStringValue()
}

func tableToSQL(table *obinlog.Table) (sqls []string, sqlArgs [][]interface{}) {

	replace := func() {
		sql := fmt.Sprintf("replace into %s.%s", table.GetSchemaName(), table.GetTableName())

		var names []string
		var holder []string
		for _, c := range table.GetColumnInfo() {
			names = append(names, c.GetName())
			holder = append(holder, "?")
		}
		sql += "(" + strings.Join(names, ",") + ")"
		sql += "values(" + strings.Join(holder, ",") + ")"

		for _, row := range table.Rows {
			var args []interface{}
			for _, col := range row.GetColumns() {
				args = append(args, columnToArg(col))
			}

			sqls = append(sqls, sql)
			sqlArgs = append(sqlArgs, args)
		}

	}

	constructWhere := func() (sql string, usePK bool) {
		var whereColumns []string
		for _, col := range table.GetColumnInfo() {
			if col.GetIsPrimaryKey() {
				whereColumns = append(whereColumns, col.GetName())
				usePK = true
			}
		}
		// no primary key
		if len(whereColumns) == 0 {
			for _, col := range table.GetColumnInfo() {
				whereColumns = append(whereColumns, col.GetName())
			}
		}

		sql = " where "
		for i, col := range whereColumns {
			if i != 0 {
				sql += " and "
			}

			sql += fmt.Sprintf("%s = ? ", col)
		}

		sql += " limit 1"

		return
	}

	switch table.GetType() {
	case obinlog.MutationType_Insert:
		replace()
	case obinlog.MutationType_Update:
		columnInfo := table.GetColumnInfo()
		sql := fmt.Sprintf("update %s.%s set ", table.GetSchemaName(), table.GetTableName())
		// construct c1 = ?, c2 = ?...
		for i, col := range columnInfo {
			if i != 0 {
				sql += ","
			}
			sql += fmt.Sprintf("%s = ? ", col.Name)
		}

		where, usePK := constructWhere()
		sql += where

		for rowIdx, row := range table.Rows {
			changed_row := table.ChangedRows[rowIdx]

			var args []interface{}
			// for set
			for _, col := range row.GetColumns() {
				args = append(args, columnToArg(col))
			}

			// for where
			for i, col := range changed_row.GetColumns() {
				if !usePK || columnInfo[i].GetIsPrimaryKey() {
					args = append(args, columnToArg(col))
				}
			}

			sqls = append(sqls, sql)
			sqlArgs = append(sqlArgs, args)
		}

	case obinlog.MutationType_Delete:
		columnInfo := table.GetColumnInfo()
		sql := fmt.Sprintf("delete from %s.%s ", table.GetSchemaName(), table.GetTableName())

		where, usePK := constructWhere()
		sql += where

		for _, row := range table.Rows {
			var args []interface{}
			for i, col := range row.GetColumns() {
				if !usePK || columnInfo[i].GetIsPrimaryKey() {
					args = append(args, columnToArg(col))
				}
			}

			sqls = append(sqls, sql)
			sqlArgs = append(sqlArgs, args)
		}
	}

	return
}

func toSql(binlog *obinlog.Binlog) ([]string, [][]interface{}) {
	var allSQL []string
	var allArgs [][]interface{}

	switch binlog.GetType() {
	case obinlog.BinlogType_DDL:
		ddl := binlog.DdlData
		sql := fmt.Sprintf("use %s", ddl.GetSchemaName())
		allSQL = append(allSQL, sql)
		allArgs = append(allArgs, nil)
		sql = string(ddl.DdlQuery)
		allSQL = append(allSQL, sql)
		allArgs = append(allArgs, nil)

	case obinlog.BinlogType_DML:
		dml := binlog.DmlData
		for _, table := range dml.GetTables() {
			sqls, sqlArgs := tableToSQL(table)
			allSQL = append(allSQL, sqls...)
			allArgs = append(allArgs, sqlArgs...)
		}

	default:
		log.Warn("unknow type: ", binlog.GetType())
	}

	return allSQL, allArgs
}
