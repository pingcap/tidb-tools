// Copyright 2021 PingCAP, Inc.
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

package utils

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb-tools/pkg/dbutil"
	"github.com/pkg/term"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

// WorkerPool contains a pool of workers.
type WorkerPool struct {
	limit   uint
	workers chan *Worker
	name    string
	wg      sync.WaitGroup
}

// Worker identified by ID.
type Worker struct {
	ID uint64
}

type taskFunc func()
type identifiedTaskFunc func(uint64)

// NewWorkerPool returns a WorkPool.
func NewWorkerPool(limit uint, name string) *WorkerPool {
	workers := make(chan *Worker, limit)
	for i := uint(0); i < limit; i++ {
		workers <- &Worker{ID: uint64(i + 1)}
	}
	return &WorkerPool{
		limit:   limit,
		workers: workers,
		name:    name,
	}
}

// GetChar Returns either an ascii code, or (if input is an arrow) a Javascript key code.
func GetChar() (ascii int, keyCode int, err error) {
	t, _ := term.Open("/dev/tty")
	term.RawMode(t)
	bytes := make([]byte, 3)

	var numRead int
	numRead, err = t.Read(bytes)
	if err != nil {
		return
	}
	if numRead == 3 && bytes[0] == 27 && bytes[1] == 91 {
		// Three-character control sequence, beginning with "ESC-[".

		// Since there are no ASCII codes for arrow keys, we use
		// Javascript key codes.
		if bytes[2] == 65 {
			// Up
			keyCode = 38
		} else if bytes[2] == 66 {
			// Down
			keyCode = 40
		} else if bytes[2] == 67 {
			// Right
			keyCode = 39
		} else if bytes[2] == 68 {
			// Left
			keyCode = 37
		}
	} else if numRead == 1 {
		ascii = int(bytes[0])
	} else {
		// Two characters read??
	}
	t.Restore()
	t.Close()
	return
}

// Apply executes a task.
func (pool *WorkerPool) Apply(fn taskFunc) {
	worker := pool.apply()
	go func() {
		pool.wg.Add(1)
		defer pool.wg.Done()
		defer pool.recycle(worker)
		fn()
	}()
}

// ApplyWithID execute a task and provides it with the worker ID.
func (pool *WorkerPool) ApplyWithID(fn identifiedTaskFunc) {
	worker := pool.apply()
	go func() {
		pool.wg.Add(1)
		defer pool.wg.Done()
		defer pool.recycle(worker)
		fn(worker.ID)
	}()
}

// ApplyOnErrorGroup executes a task in an errgroup.
func (pool *WorkerPool) ApplyOnErrorGroup(eg *errgroup.Group, fn func() error) {
	worker := pool.apply()
	eg.Go(func() error {
		pool.wg.Add(1)
		defer pool.wg.Done()
		defer pool.recycle(worker)
		return fn()
	})
}

// ApplyWithIDInErrorGroup executes a task in an errgroup and provides it with the worker ID.
func (pool *WorkerPool) ApplyWithIDInErrorGroup(eg *errgroup.Group, fn func(id uint64) error) {
	worker := pool.apply()
	eg.Go(func() error {
		pool.wg.Add(1)
		defer pool.wg.Done()
		defer pool.recycle(worker)
		return fn(worker.ID)
	})
}

func (pool *WorkerPool) apply() *Worker {
	var worker *Worker
	select {
	case worker = <-pool.workers:
	default:
		log.Debug("wait for workers", zap.String("pool", pool.name))
		worker = <-pool.workers
	}
	return worker
}

func (pool *WorkerPool) recycle(worker *Worker) {
	if worker == nil {
		panic("invalid restore worker")
	}
	pool.workers <- worker
}

// HasWorker checks if the pool has unallocated workers.
func (pool *WorkerPool) HasWorker() bool {
	return len(pool.workers) > 0
}

// WaitFinished waits till the pool finishs all the tasks.
func (pool *WorkerPool) WaitFinished() {
	pool.wg.Wait()
}

func GetColumnsFromIndex(index *model.IndexInfo, tableInfo *model.TableInfo) []*model.ColumnInfo {
	indexColumns := make([]*model.ColumnInfo, 0, len(index.Columns))
	for _, indexColumn := range index.Columns {
		for _, column := range tableInfo.Columns {
			if column.Name.O == indexColumn.Name.O {
				indexColumns = append(indexColumns, column)
			}
		}
	}

	return indexColumns
}

// StringsToInterfaces converts string slice to interface slice
func StringsToInterfaces(strs []string) []interface{} {
	is := make([]interface{}, 0, len(strs))
	for _, str := range strs {
		is = append(is, str)
	}

	return is
}

func GetTableRowsQueryFormat(schema, table string, tableInfo *model.TableInfo, collation string) (string, []*model.ColumnInfo) {
	orderKeys, orderKeyCols := dbutil.SelectUniqueOrderKey(tableInfo)

	columnNames := make([]string, 0, len(tableInfo.Columns))
	for _, col := range tableInfo.Columns {
		name := dbutil.ColumnName(col.Name.O)
		if col.FieldType.Tp == mysql.TypeFloat {
			name = fmt.Sprintf("round(%s, 5-floor(log10(%s))) as %s", name, name, name)
		} else if col.FieldType.Tp == mysql.TypeDouble {
			name = fmt.Sprintf("round(%s, 14-floor(log10(%s))) as %s", name, name, name)
		}
		columnNames = append(columnNames, name)
	}
	columns := strings.Join(columnNames, ", ")
	if collation != "" {
		collation = fmt.Sprintf(" COLLATE \"%s\"", collation)
	}

	for i, key := range orderKeys {
		orderKeys[i] = dbutil.ColumnName(key)
	}

	query := fmt.Sprintf("SELECT /*!40001 SQL_NO_CACHE */ %s FROM %s WHERE %%s ORDER BY %s%s",
		columns, dbutil.TableName(schema, table), strings.Join(orderKeys, ","), collation)

	return query, orderKeyCols
}

func GenerateReplaceDML(data map[string]*dbutil.ColumnData, table *model.TableInfo, schema string) string {
	colNames := make([]string, 0, len(table.Columns))
	values := make([]string, 0, len(table.Columns))
	for _, col := range table.Columns {
		if col.IsGenerated() {
			continue
		}

		colNames = append(colNames, dbutil.ColumnName(col.Name.O))
		if data[col.Name.O].IsNull {
			values = append(values, "NULL")
			continue
		}

		if needQuotes(col.FieldType.Tp) {
			values = append(values, fmt.Sprintf("'%s'", strings.Replace(string(data[col.Name.O].Data), "'", "\\'", -1)))
		} else {
			values = append(values, string(data[col.Name.O].Data))
		}
	}

	return fmt.Sprintf("REPLACE INTO %s(%s) VALUES (%s);", dbutil.TableName(schema, table.Name.O), strings.Join(colNames, ","), strings.Join(values, ","))
}

func GenerateReplaceDMLWithAnnotation(source, target map[string]*dbutil.ColumnData, table *model.TableInfo, schema string) string {
	colNames := make([]string, 0, len(table.Columns))
	values1 := make([]string, 0, len(table.Columns))
	values2 := make([]string, 0, len(table.Columns))
	for _, col := range table.Columns {
		if col.IsGenerated() {
			continue
		}

		var data1, data2 *dbutil.ColumnData

		colNames = append(colNames, dbutil.ColumnName(col.Name.O))

		data1 = source[col.Name.O]
		if data1.IsNull {
			values1 = append(values1, "NULL")
		} else {
			if needQuotes(col.FieldType.Tp) {
				values1 = append(values1, fmt.Sprintf("'%s'", strings.Replace(string(data1.Data), "'", "\\'", -1)))
			} else {
				values1 = append(values1, string(data1.Data))
			}
		}

		data2 = target[col.Name.O]
		if source[col.Name.O].IsNull {
			values2 = append(values2, "NULL")
		} else {
			if needQuotes(col.FieldType.Tp) {
				values2 = append(values2, fmt.Sprintf("'%s'", strings.Replace(string(data2.Data), "'", "\\'", -1)))
			} else {
				values2 = append(values2, string(data2.Data))
			}
		}

	}

	return fmt.Sprintf("-- original data: (%s) VALUES (%s) \nREPLACE INTO %s(%s) VALUES (%s);", strings.Join(colNames, ","), strings.Join(values2, ","), dbutil.TableName(schema, table.Name.O), strings.Join(colNames, ","), strings.Join(values1, ","))
}

func GenerateDeleteDML(data map[string]*dbutil.ColumnData, table *model.TableInfo, schema string) string {
	kvs := make([]string, 0, len(table.Columns))
	for _, col := range table.Columns {
		if col.IsGenerated() {
			continue
		}

		if data[col.Name.O].IsNull {
			kvs = append(kvs, fmt.Sprintf("%s is NULL", dbutil.ColumnName(col.Name.O)))
			continue
		}

		if needQuotes(col.FieldType.Tp) {
			kvs = append(kvs, fmt.Sprintf("%s = '%s'", dbutil.ColumnName(col.Name.O), strings.Replace(string(data[col.Name.O].Data), "'", "\\'", -1)))
		} else {
			kvs = append(kvs, fmt.Sprintf("%s = %s", dbutil.ColumnName(col.Name.O), string(data[col.Name.O].Data)))
		}
	}
	return fmt.Sprintf("DELETE FROM %s WHERE %s;", dbutil.TableName(schema, table.Name.O), strings.Join(kvs, " AND "))

}

func needQuotes(tp byte) bool {
	return !(dbutil.IsNumberType(tp) || dbutil.IsFloatType(tp))
}

// CompareData compare two row datas.
// equal = true: map1 = map2
// equal = false:
// 		1. cmp = 0: map1 and map2 have the same orderkeycolumns, but other columns are in difference.
//		2. cmp = -1: map1 < map2
// 		3. cmp = 1: map1 > map2
func CompareData(map1, map2 map[string]*dbutil.ColumnData, orderKeyCols []*model.ColumnInfo) (equal bool, cmp int32, err error) {
	var (
		data1, data2 *dbutil.ColumnData
		key          string
		ok           bool
	)

	equal = true

	defer func() {
		if equal || err != nil {
			return
		}

		if cmp == 0 {
			log.Warn("find different row", zap.String("column", key), zap.String("row1", rowToString(map1)), zap.String("row2", rowToString(map2)))
		} else if cmp > 0 {
			log.Warn("target had superfluous data", zap.String("row", rowToString(map2)))
		} else {
			log.Warn("target lack data", zap.String("row", rowToString(map1)))
		}
	}()

	for key, data1 = range map1 {
		if data2, ok = map2[key]; !ok {
			return false, 0, errors.Errorf("don't have key %s", key)
		}
		if (string(data1.Data) == string(data2.Data)) && (data1.IsNull == data2.IsNull) {
			continue
		}
		equal = false

		break
	}
	if equal {
		return
	}

	for _, col := range orderKeyCols {
		if data1, ok = map1[col.Name.O]; !ok {
			err = errors.Errorf("don't have key %s", col.Name.O)
			return
		}
		if data2, ok = map2[col.Name.O]; !ok {
			err = errors.Errorf("don't have key %s", col.Name.O)
			return
		}

		if needQuotes(col.FieldType.Tp) {
			strData1 := string(data1.Data)
			strData2 := string(data2.Data)

			if len(strData1) == len(strData2) && strData1 == strData2 {
				continue
			}

			if strData1 < strData2 {
				cmp = -1
			} else {
				cmp = 1
			}
			break
		} else if data1.IsNull || data2.IsNull {
			if data1.IsNull && data2.IsNull {
				continue
			}

			if data1.IsNull {
				cmp = -1
			} else {
				cmp = 1
			}
			break
		} else {
			num1, err1 := strconv.ParseFloat(string(data1.Data), 64)
			num2, err2 := strconv.ParseFloat(string(data2.Data), 64)
			if err1 != nil || err2 != nil {
				err = errors.Errorf("convert %s, %s to float failed, err1: %v, err2: %v", string(data1.Data), string(data2.Data), err1, err2)
				return
			}

			if num1 == num2 {
				continue
			}

			if num1 < num2 {
				cmp = -1
			} else {
				cmp = 1
			}
			break
		}
	}

	return
}

func rowToString(row map[string]*dbutil.ColumnData) string {
	var s strings.Builder
	s.WriteString("{ ")
	for key, val := range row {
		if val.IsNull {
			s.WriteString(fmt.Sprintf("%s: IsNull, ", key))
		} else {
			s.WriteString(fmt.Sprintf("%s: %s, ", key, val.Data))
		}
	}
	s.WriteString(" }")

	return s.String()
}

func MinLenInSlices(slices [][]string) int {
	min := 0
	for i, slice := range slices {
		if i == 0 || len(slice) < min {
			min = len(slice)
		}
	}

	return min
}

// SliceToMap converts slice to map
func SliceToMap(slice []string) map[string]interface{} {
	sMap := make(map[string]interface{})
	for _, str := range slice {
		sMap[str] = struct{}{}
	}
	return sMap
}

// GetApproximateMid get a mid point from index columns by doing multiple sample, and get mid point from sample points
func GetApproximateMid(ctx context.Context, db *sql.DB, schema, table string, columns []*model.ColumnInfo, num int, limitRange string, limitArgs []interface{}, collation string) ([]string, error) {
	if len(columns) == 0 {
		return nil, errors.Annotate(errors.NotValidf("not valid columns"), "the columns is empty")
	}
	midValues := make([]string, len(columns))
	for i, column := range columns {
		randomValues, err := dbutil.GetRandomValues(ctx, db, schema, table, column.Name.O, num, limitRange, limitArgs, collation)
		if err != nil {
			return nil, errors.Trace(err)
		}
		midValues[i] = randomValues[len(randomValues)/2]
	}
	return midValues, nil
}

func GetApproximateMidBySize(ctx context.Context, db *sql.DB, schema, table string, tbInfo *model.TableInfo, limitRange string, args []interface{}, count int64) (map[string]string, error) {
	/*
		example
		mysql> select i_id, i_im_id, i_name from item where i_id > 0 order by i_id, i_im_id limit 5000,1;
		+------+---------+-----------------+
		| i_id | i_im_id | i_name          |
		+------+---------+-----------------+
		| 5001 |    3494 | S66WiWB3t1FUG02 |
		+------+---------+-----------------+
		1 row in set (0.09 sec)
	*/
	columnNames := make([]string, 0, len(tbInfo.Columns))
	for _, col := range tbInfo.Columns {
		columnNames = append(columnNames, dbutil.ColumnName(col.Name.O))
	}
	query := fmt.Sprintf("SELECT %s FROM %s WHERE %s ORDER BY %s LIMIT %s,1",
		strings.Join(columnNames, ", "),
		dbutil.TableName(schema, table),
		limitRange,
		strings.Join(columnNames, ", "),
		strconv.FormatInt(count/2, 10))
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer rows.Close()
	columns := make([]interface{}, len(tbInfo.Columns))
	for i := range columns {
		columns[i] = new(string)
	}
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return nil, errors.Trace(err)
		}
		log.Error("there is no row in result set")
	}
	err = rows.Scan(columns...)
	if err != nil {
		return nil, errors.Trace(err)
	}
	columnValues := make(map[string]string)
	for i, column := range columns {
		columnValues[columnNames[i][1:len(columnNames[i])-1]] = *column.(*string)
	}
	return columnValues, nil
}

func GetTableSize(ctx context.Context, db *sql.DB, schemaName, tableName string) (int64, error) {
	query := fmt.Sprintf("select sum(data_length) as data from `information_schema`.`tables` where table_schema='%s' and table_name='%s' GROUP BY data_length;", schemaName, tableName)
	var dataSize sql.NullInt64
	err := db.QueryRowContext(ctx, query).Scan(&dataSize)
	if err != nil {
		return int64(0), errors.Trace(err)
	}
	return dataSize.Int64, nil
}

// GetCountAndCRC32Checksum returns checksum code of some data by given condition
func GetCountAndCRC32Checksum(ctx context.Context, db *sql.DB, schemaName, tableName string, tbInfo *model.TableInfo, limitRange string, args []interface{}) (int64, int64, error) {
	/*
		calculate CRC32 checksum and count example:
		mysql> select count(t.checksum), BIT_XOR(t.checksum) from
		(select CAST(CRC32(CONCAT_WS(',', id, name, age, CONCAT(ISNULL(id), ISNULL(name), ISNULL(age))))AS UNSIGNED) as checksum from test.test where id > 0) as t;
		+--------+------------+
		| count  | checksum   |
		+--------+------------+
		| 100000 | 1128664311 |
		+--------+------------+
		1 row in set (0.46 sec)
	*/
	columnNames := make([]string, 0, len(tbInfo.Columns))
	columnIsNull := make([]string, 0, len(tbInfo.Columns))
	for _, col := range tbInfo.Columns {
		name := dbutil.ColumnName(col.Name.O)
		if col.FieldType.Tp == mysql.TypeFloat {
			name = fmt.Sprintf("round(%s, 5-floor(log10(%s)))", name, name)
		} else if col.FieldType.Tp == mysql.TypeDouble {
			name = fmt.Sprintf("round(%s, 14-floor(log10(%s)))", name, name)
		}
		columnNames = append(columnNames, name)
		columnIsNull = append(columnIsNull, fmt.Sprintf("ISNULL(%s)", dbutil.ColumnName(col.Name.O)))
	}

	query := fmt.Sprintf("SELECT COUNT(t.crc32) as CNT, BIT_XOR(t.crc32) as CHECKSUM from (SELECT CAST(CRC32(CONCAT_WS(',', %s, CONCAT(%s)))AS UNSIGNED) AS crc32 FROM %s WHERE %s) as t;",
		strings.Join(columnNames, ", "), strings.Join(columnIsNull, ", "), dbutil.TableName(schemaName, tableName), limitRange)
	log.Debug("count and checksum", zap.String("sql", query), zap.Reflect("args", args))

	var count sql.NullInt64
	var checksum sql.NullInt64
	err := db.QueryRowContext(ctx, query, args...).Scan(&count, &checksum)
	if err != nil {
		log.Warn("execute checksum query fail", zap.String("query", query), zap.Reflect("args", args))
		return -1, -1, errors.Trace(err)
	}
	if !count.Valid || !checksum.Valid {
		// if don't have any data, the checksum will be `NULL`
		log.Warn("get empty count or checksum", zap.String("sql", query), zap.Reflect("args", args))
		return 0, 0, nil
	}

	return count.Int64, checksum.Int64, nil
}

func IgnoreColumns(tableInfo *model.TableInfo, columns []string) *model.TableInfo {
	if len(columns) == 0 {
		return tableInfo
	}

	removeColMap := SliceToMap(columns)
	for i := 0; i < len(tableInfo.Indices); i++ {
		index := tableInfo.Indices[i]
		for j := 0; j < len(index.Columns); j++ {
			col := index.Columns[j]
			if _, ok := removeColMap[col.Name.O]; ok {
				tableInfo.Indices = append(tableInfo.Indices[:i], tableInfo.Indices[i+1:]...)
				i--
				break
			}
		}
	}

	for j := 0; j < len(tableInfo.Columns); j++ {
		col := tableInfo.Columns[j]
		if _, ok := removeColMap[col.Name.O]; ok {
			tableInfo.Columns = append(tableInfo.Columns[:j], tableInfo.Columns[j+1:]...)
			j--
		}
	}

	// calculate column offset
	colMap := make(map[string]int, len(tableInfo.Columns))
	for i, col := range tableInfo.Columns {
		col.Offset = i
		colMap[col.Name.O] = i
	}

	for _, index := range tableInfo.Indices {
		for _, col := range index.Columns {
			offset, ok := colMap[col.Name.O]
			if !ok {
				// this should never happened
				log.Fatal("column not exists", zap.String("column", col.Name.O))
			}
			col.Offset = offset
		}
	}

	return tableInfo
}

func UniqueID(schema string, table string) string {
	return schema + ":" + table
}

func GetBetterIndex(ctx context.Context, db *sql.DB, schema, table string, tableInfo *model.TableInfo, indices []*model.IndexInfo) error {
	// SELECT COUNT(DISTINCT city)/COUNT(*) FROM `schema`.`table`;
	sels := make([]float64, len(indices))
	for _, index := range indices {
		if index.Primary || index.Unique {
			return nil
		}
		column := GetColumnsFromIndex(index, tableInfo)[0]
		selectivity, err := GetSelectivity(ctx, db, schema, table, column.Name.O, tableInfo)
		if err != nil {
			return errors.Trace(err)
		}
		log.Debug("index selectivity", zap.String("table", dbutil.TableName(schema, table)), zap.Float64("selectivity", selectivity))
		sels = append(sels, selectivity)
	}
	sort.Slice(indices, func(i, j int) bool { return sels[i] > sels[j] })
	return nil
}

func GetSelectivity(ctx context.Context, db *sql.DB, schemaName, tableName, columnName string, tbInfo *model.TableInfo) (float64, error) {
	query := fmt.Sprintf("SELECT COUNT(DISTINCE %s)/COUNT(1) as SEL FROM %s;", columnName, dbutil.TableName(schemaName, tableName))
	var selectivity sql.NullFloat64
	args := []interface{}{}
	err := db.QueryRowContext(ctx, query, args...).Scan(&selectivity)
	if err != nil {
		log.Warn("execute get selectivity query fail", zap.String("query", query))
		return 0.0, errors.Trace(err)
	}
	if !selectivity.Valid {
		// if don't have any data, the checksum will be `NULL`
		log.Warn("get empty count or checksum", zap.String("sql", query))
		return 0.0, nil
	}
	return selectivity.Float64, nil
}
