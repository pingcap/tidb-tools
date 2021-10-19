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
	"math"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/olekukonko/tablewriter"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb-tools/pkg/dbutil"
	"github.com/pingcap/tidb-tools/sync_diff_inspector/chunk"
	"go.uber.org/zap"
)

// WorkerPool contains a pool of workers.
// The number of workers in the channel represents how many goruntines
// can be created to execute the task.
// After the task is done, worker will be sent back to the channel.
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

// NewWorkerPool returns a WorkerPool with `limit` workers in the channel.
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

// Apply wait for an idle worker to run `taskFunc`.
// Notice: function `Apply` and `WaitFinished` cannot be called in parallel
func (pool *WorkerPool) Apply(fn taskFunc) {
	worker := pool.apply()
	pool.wg.Add(1)
	go func() {
		defer pool.wg.Done()
		defer pool.recycle(worker)
		fn()
	}()
}

// apply waits for an idle worker from the channel and return it
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

// recycle sends an idle worker back to the channel
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

// GetColumnsFromIndex returns `ColumnInfo`s of the specified index.
func GetColumnsFromIndex(index *model.IndexInfo, tableInfo *model.TableInfo) []*model.ColumnInfo {
	indexColumns := make([]*model.ColumnInfo, 0, len(index.Columns))
	for _, indexColumn := range index.Columns {
		indexColumns = append(indexColumns, tableInfo.Columns[indexColumn.Offset])
	}

	return indexColumns
}

// GetTableRowsQueryFormat returns a rowsQuerySQL template for the specific table.
//  e.g. SELECT /*!40001 SQL_NO_CACHE */ `a`, `b` FROM `schema`.`table` WHERE %s ORDER BY `a`.
func GetTableRowsQueryFormat(schema, table string, tableInfo *model.TableInfo, collation string) (string, []*model.ColumnInfo) {
	orderKeys, orderKeyCols := dbutil.SelectUniqueOrderKey(tableInfo)

	columnNames := make([]string, 0, len(tableInfo.Columns))
	for _, col := range tableInfo.Columns {
		columnNames = append(columnNames, dbutil.ColumnName(col.Name.O))
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

// GenerateReplaceDML returns the insert SQL for the specific row values.
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

// GerateReplaceDMLWithAnnotation returns the replace SQL for the specific 2 rows.
// And add Annotations to show the different columns.
func GenerateReplaceDMLWithAnnotation(source, target map[string]*dbutil.ColumnData, table *model.TableInfo, schema string) string {
	sqlColNames := make([]string, 0, len(table.Columns))
	sqlValues := make([]string, 0, len(table.Columns))
	colNames := append(make([]string, 0, len(table.Columns)+1), "diff columns")
	values1 := append(make([]string, 0, len(table.Columns)+1), "source data")
	values2 := append(make([]string, 0, len(table.Columns)+1), "target data")
	tableString := &strings.Builder{}
	diffTable := tablewriter.NewWriter(tableString)
	for _, col := range table.Columns {
		if col.IsGenerated() {
			continue
		}

		var data1, data2 *dbutil.ColumnData
		var value1 string
		data1 = source[col.Name.O]
		data2 = target[col.Name.O]

		if data1.IsNull {
			value1 = "NULL"
		} else {
			if needQuotes(col.FieldType.Tp) {
				value1 = fmt.Sprintf("'%s'", strings.Replace(string(data1.Data), "'", "\\'", -1))
			} else {
				value1 = string(data1.Data)
			}
		}
		colName := dbutil.ColumnName(col.Name.O)
		sqlColNames = append(sqlColNames, colName)
		sqlValues = append(sqlValues, value1)

		// Only show different columns in annotations.
		if (string(data1.Data) == string(data2.Data)) && (data1.IsNull == data2.IsNull) {
			continue
		}

		colNames = append(colNames, colName)
		values1 = append(values1, value1)

		if data2.IsNull {
			values2 = append(values2, "NULL")
		} else {
			if needQuotes(col.FieldType.Tp) {
				values2 = append(values2, fmt.Sprintf("'%s'", strings.Replace(string(data2.Data), "'", "\\'", -1)))
			} else {
				values2 = append(values2, string(data2.Data))
			}
		}

	}

	diffTable.SetRowLine(true)
	diffTable.SetHeader(colNames)
	diffTable.Append(values1)
	diffTable.Append(values2)
	diffTable.SetCenterSeparator("╋")
	diffTable.SetColumnSeparator("╏")
	diffTable.SetRowSeparator("╍")
	diffTable.SetAlignment(tablewriter.ALIGN_LEFT)
	diffTable.SetBorder(false)
	diffTable.Render()

	return fmt.Sprintf("/*\n%s*/\nREPLACE INTO %s(%s) VALUES (%s);", tableString.String(), dbutil.TableName(schema, table.Name.O), strings.Join(sqlColNames, ","), strings.Join(sqlValues, ","))
}

// GerateReplaceDMLWithAnnotation returns the delete SQL for the specific row.
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
	return fmt.Sprintf("DELETE FROM %s WHERE %s LIMIT 1;", dbutil.TableName(schema, table.Name.O), strings.Join(kvs, " AND "))

}

// isCompatible checks whether 2 column types are compatible.
// e.g. char and vachar.
func isCompatible(tp1, tp2 byte) bool {
	if tp1 == tp2 {
		return true
	}

	log.Warn("column type different, check compatibility.")
	var t1, t2 int
	switch tp1 {
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeLong, mysql.TypeLonglong, mysql.TypeInt24:
		t1 = 1
	case mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob, mysql.TypeBlob:
		t1 = 2
	case mysql.TypeVarString, mysql.TypeString, mysql.TypeVarchar:
		t1 = 3
	default:
		t1 = 111
	}

	switch tp2 {
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeLong, mysql.TypeLonglong, mysql.TypeInt24:
		t2 = 1
	case mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob, mysql.TypeBlob:
		t2 = 2
	case mysql.TypeVarString, mysql.TypeString, mysql.TypeVarchar:
		t2 = 3
	default:
		t2 = 222
	}

	return t1 == t2
}

// CompareStruct compare tables' columns and indices from upstream and downstream.
// There are 2 return values:
// 	isEqual	: result of comparing tables' columns and indices
// 	isPanic	: the differences of tables' struct can not be ignored. Need to skip data comparing.
func CompareStruct(upstreamTableInfos []*model.TableInfo, downstreamTableInfo *model.TableInfo) (isEqual bool, isPanic bool) {
	// compare columns
	for _, upstreamTableInfo := range upstreamTableInfos {
		if len(upstreamTableInfo.Columns) != len(downstreamTableInfo.Columns) {
			// the numbers of each columns are different, don't compare data
			log.Error("column num not equal", zap.String("upstream table", upstreamTableInfo.Name.O), zap.Int("column num", len(upstreamTableInfo.Columns)), zap.String("downstream table", downstreamTableInfo.Name.O), zap.Int("column num", len(upstreamTableInfo.Columns)))
			return false, true
		}

		for i, column := range upstreamTableInfo.Columns {
			if column.Name.O != downstreamTableInfo.Columns[i].Name.O {
				// names are different, panic!
				log.Error("column name not equal", zap.String("upstream table", upstreamTableInfo.Name.O), zap.String("column name", column.Name.O), zap.String("downstream table", downstreamTableInfo.Name.O), zap.String("column name", downstreamTableInfo.Columns[i].Name.O))
				return false, true
			}

			if !isCompatible(column.Tp, downstreamTableInfo.Columns[i].Tp) {
				// column types are different, panic!
				log.Error("column type not compatible", zap.String("upstream table", upstreamTableInfo.Name.O), zap.String("column name", column.Name.O), zap.Uint8("column type", column.Tp), zap.String("downstream table", downstreamTableInfo.Name.O), zap.String("column name", downstreamTableInfo.Columns[i].Name.O), zap.Uint8("column type", downstreamTableInfo.Columns[i].Tp))
				return false, true
			}
		}
	}

	// compare indices
	deleteIndicesSet := make(map[string]struct{})
	unilateralIndicesSet := make(map[string]struct{})
	downstreamIndicesMap := make(map[string]*struct {
		index *model.IndexInfo
		cnt   int
	})
	for _, index := range downstreamTableInfo.Indices {
		downstreamIndicesMap[index.Name.O] = &struct {
			index *model.IndexInfo
			cnt   int
		}{index, 0}
	}
	for _, upstreamTableInfo := range upstreamTableInfos {

	NextIndex:
		for _, upstreamIndex := range upstreamTableInfo.Indices {
			if _, ok := deleteIndicesSet[upstreamIndex.Name.O]; ok {
				continue NextIndex
			}

			indexU, ok := downstreamIndicesMap[upstreamIndex.Name.O]
			if ok {
				if len(indexU.index.Columns) != len(upstreamIndex.Columns) {
					// different index, should be removed
					deleteIndicesSet[upstreamIndex.Name.O] = struct{}{}
					continue NextIndex
				}

				for i, indexColumn := range upstreamIndex.Columns {
					if indexColumn.Offset != indexU.index.Columns[i].Offset || indexColumn.Name.O != indexU.index.Columns[i].Name.O {
						// different index, should be removed
						deleteIndicesSet[upstreamIndex.Name.O] = struct{}{}
						continue NextIndex
					}
				}
				indexU.cnt = indexU.cnt + 1
			} else {
				unilateralIndicesSet[upstreamIndex.Name.O] = struct{}{}
			}
		}
	}

	existBilateralIndex := false
	for _, indexU := range downstreamIndicesMap {
		if _, ok := deleteIndicesSet[indexU.index.Name.O]; ok {
			continue
		}
		if indexU.cnt < len(upstreamTableInfos) {
			// Some upstreamInfos don't have this index.
			unilateralIndicesSet[indexU.index.Name.O] = struct{}{}
		} else {
			// there is an index the whole tables have,
			// so unilateral indices can be deleted.
			existBilateralIndex = true
		}
	}

	// delete indices
	// If there exist bilateral index, unilateral indices can be deleted.
	if existBilateralIndex {
		for indexName := range unilateralIndicesSet {
			deleteIndicesSet[indexName] = struct{}{}
		}
	} else {
		log.Warn("no index exists in both upstream and downstream", zap.String("table", downstreamTableInfo.Name.O))
	}
	if len(deleteIndicesSet) > 0 {
		newDownstreamIndices := make([]*model.IndexInfo, 0, len(downstreamTableInfo.Indices))
		for _, index := range downstreamTableInfo.Indices {
			if _, ok := deleteIndicesSet[index.Name.O]; !ok {
				newDownstreamIndices = append(newDownstreamIndices, index)
			} else {
				log.Debug("delete downstream index", zap.String("name", downstreamTableInfo.Name.O), zap.String("index", index.Name.O))
			}
		}
		downstreamTableInfo.Indices = newDownstreamIndices

		for _, upstreamTableInfo := range upstreamTableInfos {
			newUpstreamIndices := make([]*model.IndexInfo, 0, len(upstreamTableInfo.Indices))
			for _, index := range upstreamTableInfo.Indices {
				if _, ok := deleteIndicesSet[index.Name.O]; !ok {
					newUpstreamIndices = append(newUpstreamIndices, index)
				} else {
					log.Debug("delete upstream index", zap.String("name", upstreamTableInfo.Name.O), zap.String("index", index.Name.O))
				}
			}
			upstreamTableInfo.Indices = newUpstreamIndices
		}

	}

	return len(deleteIndicesSet) == 0, false
}

// needQuotes determines whether an escape character is required for `'`.
func needQuotes(tp byte) bool {
	return !(dbutil.IsNumberType(tp) || dbutil.IsFloatType(tp))
}

// CompareData compare two row datas.
// equal = true: map1 = map2
// equal = false:
// 		1. cmp = 0: map1 and map2 have the same orderkeycolumns, but other columns are in difference.
//		2. cmp = -1: map1 < map2 (by comparing the orderkeycolumns)
// 		3. cmp = 1: map1 > map2
func CompareData(map1, map2 map[string]*dbutil.ColumnData, orderKeyCols, columns []*model.ColumnInfo) (equal bool, cmp int32, err error) {
	var (
		data1, data2 *dbutil.ColumnData
		str1, str2   string
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

	for _, column := range columns {
		if data1, ok = map1[column.Name.O]; !ok {
			return false, 0, errors.Errorf("upstream don't have key %s", key)
		}
		if data2, ok = map2[column.Name.O]; !ok {
			return false, 0, errors.Errorf("downstream don't have key %s", key)
		}
		str1 = string(data1.Data)
		str2 = string(data2.Data)
		if column.FieldType.Tp == mysql.TypeFloat || column.FieldType.Tp == mysql.TypeDouble {
			if data1.IsNull == data2.IsNull && data1.IsNull {
				continue
			}

			num1, err1 := strconv.ParseFloat(str1, 64)
			num2, err2 := strconv.ParseFloat(str2, 64)
			if err1 != nil || err2 != nil {
				err = errors.Errorf("convert %s, %s to float failed, err1: %v, err2: %v", str1, str2, err1, err2)
				return
			}
			if math.Abs(num1-num2) <= 1e-6 {
				continue
			}
		} else {
			if (str1 == str2) && (data1.IsNull == data2.IsNull) {
				continue
			}
		}

		equal = false
		break

	}
	if equal {
		return
	}

	// Not Equal. Compare orderkeycolumns.
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

// rowtoString covert rowData to String
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

// MinLenInSlices returns the smallest length among slices.
func MinLenInSlices(slices [][]string) int {
	min := 0
	for i, slice := range slices {
		if i == 0 || len(slice) < min {
			min = len(slice)
		}
	}

	return min
}

// SliceToMap converts Slice to Set
func SliceToMap(slice []string) map[string]interface{} {
	sMap := make(map[string]interface{})
	for _, str := range slice {
		sMap[str] = struct{}{}
	}
	return sMap
}

// GetApproximateMidBySize return the `count`th row in rows that meet the `limitRange`.
func GetApproximateMidBySize(ctx context.Context, db *sql.DB, schema, table string, indexColumns []*model.ColumnInfo, limitRange string, args []interface{}, count int64) (map[string]string, error) {
	/*
		example
		mysql> select i_id, i_im_id, i_name from item where i_id > 0 order by i_id, i_im_id, i_name limit 5000,1;
		+------+---------+-----------------+
		| i_id | i_im_id | i_name          |
		+------+---------+-----------------+
		| 5001 |    3494 | S66WiWB3t1FUG02 |
		+------+---------+-----------------+
		1 row in set (0.09 sec)
	*/
	columnNames := make([]string, 0, len(indexColumns))
	for _, col := range indexColumns {
		columnNames = append(columnNames, dbutil.ColumnName(col.Name.O))
	}
	query := fmt.Sprintf("SELECT %s FROM %s WHERE %s ORDER BY %s LIMIT %s,1",
		strings.Join(columnNames, ", "),
		dbutil.TableName(schema, table),
		limitRange,
		strings.Join(columnNames, ", "),
		strconv.FormatInt(count/2, 10))
	log.Debug("get mid by size", zap.String("sql", query), zap.Reflect("args", args))
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer rows.Close()
	columns := make([]interface{}, len(indexColumns))
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

// GetTableSize loads the TableSize from `information_schema`.`tables`.
func GetTableSize(ctx context.Context, db *sql.DB, schemaName, tableName string) (int64, error) {
	query := fmt.Sprintf("select sum(data_length) as data from `information_schema`.`tables` where table_schema='%s' and table_name='%s' GROUP BY data_length;", schemaName, tableName)
	var dataSize sql.NullInt64
	err := db.QueryRowContext(ctx, query).Scan(&dataSize)
	if err != nil {
		return int64(0), errors.Trace(err)
	}
	return dataSize.Int64, nil
}

// GetCountAndCRC32Checksum returns checksum code and count of some data by given condition
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
		// When col value is 0, the result is NULL.
		// But we can use ISNULL to distinguish between null and 0.
		if col.FieldType.Tp == mysql.TypeFloat {
			name = fmt.Sprintf("round(%s, 5-floor(log10(abs(%s))))", name, name)
		} else if col.FieldType.Tp == mysql.TypeDouble {
			name = fmt.Sprintf("round(%s, 14-floor(log10(abs(%s))))", name, name)
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
		log.Warn("execute checksum query fail", zap.String("query", query), zap.Reflect("args", args), zap.Error(err))
		return -1, -1, errors.Trace(err)
	}
	if !count.Valid || !checksum.Valid {
		// if don't have any data, the checksum will be `NULL`
		log.Warn("get empty count or checksum", zap.String("sql", query), zap.Reflect("args", args))
		return 0, 0, nil
	}

	return count.Int64, checksum.Int64, nil
}

// IgnoreColumns removes index from `tableInfo.Indices`, whose columns appear in `columns`.
// And removes column from `tableInfo.Columns`, which appears in `columns`.
// And initializes the offset of the column of each index to new `tableInfo.Columns`.
func IgnoreColumns(tableInfo *model.TableInfo, columns []string) *model.TableInfo {
	if len(columns) == 0 {
		return tableInfo
	}

	// Remove all index from `tableInfo.Indices`, whose columns are involved of any column in `columns`.
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

	// Remove column from `tableInfo.Columns`, which appears in `columns`.
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

	// Initialize the offset of the column of each index to new `tableInfo.Columns`.
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

// UniqueID returns `schema:table`
func UniqueID(schema string, table string) string {
	return schema + ":" + table
}

// GetBetterIndex returns the index more dinstict.
// If the index is primary key or unique, it can be return directly.
// Otherwise select the index which has higher value of `COUNT(DISTINCT a)/COUNT(*)`.
func GetBetterIndex(ctx context.Context, db *sql.DB, schema, table string, tableInfo *model.TableInfo) ([]*model.IndexInfo, error) {
	// SELECT COUNT(DISTINCT city)/COUNT(*) FROM `schema`.`table`;
	indices := dbutil.FindAllIndex(tableInfo)
	for _, index := range indices {
		if index.Primary || index.Unique {
			return []*model.IndexInfo{index}, nil
		}
	}
	sels := make([]float64, len(indices))
	for _, index := range indices {
		column := GetColumnsFromIndex(index, tableInfo)[0]
		selectivity, err := GetSelectivity(ctx, db, schema, table, column.Name.O, tableInfo)
		if err != nil {
			return indices, errors.Trace(err)
		}
		log.Debug("index selectivity", zap.String("table", dbutil.TableName(schema, table)), zap.Float64("selectivity", selectivity))
		sels = append(sels, selectivity)
	}
	sort.Slice(indices, func(i, j int) bool {
		return sels[i] > sels[j]
	})
	return indices, nil
}

// GetSelectivity returns the value of `COUNT(DISTINCT col)/COUNT(1)` SQL.
func GetSelectivity(ctx context.Context, db *sql.DB, schemaName, tableName, columnName string, tbInfo *model.TableInfo) (float64, error) {
	query := fmt.Sprintf("SELECT COUNT(DISTINCT %s)/COUNT(1) as SEL FROM %s;", dbutil.ColumnName(columnName), dbutil.TableName(schemaName, tableName))
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

// CalculateChunkSize returns chunkSize according to table rows count.
func CalculateChunkSize(rowCount int64) int64 {
	// we assume chunkSize is 50000 for any cluster.
	chunkSize := int64(50000)
	if rowCount > int64(chunkSize)*10000 {
		// we assume we only need 10k chunks for any table.
		chunkSize = rowCount / 10000
	}
	return chunkSize
}

// AnalyzeTable do 'ANALYZE TABLE `table`' SQL.
func AnalyzeTable(ctx context.Context, db *sql.DB, tableName string) error {
	_, err := db.ExecContext(ctx, "ANALYZE TABLE "+tableName)
	return err
}

// GetSQLFileName returns filename of fix-SQL identified by chunk's `Index`.
func GetSQLFileName(index *chunk.ChunkID) string {
	return fmt.Sprintf("%d:%d-%d:%d", index.TableIndex, index.BucketIndexLeft, index.BucketIndexRight, index.ChunkIndex)
}

// GetChunkIDFromSQLFileName convert the filename to chunk's `Index`.
func GetChunkIDFromSQLFileName(fileIDStr string) (int, int, int, int, error) {
	ids := strings.Split(fileIDStr, ":")
	tableIndex, err := strconv.Atoi(ids[0])
	if err != nil {
		return 0, 0, 0, 0, errors.Trace(err)
	}
	bucketIndex := strings.Split(ids[1], "-")
	bucketIndexLeft, err := strconv.Atoi(bucketIndex[0])
	if err != nil {
		return 0, 0, 0, 0, errors.Trace(err)
	}
	bucketIndexRight, err := strconv.Atoi(bucketIndex[1])
	if err != nil {
		return 0, 0, 0, 0, errors.Trace(err)
	}
	chunkIndex, err := strconv.Atoi(ids[2])
	if err != nil {
		return 0, 0, 0, 0, errors.Trace(err)
	}
	return tableIndex, bucketIndexLeft, bucketIndexRight, chunkIndex, nil
}
