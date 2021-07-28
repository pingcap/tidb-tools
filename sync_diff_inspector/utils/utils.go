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
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb-tools/pkg/dbutil"
	"github.com/pingcap/tidb-tools/sync_diff_inspector/config"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

// WorkerPool contains a pool of workers.
type WorkerPool struct {
	limit   uint
	workers chan *Worker
	name    string
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

// Apply executes a task.
func (pool *WorkerPool) Apply(fn taskFunc) {
	worker := pool.apply()
	go func() {
		defer pool.recycle(worker)
		fn()
	}()
}

// ApplyWithID execute a task and provides it with the worker ID.
func (pool *WorkerPool) ApplyWithID(fn identifiedTaskFunc) {
	worker := pool.apply()
	go func() {
		defer pool.recycle(worker)
		fn(worker.ID)
	}()
}

// ApplyOnErrorGroup executes a task in an errorgroup.
func (pool *WorkerPool) ApplyOnErrorGroup(eg *errgroup.Group, fn func() error) {
	worker := pool.apply()
	eg.Go(func() error {
		defer pool.recycle(worker)
		return fn()
	})
}

// ApplyWithIDInErrorGroup executes a task in an errorgroup and provides it with the worker ID.
func (pool *WorkerPool) ApplyWithIDInErrorGroup(eg *errgroup.Group, fn func(id uint64) error) {
	worker := pool.apply()
	eg.Go(func() error {
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
func sliceToMap(slice []string) map[string]interface{} {
	sMap := make(map[string]interface{})
	for _, str := range slice {
		sMap[str] = struct{}{}
	}
	return sMap
}

func GetAllTables(ctx context.Context, cfg *config.Config) (map[string]map[string]map[string]interface{}, error) {
	// instanceID => schema => table
	allTablesMap := make(map[string]map[string]map[string]interface{})

	allTablesMap[cfg.TargetDBCfg.InstanceID] = make(map[string]map[string]interface{})
	targetSchemas, err := dbutil.GetSchemas(ctx, cfg.TargetDBCfg.Conn)
	if err != nil {
		return nil, errors.Annotatef(err, "get schemas from %s", cfg.TargetDBCfg.InstanceID)
	}

	for _, schema := range targetSchemas {
		allTables, err := dbutil.GetTables(ctx, cfg.TargetDBCfg.Conn, schema)
		if err != nil {
			return nil, errors.Annotatef(err, "get tables from %s.%s", cfg.TargetDBCfg.InstanceID, schema)
		}
		allTablesMap[cfg.TargetDBCfg.InstanceID][schema] = sliceToMap(allTables)
	}

	for _, source := range cfg.SourceDBCfg {
		allTablesMap[source.InstanceID] = make(map[string]map[string]interface{})
		sourceSchemas, err := dbutil.GetSchemas(ctx, source.Conn)
		if err != nil {
			return nil, errors.Annotatef(err, "get schemas from %s", source.InstanceID)
		}

		for _, schema := range sourceSchemas {
			allTables, err := dbutil.GetTables(ctx, source.Conn, schema)
			if err != nil {
				return nil, errors.Annotatef(err, "get tables from %s.%s", source.InstanceID, schema)
			}
			allTablesMap[source.InstanceID][schema] = sliceToMap(allTables)
		}
	}

	return allTablesMap, nil
}

// GetMatchTable returns all the matched table.
func GetMatchTable(db *config.DBConfig, schema, table string, allTables map[string]interface{}) ([]string, error) {
	tableNames := make([]string, 0, 1)

	if table[0] == '~' {
		tableRegex := regexp.MustCompile(fmt.Sprintf("(?i)%s", table[1:]))
		for tableName := range allTables {
			if !tableRegex.MatchString(tableName) {
				continue
			}
			tableNames = append(tableNames, tableName)
		}
	} else {
		if _, ok := allTables[table]; ok {
			tableNames = append(tableNames, table)
		} else {
			return nil, errors.Errorf("%s.%s not found in %s", schema, table, db.InstanceID)
		}
	}

	return tableNames, nil
}

// Judge if a table is in "exclude-tables" list
func InExcludeTables(exclude_tables []string, table string) bool {
	for _, exclude_table := range exclude_tables {
		if strings.EqualFold(exclude_table, table) {
			return true
		}
	}
	return false
}
