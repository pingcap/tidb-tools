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

package util

import (
	"database/sql"
	"fmt"
	"reflect"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb-tools/pkg/db"
	"github.com/pingcap/tidb/model"
)

// chunkRange represents chunk range
type chunkRange struct {
	begin interface{}
	end   interface{}
	// for example:
	// containB and containE is true, means [begin, end]
	// containB is true, containE is false, means [begin, end)
	containB bool
	containE bool
	notNil   bool
}

// DumpJob is the struct for job of dump
type DumpJob struct {
	DbName string
	Table  string
	Column *model.ColumnInfo
	Where  string
	Chunk  chunkRange
}

// newChunkRange return a range struct
func newChunkRange(begin, end interface{}, containB, containE bool, notNil bool) chunkRange {
	return chunkRange{
		begin:    begin,
		end:      end,
		containE: containE,
		containB: containB,
		notNil:   notNil,
	}
}

func getChunksForTable(db *sql.DB, dbname, tableName string, column *model.ColumnInfo, where string, chunkSize, sample int) ([]chunkRange, error) {
	noChunks := []chunkRange{{}}
	if column == nil {
		log.Warnf("No suitable index found for %s.%s", dbname, tableName)
		return noChunks, nil
	}

	field := column.Name.O

	// fetch min, max
	query := fmt.Sprintf("SELECT %s MIN(`%s`) as MIN, MAX(`%s`) as MAX FROM `%s`.`%s` where %s",
		"/*!40001 SQL_NO_CACHE */", field, field, dbname, tableName, where)
	log.Debugf("[dumper] get max min query sql: %s", query)

	// get the chunk count
	cnt, err := pkgdb.GetCount(db, dbname, tableName, where)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if cnt == 0 {
		log.Infof("no data found in %s.%s", dbname, tableName)
		return noChunks, nil
	}

	chunkCnt := cnt / int64(chunkSize)
	if sample != 100 {
		// use sampling check, can check more fragmented by split to more chunk
		chunkCnt *= 10
	}

	var chunk chunkRange
	if pkgdb.IsNumberType(column.Tp) {
		var min, max sql.NullInt64
		err := db.QueryRow(query).Scan(&min, &max)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if !min.Valid {
			// min is NULL, means that no table data.
			return []chunkRange{}, nil
		}
		chunk = newChunkRange(min.Int64, max.Int64+1, true, false, true)
	} else if pkgdb.IsFloatType(column.Tp) {
		var min, max sql.NullFloat64
		err := db.QueryRow(query).Scan(&min, &max)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if !min.Valid {
			// min is NULL, means that no table data.
			return []chunkRange{}, nil
		}
		chunk = newChunkRange(min.Float64-0.1, max.Float64+0.1, false, false, true)
	} else {
		var min, max sql.NullString
		err := db.QueryRow(query).Scan(&min, &max)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if !min.Valid || !max.Valid {
			return []chunkRange{}, nil
		}
		chunk = newChunkRange(min, max, true, true, true)
	}
	return splitRange(db, &chunk, chunkCnt, dbname, tableName, column, where)
}

func splitRange(db *sql.DB, chunk *chunkRange, count int64, dbname string, table string, column *model.ColumnInfo, timeRange string) ([]chunkRange, error) {
	var chunks []chunkRange

	if count <= 1 {
		chunks = append(chunks, *chunk)
		return chunks, nil
	}

	if !chunk.notNil {
		return nil, errors.Errorf("the chunk is empty: %v", chunk)
	}

	if reflect.TypeOf(chunk.begin).String() == "int64" {
		min, ok1 := chunk.begin.(int64)
		max, ok2 := chunk.end.(int64)
		if !ok1 || !ok2 {
			return nil, errors.Errorf("can't parse chunk's begin: %v, end: %v", chunk.begin, chunk.end)
		}
		step := (max-min)/count + 1
		cutoff := min
		for cutoff <= max {
			r := newChunkRange(cutoff, cutoff+step, true, false, true)
			chunks = append(chunks, r)
			cutoff += step
		}
		log.Debugf("getChunksForTable cut table: cnt=%d min=%v max=%v step=%v chunk=%d",
			count, min, max, step, len(chunks))
	} else if reflect.TypeOf(chunk.begin).String() == "float64" {
		min, ok1 := chunk.begin.(float64)
		max, ok2 := chunk.end.(float64)
		if !ok1 || !ok2 {
			return nil, errors.Errorf("can't parse chunk's begin: %v, end: %v", chunk.begin, chunk.end)
		}
		step := (max-min)/float64(count) + 1
		cutoff := min
		for cutoff <= max {
			r := newChunkRange(cutoff, cutoff+step, true, false, true)
			chunks = append(chunks, r)
			cutoff += step
		}
		log.Debugf("getChunksForTable cut table: cnt=%d min=%v max=%v step=%v chunk=%d",
			count, min, max, step, len(chunks))
	} else {
		max, ok1 := chunk.end.(string)
		min, ok2 := chunk.begin.(string)
		if !ok1 || !ok2 {
			return nil, errors.Errorf("can't parse chunk's begin: %v, end: %v", chunk.begin, chunk.end)
		}

		// get random value as split value
		splitValues, err := pkgdb.GetRandomValues(db, dbname, table, column.Name.O, count-1, min, max, timeRange)
		if err != nil {
			return nil, errors.Trace(err)
		}
		var minTmp, maxTmp string
		var i int64
		for i = 0; i < count; i++ {
			if i == 0 {
				minTmp = min
				maxTmp = splitValues[i]
			} else if i == int64(len(splitValues)) {
				minTmp = splitValues[i-1]
				maxTmp = max
			} else {
				minTmp = splitValues[i-1]
				maxTmp = splitValues[i]
			}
			r := newChunkRange(minTmp, maxTmp, true, false, true)
			chunks = append(chunks, r)
		}
		log.Debugf("[dumper] getChunksForTable cut table: cnt=%d min=%s max=%s chunk=%d",
			count, min, max, len(chunks))
	}
	chunks[0].containB = chunk.containB
	chunks[len(chunks)-1].containE = chunk.containE
	return chunks, nil
}

func findSuitableField(db *sql.DB, dbname string, table string, useRowID bool) (*model.ColumnInfo, error) {
	// first select the index, and number type index first
	column, err := pkgdb.FindSuitableIndex(db, dbname, table, useRowID)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if column != nil {
		return column, nil
	}
	log.Infof("%s.%s don't have index, will use a number column as split field", dbname, table)

	// use the first column
	column, err = pkgdb.GetFirstColumn(db, dbname, table)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if column != nil {
		return column, nil
	}

	return nil, errors.Errorf("no column find in table %s.%s", dbname, table)
}

// GenerateDumpJob generates some DumpJobs.
func GenerateDumpJob(db *sql.DB, dbname, tableName, splitField string,
	limitRange string, chunkSize int, sample int, useRowID bool) ([]*DumpJob, error) {
	jobBucket := make([]*DumpJob, 0, 10)
	var jobCnt int
	var column *model.ColumnInfo
	var err error

	if splitField == "" {
		// find a column for split data
		column, err = findSuitableField(db, dbname, tableName, useRowID)
		if err != nil {
			return nil, errors.Trace(err)
		}
	} else {
		var table *model.TableInfo
		table, err = pkgdb.GetSchemaTable(db, dbname, tableName)
		if err != nil {
			return nil, errors.Trace(err)
		}
		exist := false
		column, exist = pkgdb.GetColumnByName(table, splitField)
		if !exist {
			return nil, fmt.Errorf("can't find column %s in table %s", splitField, tableName)
		}
	}

	// set limitRange to "true" can make the code more simple, no need to judge the limitRange's value.
	// for example: sql will looks like "select * from itest where a > 10 AND true" if don't set range in config.
	if limitRange == "" {
		limitRange = "true"
	}

	chunks, err := getChunksForTable(db, dbname, tableName, column, limitRange, chunkSize, sample)
	if err != nil {
		return nil, errors.Trace(err)
	}
	log.Debugf("chunks: %+v", chunks)

	jobCnt += len(chunks)
	var (
		chunk chunkRange
		where string
	)
	for {
		length := len(chunks)
		if length == 0 {
			break
		}
		if length%2 == 0 {
			chunk = chunks[0]
			chunks = chunks[1:]
		} else {
			chunk = chunks[length-1]
			chunks = chunks[:length-1]
		}
		if chunk.notNil {
			gt := ">"
			lt := "<"
			if chunk.containB {
				gt = ">="
			}
			if chunk.containE {
				lt = "<="
			}
			if reflect.TypeOf(chunk.begin).String() == "int64" {
				where = fmt.Sprintf("(`%s` %s %d AND `%s` %s %d)", column.Name, gt, chunk.begin, column.Name, lt, chunk.end)
			} else if reflect.TypeOf(chunk.begin).String() == "float64" {
				where = fmt.Sprintf("(`%s` %s %f AND `%s` %s %f)", column.Name, gt, chunk.begin, column.Name, lt, chunk.end)
			} else {
				where = fmt.Sprintf("(`%s` %s \"%v\" AND `%s` %s \"%v\")", column.Name, gt, chunk.begin, column.Name, lt, chunk.end)
			}
			where = fmt.Sprintf("%s AND %s", where, limitRange)
		} else {
			where = limitRange
		}

		log.Debugf("%s.%s create dump job: where: %s", dbname, tableName, where)
		jobBucket = append(jobBucket, &DumpJob{
			DbName: dbname,
			Table:  tableName,
			Column: column,
			Where:  where,
			Chunk:  chunk,
		})
	}

	return jobBucket, nil
}
