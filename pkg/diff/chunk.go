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

package diff

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"

	"github.com/juju/errors"
	"github.com/pingcap/tidb-tools/pkg/dbutil"
	"github.com/pingcap/tidb/model"
	log "github.com/sirupsen/logrus"
)

// chunkRange represents chunk range
type chunkRange struct {
	begin interface{}
	end   interface{}
	// for example:
	// containBegin and containEnd is true, means [begin, end]
	// containBegin is true, containEnd is false, means [begin, end)
	containBegin bool
	containEnd   bool

	// if noBegin is true, means there is no lower limit
	// if noEnd is true, means there is no upper limit
	noBegin bool
	noEnd   bool
}

// CheckJob is the struct of job for check
type CheckJob struct {
	Schema string
	Table  string
	Column *model.ColumnInfo
	Where  string
	Args   []interface{}
	Chunk  chunkRange
}

// newChunkRange return a range struct
func newChunkRange(begin, end interface{}, containBegin, containEnd, noBegin, noEnd bool) chunkRange {
	return chunkRange{
		begin:        begin,
		end:          end,
		containEnd:   containEnd,
		containBegin: containBegin,
		noBegin:      noBegin,
		noEnd:        noEnd,
	}
}
func getChunksForTable(table *TableInstance, column *model.ColumnInfo, chunkSize, sample int, limits string, collation string) ([]chunkRange, error) {
	if column == nil {
		log.Warnf("no suitable index found for %s.%s", table.Schema, table.Table)
		return nil, nil
	}

	// get the chunk count
	cnt, err := dbutil.GetRowCount(context.Background(), table.Conn, table.Schema, table.Table, limits)
	if err != nil {
		return nil, errors.Trace(err)
	}

	if cnt == 0 {
		log.Infof("no data found in %s.%s", table.Schema, table.Table)
		return nil, nil
	}

	chunkCnt := (cnt + int64(chunkSize) - 1) / int64(chunkSize)
	if sample != 100 {
		// use sampling check, can check more fragmented by split to more chunk
		chunkCnt *= 10
	}

	field := column.Name.O

	if collation != "" {
		collation = fmt.Sprintf(" COLLATE \"%s\"", collation)
	}

	// fetch min, max
	query := fmt.Sprintf("SELECT /*!40001 SQL_NO_CACHE */ MIN(`%s`%s) as MIN, MAX(`%s`%s) as MAX FROM `%s`.`%s` WHERE %s",
		field, collation, field, collation, table.Schema, table.Table, limits)

	var chunk chunkRange
	if dbutil.IsNumberType(column.Tp) {
		var min, max sql.NullInt64
		err := table.Conn.QueryRow(query).Scan(&min, &max)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if !min.Valid {
			// min is NULL, means that no table data.
			return nil, nil
		}
		chunk = newChunkRange(min.Int64, max.Int64, true, true, false, false)
	} else if dbutil.IsFloatType(column.Tp) {
		var min, max sql.NullFloat64
		err := table.Conn.QueryRow(query).Scan(&min, &max)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if !min.Valid {
			// min is NULL, means that no table data.
			return nil, nil
		}
		chunk = newChunkRange(min.Float64, max.Float64, true, true, false, false)
	} else {
		var min, max sql.NullString
		err := table.Conn.QueryRow(query).Scan(&min, &max)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if !min.Valid || !max.Valid {
			return nil, nil
		}
		chunk = newChunkRange(min.String, max.String, true, true, false, false)
	}

	return splitRange(table.Conn, &chunk, chunkCnt, table.Schema, table.Table, column, limits, collation)
}

func splitRange(db *sql.DB, chunk *chunkRange, count int64, Schema string, table string, column *model.ColumnInfo, limitRange string, collation string) ([]chunkRange, error) {
	var chunks []chunkRange

	// for example, the min and max value in target table is 2-9, but 1-10 in source table. so we need generate chunk for data < 2 and data > 9
	addOutRangeChunk := func() {
		chunks = append(chunks, newChunkRange(struct{}{}, chunk.begin, false, false, true, false))
		chunks = append(chunks, newChunkRange(chunk.end, struct{}{}, false, false, false, true))
	}

	if count <= 1 {
		chunks = append(chunks, *chunk)
		addOutRangeChunk()
		return chunks, nil
	}

	if reflect.TypeOf(chunk.begin).Kind() == reflect.Int64 {
		min, ok1 := chunk.begin.(int64)
		max, ok2 := chunk.end.(int64)
		if !ok1 || !ok2 {
			return nil, errors.Errorf("can't parse chunk's begin: %v, end: %v", chunk.begin, chunk.end)
		}
		step := (max - min + count - 1) / count
		cutoff := min
		for cutoff <= max {
			r := newChunkRange(cutoff, cutoff+step, true, false, false, false)
			chunks = append(chunks, r)
			cutoff += step
		}

		log.Debugf("getChunksForTable cut table: cnt=%d min=%v max=%v step=%v chunk=%d", count, min, max, step, len(chunks))
	} else if reflect.TypeOf(chunk.begin).Kind() == reflect.Float64 {
		min, ok1 := chunk.begin.(float64)
		max, ok2 := chunk.end.(float64)
		if !ok1 || !ok2 {
			return nil, errors.Errorf("can't parse chunk's begin: %v, end: %v", chunk.begin, chunk.end)
		}
		step := (max - min + float64(count-1)) / float64(count)
		cutoff := min
		for cutoff <= max {
			r := newChunkRange(cutoff, cutoff+step, true, false, false, false)
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
		splitValues, err := dbutil.GetRandomValues(context.Background(), db, Schema, table, column.Name.O, count-1, min, max, limitRange, collation)
		if err != nil {
			return nil, errors.Trace(err)
		}

		var minTmp, maxTmp string
		var i int64
		for i = 0; i < int64(len(splitValues)+1); i++ {
			if i == 0 {
				minTmp = min
			} else {
				minTmp = fmt.Sprintf("%s", splitValues[i-1])
			}
			if i == int64(len(splitValues)) {
				maxTmp = max
			} else {
				maxTmp = fmt.Sprintf("%s", splitValues[i])
			}
			r := newChunkRange(minTmp, maxTmp, true, false, false, false)
			chunks = append(chunks, r)
		}

		log.Debugf("getChunksForTable cut table: cnt=%d min=%s max=%s chunk=%d", count, min, max, len(chunks))
	}

	chunks[len(chunks)-1].end = chunk.end
	chunks[0].containBegin = chunk.containBegin
	chunks[len(chunks)-1].containEnd = chunk.containEnd

	addOutRangeChunk()

	return chunks, nil
}

func findSuitableField(db *sql.DB, Schema string, table *model.TableInfo) (*model.ColumnInfo, error) {
	// first select the index, and number type index first
	column, err := dbutil.FindSuitableIndex(context.Background(), db, Schema, table)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if column != nil {
		return column, nil
	}

	// use the first column
	log.Infof("%s.%s don't have index, will use the first column as split field", Schema, table.Name.O)
	return table.Columns[0], nil
}

// GenerateCheckJob generates some CheckJobs.
func GenerateCheckJob(table *TableInstance, splitField, limits string, chunkSize, sample int, collation string) ([]*CheckJob, error) {
	jobBucket := make([]*CheckJob, 0, 10)
	var jobCnt int
	var column *model.ColumnInfo
	var err error

	if splitField == "" {
		column, err = findSuitableField(table.Conn, table.Schema, table.info)
		if err != nil {
			return nil, errors.Trace(err)
		}
	} else {
		column = dbutil.FindColumnByName(table.info.Columns, splitField)
		if column == nil {
			return nil, errors.NotFoundf("column %s in table %s", splitField, table.Table)
		}
	}

	chunks, err := getChunksForTable(table, column, chunkSize, sample, limits, collation)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if chunks == nil {
		return nil, nil
	}
	log.Debugf("chunks: %+v", chunks)

	jobCnt += len(chunks)

	if collation != "" {
		collation = fmt.Sprintf(" COLLATE \"%s\"", collation)
	}

	for {
		length := len(chunks)
		if length == 0 {
			break
		}

		chunk := chunks[0]
		chunks = chunks[1:]

		args := make([]interface{}, 0, 2)
		var condition1, condition2 string
		if !chunk.noBegin {
			if chunk.containBegin {
				condition1 = fmt.Sprintf("`%s`%s >= ?", column.Name, collation)
			} else {
				condition1 = fmt.Sprintf("`%s`%s > ?", column.Name, collation)
			}
			args = append(args, chunk.begin)
		} else {
			condition1 = "TRUE"
		}
		if !chunk.noEnd {
			if chunk.containEnd {
				condition2 = fmt.Sprintf("`%s`%s <= ?", column.Name, collation)
			} else {
				condition2 = fmt.Sprintf("`%s`%s < ?", column.Name, collation)
			}
			args = append(args, chunk.end)
		} else {
			condition2 = "TRUE"
		}
		where := fmt.Sprintf("(%s AND %s AND %s)", condition1, condition2, limits)

		log.Debugf("%s.%s create dump job, where: %s, begin: %v, end: %v", table.Schema, table.Table, where, chunk.begin, chunk.end)
		jobBucket = append(jobBucket, &CheckJob{
			Schema: table.Schema,
			Table:  table.Table,
			Column: column,
			Where:  where,
			Args:   args,
			Chunk:  chunk,
		})
	}

	return jobBucket, nil
}
