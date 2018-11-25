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
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb-tools/pkg/dbutil"
	log "github.com/sirupsen/logrus"
)

var (
	equal string = "="
	lt    string = "<"
	lte   string = "<="
	gt    string = ">"
	gte   string = ">="
)

// chunkRange represents chunk range
type chunkRange struct {
	columns []string
	bounds  [][]string
	symbols [][]string
}

// newChunkRange return a range struct
func newChunkRange() *chunkRange {
	return &chunkRange{
		columns: make([]string, 0, 2),
		bounds:  make([][]string, 0, 2),
		symbols: make([][]string, 0, 2),
	}
}

func (c *chunkRange) toString(collation string) (string, []interface{}) {
	conditions := make([]string, 0, 2)
	args := make([]interface{}, 0, 2)

	for i, col := range c.columns {
		for j, bound := range c.bounds[i] {
			conditions = append(conditions, fmt.Sprintf("`%s`%s %s ?", col, collation, c.symbols[i][j]))
			args = append(args, bound)
		}
	}

	return strings.Join(conditions, " AND "), args
}

func (c *chunkRange) update(col string, bounds []string, symbols []string) *chunkRange {
	newChunk := c.copy()
	for i, column := range c.columns {
		if column == col {
			// update the column's range
			newChunk.bounds[i] = bounds
			newChunk.symbols[i] = symbols
			return newChunk
		}
	}

	// add a new column
	newChunk.columns = append(newChunk.columns, col)
	newChunk.bounds = append(newChunk.bounds, bounds)
	newChunk.symbols = append(newChunk.symbols, symbols)

	return newChunk
}

func (c *chunkRange) copy() *chunkRange {
	newChunk := newChunkRange()
	for i, column := range c.columns {
		newChunk.columns = append(newChunk.columns, column)
		bounds := make([]string, 0, 2)
		for _, bound := range c.bounds[i] {
			bounds = append(bounds, bound)
		}
		newChunk.bounds = append(newChunk.bounds, bounds)

		symbols := make([]string, 0, 2)
		for _, symbol := range c.symbols[i] {
			symbols = append(symbols, symbol)
		}
		newChunk.symbols = append(newChunk.symbols, symbols)
	}

	return newChunk
}

type spliter interface {
	// split splits a table's data to several chunks.
	split(table *TableInstance, columns []*model.ColumnInfo, chunkSize, sample int, limits string, collation string) ([]*chunkRange, error)
}

type randomSpliter struct {
	table     *TableInstance
	chunkSize int
	limits    string
	collation string
	sample    int
}

func (s *randomSpliter) split(table *TableInstance, columns []*model.ColumnInfo, chunkSize, sample int, limits string, collation string) ([]*chunkRange, error) {
	s.table = table
	s.chunkSize = chunkSize
	s.limits = limits
	s.collation = collation
	s.sample = sample

	// get the chunk count
	cnt, err := dbutil.GetRowCount(context.Background(), table.Conn, table.Schema, table.Table, limits)
	if err != nil {
		return nil, errors.Trace(err)
	}

	if cnt == 0 {
		log.Infof("no data found in %s.%s", table.Schema, table.Table)
		return nil, nil
	}

	chunkCnt := (int(cnt) + chunkSize - 1) / chunkSize
	if sample != 100 {
		// use sampling check, can check more fragmented by split to more chunk
		chunkCnt *= 10
	}

	field := columns[0].Name.O

	// fetch min, max
	min, max, err := dbutil.GetMinMaxValue(context.Background(), table.Conn, table.Schema, table.Table, field, limits, collation, nil)
	if err != nil {
		if errors.Cause(err) == dbutil.ErrNoData {
			log.Infof("no data found in %s.%s", table.Schema, table.Table)
			return nil, nil
		}
		return nil, errors.Trace(err)
	}

	chunk := newChunkRange()
	chunk.columns = append(chunk.columns, field)
	if min == max {
		chunk.bounds = append(chunk.bounds, []string{min})
		chunk.symbols = append(chunk.symbols, []string{equal})
	} else {
		chunk.bounds = append(chunk.bounds, []string{min, max})
		chunk.symbols = append(chunk.symbols, []string{gte, lte})
	}

	chunks, err := s.splitRange(table.Conn, chunk, chunkCnt, table.Schema, table.Table, columns)
	if err != nil {
		return nil, errors.Trace(err)
	}

	// for example, the min and max value in target table is 2-9, but 1-10 in source table. so we need generate chunk for data < 2 and data > 9
	maxChunk := chunk.update(field, []string{max}, []string{gt})
	minChunk := chunk.update(field, []string{min}, []string{lt})
	chunks = append(chunks, maxChunk)
	chunks = append(chunks, minChunk)

	return chunks, nil
}

// splitRange splits a chunk to multiple chunks. Notice: can only split chunks have max and min value or equal to a value, otherwise will panic.
func (s *randomSpliter) splitRange(db *sql.DB, chunk *chunkRange, count int, schema string, table string, columns []*model.ColumnInfo) ([]*chunkRange, error) {
	var chunks []*chunkRange

	if count <= 1 {
		chunks = append(chunks, chunk)
		return chunks, nil
	}

	var (
		splitCol, min, max, symbolMin, symbolMax string
		err                                      error
		useNewColumn                             bool
	)

	chunkLimits, args := chunk.toString(s.collation)
	limitRange := fmt.Sprintf("%s AND %s", chunkLimits, s.limits)

	// if the last column's condition is not '=', continue use this column split data.
	colNum := len(chunk.columns)
	if chunk.symbols[colNum-1][0] != equal {
		splitCol = chunk.columns[colNum-1]
		min = chunk.bounds[colNum-1][0]
		max = chunk.bounds[colNum-1][1]
		symbolMin = chunk.symbols[colNum-1][0]
		symbolMax = chunk.symbols[colNum-1][1]
	} else {
		// choose the next column to split data
		if len(columns) > colNum {
			useNewColumn = true
			splitCol = columns[colNum].Name.O

			min, max, err = dbutil.GetMinMaxValue(context.Background(), db, schema, table, splitCol, limitRange, s.collation, args)
			if err != nil {
				if errors.Cause(err) == dbutil.ErrNoData {
					log.Infof("no data found in %s.%s", schema, table)
					return append(chunks, chunk), nil
				}
				return nil, errors.Trace(err)
			}

			symbolMin = gte
			symbolMax = lte
		} else {
			log.Warnf("chunk %v can't be splited", chunk)
			return append(chunks, chunk), nil
		}
	}

	splitValues := make([]string, 0, count)
	valueCounts := make([]int, 0, count)

	// get random value as split value
	randomValues, randomValueCount, err := dbutil.GetRandomValues(context.Background(), db, schema, table, splitCol, count-1, limitRange, s.collation, args)
	if err != nil {
		return nil, errors.Trace(err)
	}
	log.Infof("split chunk %v, get split values from GetRandomValues: %v", chunk, splitValues)

	/*
		for examples:
		the result of GetRandomValues is:
		mysql> SELECT `id`, count(*) count FROM (SELECT `id` FROM `test`.`test` ORDER BY RAND() LIMIT 100) rand_tmp GROUP BY `id` ORDER BY `id`;
		+------+-------+
		| id   | count |
		+------+-------+
		|    1 |     1 |
		|    2 |     1 |
		|    3 |    96 |
		|    4 |     1 |
		|    5 |     1 |
		+------+-------+

		We can assume that the 96% of this table's data is in range [id = 3], so we should use another column to split range `id = 3`,
		just like [id = 3 AND cid > 10], [id = 3 AND cid >= 5 AND cid <= 10], [id = 3 AND cid < 5]...
	*/

	if len(randomValues) > 0 && randomValues[0] == min {
		splitValues = append(splitValues, randomValues...)
		valueCounts = append(valueCounts, randomValueCount...)
		valueCounts[0]++
	} else {
		splitValues = append(append(splitValues, min), randomValues...)
		valueCounts = append(append(valueCounts, 1), randomValueCount...)
	}

	if len(randomValues) > 0 && randomValues[len(randomValues)-1] == max {
		valueCounts[len(valueCounts)-1]++
	} else {
		splitValues = append(splitValues, max)
		valueCounts = append(valueCounts, 1)
	}

	var symbols []string
	for i := 0; i < len(splitValues); i++ {
		if valueCounts[i] > 1 {
			// means should split it
			newChunk := chunk.update(splitCol, []string{splitValues[i]}, []string{equal})
			splitChunks, err := s.splitRange(db, newChunk, valueCounts[i], schema, table, columns)
			if err != nil {
				return nil, errors.Trace(err)
			}
			chunks = append(chunks, splitChunks...)

			symbols = []string{gt, lt}
		} else {
			if i == 0 {
				symbols = []string{symbolMin, lt}
			} else {
				symbols = []string{gte, lt}
			}
		}

		if i == len(splitValues)-2 && valueCounts[len(valueCounts)-1] == 1 {
			symbols[1] = symbolMax
		}

		if i < len(splitValues)-1 {
			newChunk := chunk.update(splitCol, []string{splitValues[i], splitValues[i+1]}, symbols)
			chunks = append(chunks, newChunk)
		}
	}

	if useNewColumn {
		// add chunk > max and < min
		maxChunk := chunk.update(splitCol, []string{max}, []string{gt})
		minChunk := chunk.update(splitCol, []string{min}, []string{lt})
		chunks = append(chunks, maxChunk)
		chunks = append(chunks, minChunk)
	}

	log.Debugf("getChunksForTable cut table: cnt=%d min=%s max=%s chunk=%d", count, min, max, len(chunks))

	return chunks, nil
}

type bucketSpliter struct {
	table     *TableInstance
	chunkSize int
	limits    string
	collation string
	sample    int
	buckets   map[string][]dbutil.Bucket
}

func (s *bucketSpliter) split(table *TableInstance, columns []*model.ColumnInfo, chunkSize, sample int, limits string, collation string) ([]*chunkRange, error) {
	s.table = table
	s.chunkSize = chunkSize
	s.limits = limits
	s.collation = collation
	s.sample = sample

	buckets, err := dbutil.GetBucketsInfo(context.Background(), s.table.Conn, s.table.Schema, s.table.Table)
	if err != nil {
		return nil, errors.Trace(err)
	}
	s.buckets = buckets

	return s.getChunksByBuckets()
}

func (s *bucketSpliter) getChunksByBuckets() ([]*chunkRange, error) {
	chunks := make([]*chunkRange, 0, 1000)

	indices := dbutil.FindAllIndex(s.table.info)
	for _, index := range indices {
		if index == nil {
			continue
		}
		buckets, ok := s.buckets[index.Name.O]
		if !ok {
			return nil, errors.NotFoundf("index %s in buckets info", index.Name.O)
		}

		var (
			chunkNum    int64
			lowerValues []string
			upperValues []string
		)

		for i, bucket := range buckets {
			upperValues = s.getValues(bucket.UpperBound)
			if int(bucket.Count) > (int(chunkNum)+1)*s.chunkSize || i == len(buckets)-1 {
				// create a new chunk
				chunk := newChunkRange()
				for j, col := range index.Columns {
					values := make([]string, 0, 2)
					symbols := make([]string, 0, 2)
					if len(lowerValues) != 0 {
						values = append(values, lowerValues[j])
						symbols = append(symbols, gt)
					}
					if i != len(buckets)-1 {
						values = append(values, upperValues[j])
						symbols = append(symbols, lte)
					}
					chunk = chunk.update(col.Name.O, values, symbols)
				}
				chunks = append(chunks, chunk)
				lowerValues = upperValues
				chunkNum++
			}
		}
	}

	return chunks, nil
}

// upperBound and lowerBound are looks like '(123, abc)' for multiple fields, or '123' for one field.
func (s *bucketSpliter) getValues(valueString string) []string {
	vStr := strings.Trim(strings.Trim(valueString, "("), ")")
	return strings.Split(vStr, ",")
}

// CheckJob is the struct of job for check
type CheckJob struct {
	Schema string
	Table  string
	Where  string
	Args   []interface{}
}

func getChunksForTable(table *TableInstance, columns []*model.ColumnInfo, chunkSize, sample int, limits string, collation string) ([]*chunkRange, error) {
	s := bucketSpliter{}
	chunks, err := s.split(table, columns, chunkSize, sample, limits, collation)
	if err != nil || len(chunks) == 0 {
		log.Warnf("use bucket information to get chunks error: %v, chunks num: %d", errors.Trace(err), len(chunks))
		s := randomSpliter{}
		return s.split(table, columns, chunkSize, sample, limits, collation)
	}

	return chunks, nil
}

// getSplitFields returns fields to split chunks, order by pk, uk, index, columns.
func getSplitFields(db *sql.DB, schema string, table *model.TableInfo, splitFields []string) ([]*model.ColumnInfo, error) {
	cols := make([]*model.ColumnInfo, 0, len(table.Columns))
	colsMap := make(map[string]interface{})

	splitCols := make([]*model.ColumnInfo, 0, 2)
	for _, splitField := range splitFields {
		col := dbutil.FindColumnByName(table.Columns, splitField)
		if col == nil {
			return nil, errors.NotFoundf("column %s in table %s", splitField, table.Name)

		}
		splitCols = append(splitCols, dbutil.FindColumnByName(table.Columns, splitField))
	}

	indexColumns := dbutil.FindAllColumnWithIndex(context.Background(), db, schema, table)

	// user's config had higher priorities
	for _, col := range append(append(splitCols, indexColumns...), table.Columns...) {
		if _, ok := colsMap[col.Name.O]; ok {
			continue
		}

		colsMap[col.Name.O] = struct{}{}
		cols = append(cols, col)
	}

	return cols, nil
}

// GenerateCheckJob generates some CheckJobs.
func GenerateCheckJob(table *TableInstance, splitFields, limits string, chunkSize, sample int, collation string) ([]*CheckJob, error) {
	jobBucket := make([]*CheckJob, 0, 10)
	var jobCnt int
	var err error

	var splitFieldArr []string
	if len(splitFields) != 0 {
		splitFieldArr = strings.Split(splitFields, ",")
	}

	fields, err := getSplitFields(table.Conn, table.Schema, table.info, splitFieldArr)
	if err != nil {
		return nil, errors.Trace(err)
	}

	chunks, err := getChunksForTable(table, fields, chunkSize, sample, limits, collation)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if chunks == nil {
		return nil, nil
	}

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

		conditions, args := chunk.toString(collation)
		where := fmt.Sprintf("(%s AND %s)", conditions, limits)

		log.Debugf("%s.%s create check job, where: %s, args: %v", table.Schema, table.Table, where, args)
		jobBucket = append(jobBucket, &CheckJob{
			Schema: table.Schema,
			Table:  table.Table,
			Where:  where,
			Args:   args,
		})
	}

	return jobBucket, nil
}
