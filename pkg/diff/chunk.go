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
	return *chunkRange{
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
			conditions = append(conditions, fmt.Sprintf("`%s`%s %s ?"), col, collation, c.symbols[i][j])
			args = append(args, bound)
		}
	}

	return strings.Join(condition, " AND "), args
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
			symbols = append(symbols, symbnol)
		}
		newChunk.symbols = append(newChunk.symbols, symbols)
	}

	return newChunk
}

type Spliter interface {
	// Split splits a table's data to several chunks.
	Split(table *TableInstance, chunkSize int, limits string, collation string) ([]chunkRange, error)
}

type RandomSpliter struct {
	table     *TableInstance
	chunkSize int
	limits    string
	collation string
	sample    int
}

func (s *RandomSpliter) Split(table *TableInstance, column *model.ColumnInfo, chunkSize, sample int, limits string, collation string) {
	s.table = table
	s.chunkSzie = chunkSize
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

	chunkCnt := (cnt + int64(chunkSize) - 1) / int64(chunkSize)
	if sample != 100 {
		// use sampling check, can check more fragmented by split to more chunk
		chunkCnt *= 10
	}

	field := column.Name.O

	collationStr := ""
	if collation != "" {
		collationStr = fmt.Sprintf(" COLLATE \"%s\"", collation)
	}

	// fetch min, max
	query := fmt.Sprintf("SELECT /*!40001 SQL_NO_CACHE */ MIN(`%s`%s) as MIN, MAX(`%s`%s) as MAX FROM `%s`.`%s` WHERE %s",
		field, collationStr, field, collationStr, table.Schema, table.Table, limits)

	var min, max sql.NullString
	err := table.Conn.QueryRow(query).Scan(&min, &max)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if !min.Valid || !max.Valid {
		return nil, nil
	}
	chunk := newChunkRange()
	chunk.columns = append(chunk.columns, field)
	if min.String == max.String {
		chunk.bounds = append(chunk.bounds, []string{min.String})
		chunk.symbols = append(chunk.symbols, []string(equal))
	} else {
		chunk.bounds = append(chunk.bounds, []string{min.String, max.String})
		chunk.symbols = append(chunk.symbols, []string(gte, lte))
	}

	chunks, err := splitRange(table.Conn, &chunk, chunkCnt, table.Schema, table.Table, column)
	if err != nil {
		return nil, errors.Trace(err)
	}

	// for example, the min and max value in target table is 2-9, but 1-10 in source table. so we need generate chunk for data < 2 and data > 9
	maxChunk := c.update(field, []string{max}, []string{gt})
	minChunk := c.update(field, []string{min}, []string{lt})
	chunks = append(chunks, maxChunk)
	chunks = append(chunks, minChunk)

	return chunks, nil
}

func (s *RandomSpliter) splitRange(db *sql.DB, chunk *chunkRange, count int64, Schema string, table string) ([]chunkRange, error) {
	var chunks []chunkRange

	if count <= 1 {
		chunks = append(chunks, *chunk)
		return chunks, nil
	}

	var splitCol, min, max string
	for i, col := range chunk.columns {
		if chunk.symbols[i][0] == equal {
			continue
		} else {
			splitCol = col
			min = chunk.bounds[0]
			max = chunk.bounds[1]
		}
	}

	if splitCol == "" {
		// choose another column to split data
	}

	chunkLimits, args := chunk.toString()
	limitRange := fmt.Sprintf("%s AND %s", chunkLimits, s.limits)

	// get random value as split value
	splitValues, valueCount, err := dbutil.GetRandomValues(context.Background(), db, Schema, table, splitCol, count-1, limitRange, collation, args)
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
		newChunk := chunk.update(splitCol, []string{minTmp, maxTmp}, []string{gt, lt})
		chunks = append(chunks, newChunk)

		// valueCount > 1 means should split it
		if valueCount[i] > 1 {
			newChunk := chunk.update(splitCol, []string{splitValues[i], []string{eq}})
			splitChunks, err := s.splitRange(db, newChunk, valueCount[i], Schema, table)
		}

		r := newChunkRange(chunk.columns[0], minTmp, maxTmp, true, false, false, false)
		chunks = append(chunks, r)
	}

	log.Debugf("getChunksForTable cut table: cnt=%d min=%s max=%s chunk=%d", count, min, max, len(chunks))

	chunks[len(chunks)-1].end = chunk.end
	chunks[0].containBegin = chunk.containBegin
	chunks[len(chunks)-1].containEnd = chunk.containEnd

	return chunks, nil
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

	collationStr := ""
	if collation != "" {
		collationStr = fmt.Sprintf(" COLLATE \"%s\"", collation)
	}

	// fetch min, max
	query := fmt.Sprintf("SELECT /*!40001 SQL_NO_CACHE */ MIN(`%s`%s) as MIN, MAX(`%s`%s) as MAX FROM `%s`.`%s` WHERE %s",
		field, collationStr, field, collationStr, table.Schema, table.Table, limits)

	var min, max sql.NullString
	err := table.Conn.QueryRow(query).Scan(&min, &max)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if !min.Valid || !max.Valid {
		return nil, nil
	}
	chunk := newChunkRange()
	chunk.columns = append(chunk.columns, field)
	chunk.bounds = append(chunk.bounds, []string{min.String, max.String})
	chunk.symbols = append(chunk.symbols, []string(gte, lte))
	chunk = newChunkRange(field, min.String, max.String, true, true, false, false)

	return splitRange(table.Conn, &chunk, chunkCnt, table.Schema, table.Table, column, limits, collation)
}

func getChunksByBucketsInfo(db *sql.DB, schema string, table string, tableInfo *model.TableInfo, chunkSize int) ([]chunkRange, error) {
	buckets, err := dbutil.GetBucketsInfo(context.Background(), db, schema, table)
	if err != nil {
		return nil, errors.Trace(err)
	}
	log.Infof("buckets: %v", buckets)

	var indexName string
	var columns []string

	for _, index := range tableInfo.Indices {
		if index.Primary || index.Unique {
			indexName = index.Name.O
		} else {
			// TODO: choose a situable index if without pk/uk.
			if indexName == "" {
				indexName = index.Name.O
			} else {
				continue
			}
		}

		columns := make([]string, 0, len(index.Columns))
		for _, column := range index.Columns {
			columns = append(columns, column.Name.O)
		}
		if index.Primary {
			break
		}
	}

	if indexName == "" {
		return nil, nil
	}
	log.Infof("columns: %v", columns)

	return nil, nil
}

/*
func bucketsToChunks(buckets []dbutil.Bucket, columns []string, count, chunkSize int) []chunkRange {
	chunks := make([]chunkRange, 0, count/chunkSize)
	var lower, upper string
	var num int

	// add chunk for data < min and data >= max
	chunks = append(chunks, newChunkRange(columns, nil, upper, true, false, false, false))
	chunks = append(chunks, newChunkRange(columns, lower, upper, true, false, false, false))

	for _, bucket := range buckets {
		if lower == "" {
			lower = bucket.LowerBound
		}
		upper = bucket.UpperBound
		num += bucket.Count
		if count - num > chunkSize {
			chunks = append(chunks, newChunkRange(columns, lower, upper, true, false, false, false))
			lower = upper
		}
	}
}
*/

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

		conditions, args := chunk.toString(collation)
		where := fmt.Sprintf("(%s AND %s)", conditions, limits)

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
