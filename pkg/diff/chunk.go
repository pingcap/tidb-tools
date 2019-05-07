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
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb-tools/pkg/dbutil"
	"github.com/pingcap/tidb-tools/pkg/utils"
	"go.uber.org/zap"
)

var (
	equal = "="
	lt    = "<"
	lte   = "<="
	gt    = ">"
	gte   = ">="

	bucketMode = "bucketMode"
	normalMode = "normalMode"
)

// Bound represents a bound for a column
type Bound struct {
	Column      string `json:"column"`
	Lower       string `json:"lower"`
	LowerSymbol string `json:"lower-symbol"`
	Upper       string `json:"upper"`
	UpperSymbol string `json:"upper-symbol"`
}

// ChunkRange represents chunk range
type ChunkRange struct {
	ID     int      `json:"id"`
	Bounds []*Bound `json:"bounds"`
	Mode   string   `json:"mode"`

	Where string   `json:"where"`
	Args  []string `json:"args"`

	State string `json:"state"`
}

// NewChunkRange return a ChunkRange.
func NewChunkRange(mode string) *ChunkRange {
	return &ChunkRange{
		Bounds: make([]*Bound, 0, 2),
		Mode:   mode,
	}
}

// String returns the string of ChunkRange, used for log.
func (c *ChunkRange) String() string {
	chunkBytes, err := json.Marshal(c)
	if err != nil {
		log.Warn("fail to encode chunk into string", zap.Error(err))
		return ""
	}

	return string(chunkBytes)
}

func (c *ChunkRange) toString(collation string) (string, []string) {
	if collation != "" {
		collation = fmt.Sprintf(" COLLATE '%s'", collation)
	}

	if c.Mode != bucketMode {
		conditions := make([]string, 0, 2)
		args := make([]string, 0, 2)

		for _, bound := range c.Bounds {
			if len(bound.Lower) != 0 {
				conditions = append(conditions, fmt.Sprintf("`%s`%s %s ?", bound.Column, collation, bound.LowerSymbol))
				args = append(args, bound.Lower)
			}
			if len(bound.Upper) != 0 {
				conditions = append(conditions, fmt.Sprintf("`%s`%s %s ?", bound.Column, collation, bound.UpperSymbol))
				args = append(args, bound.Upper)
			}
		}

		if len(conditions) == 0 {
			return "TRUE", nil
		}

		return strings.Join(conditions, " AND "), args
	}

	/* for example:
	there is a bucket in TiDB, and the lowerbound and upperbound are (v1, v3), (v2, v4), and the columns are `a` and `b`,
	this bucket's data range is (a > v1 or (a == v1 and b >= v2)) and (a < v3 or (a == v3 and a <= v4)),
	not (a >= v1 and a <= v3 and b >= v2 and b <= v4)
	*/

	lowerCondition := make([]string, 0, 1)
	upperCondition := make([]string, 0, 1)
	lowerArgs := make([]string, 0, 1)
	upperArgs := make([]string, 0, 1)

	preConditionForLower := make([]string, 0, 1)
	preConditionForUpper := make([]string, 0, 1)
	preConditionArgsForLower := make([]string, 0, 1)
	preConditionArgsForUpper := make([]string, 0, 1)

	for _, bound := range c.Bounds {
		if len(bound.Lower) != 0 {
			if len(preConditionForLower) > 0 {
				lowerCondition = append(lowerCondition, fmt.Sprintf("(%s AND `%s`%s %s ?)", strings.Join(preConditionForLower, " AND "), bound.Column, collation, bound.LowerSymbol))
				lowerArgs = append(append(lowerArgs, preConditionArgsForLower...), bound.Lower)
			} else {
				lowerCondition = append(lowerCondition, fmt.Sprintf("(`%s`%s %s ?)", bound.Column, collation, bound.LowerSymbol))
				lowerArgs = append(lowerArgs, bound.Lower)
			}
			preConditionForLower = append(preConditionForLower, fmt.Sprintf("`%s` = ?", bound.Column))
			preConditionArgsForLower = append(preConditionArgsForLower, bound.Lower)
		}

		if len(bound.Upper) != 0 {
			if len(preConditionForUpper) > 0 {
				upperCondition = append(upperCondition, fmt.Sprintf("(%s AND `%s`%s %s ?)", strings.Join(preConditionForUpper, " AND "), bound.Column, collation, bound.UpperSymbol))
				upperArgs = append(append(upperArgs, preConditionArgsForUpper...), bound.Upper)
			} else {
				upperCondition = append(upperCondition, fmt.Sprintf("(`%s`%s %s ?)", bound.Column, collation, bound.UpperSymbol))
				upperArgs = append(upperArgs, bound.Upper)
			}
			preConditionForUpper = append(preConditionForUpper, fmt.Sprintf("`%s` = ?", bound.Column))
			preConditionArgsForUpper = append(preConditionArgsForUpper, bound.Upper)
		}
	}

	if len(upperCondition) == 0 && len(lowerCondition) == 0 {
		return "TRUE", nil
	}

	if len(upperCondition) == 0 {
		return strings.Join(lowerCondition, " OR "), lowerArgs
	}

	if len(lowerCondition) == 0 {
		return strings.Join(upperCondition, " OR "), upperArgs
	}

	return fmt.Sprintf("(%s) AND (%s)", strings.Join(lowerCondition, " OR "), strings.Join(upperCondition, " OR ")), append(lowerArgs, upperArgs...)

}

func (c *ChunkRange) update(column, lower, lowerSymbol, upper, upperSymbol string) {
	newBound := &Bound{
		Column:      column,
		Lower:       lower,
		LowerSymbol: lowerSymbol,
		Upper:       upper,
		UpperSymbol: upperSymbol,
	}

	for i, b := range c.Bounds {
		if b.Column == column {
			// update the bound
			c.Bounds[i] = newBound
			return
		}
	}

	// add a new bound
	c.Bounds = append(c.Bounds, newBound)
}

func (c *ChunkRange) copy() *ChunkRange {
	newChunk := &ChunkRange{
		Mode:   c.Mode,
		Bounds: make([]*Bound, len(c.Bounds)),
	}
	copy(newChunk.Bounds, c.Bounds)

	return newChunk
}

func (c *ChunkRange) copyAndUpdate(column, lower, lowerSymbol, upper, upperSymbol string) *ChunkRange {
	newChunk := c.copy()
	newChunk.update(column, lower, lowerSymbol, upper, upperSymbol)
	return newChunk
}

type spliter interface {
	// split splits a table's data to several chunks.
	split(table *TableInstance, columns []*model.ColumnInfo, chunkSize int, limits string, collation string) ([]*ChunkRange, error)
}

type randomSpliter struct {
	table     *TableInstance
	chunkSize int
	limits    string
	collation string
}

func (s *randomSpliter) split(table *TableInstance, columns []*model.ColumnInfo, chunkSize int, limits string, collation string) ([]*ChunkRange, error) {
	s.table = table
	s.chunkSize = chunkSize
	s.limits = limits
	s.collation = collation

	// get the chunk count by data count and chunk size
	cnt, err := dbutil.GetRowCount(context.Background(), table.Conns.DB, table.Schema, table.Table, limits)
	if err != nil {
		return nil, errors.Trace(err)
	}

	chunkCnt := (int(cnt) + chunkSize - 1) / chunkSize
	chunks, err := s.splitRange(table.Conns.DB, NewChunkRange(normalMode), chunkCnt, table.Schema, table.Table, columns)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return chunks, nil
}

// splitRange splits a chunk to multiple chunks.
func (s *randomSpliter) splitRange(db *sql.DB, chunk *ChunkRange, count int, schema string, table string, columns []*model.ColumnInfo) ([]*ChunkRange, error) {
	var chunks []*ChunkRange

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
	colNum := len(chunk.Bounds)
	if colNum != 0 && chunk.Bounds[colNum-1].LowerSymbol != equal {
		splitCol = chunk.Bounds[colNum-1].Column
		min = chunk.Bounds[colNum-1].Lower
		max = chunk.Bounds[colNum-1].Upper
		symbolMin = chunk.Bounds[colNum-1].LowerSymbol
		symbolMax = chunk.Bounds[colNum-1].UpperSymbol
	} else {
		if len(columns) <= colNum {
			log.Warn("chunk can't be splited", zap.Stringer("chunk", chunk))
			return append(chunks, chunk), nil
		}

		// choose the next column to split data
		useNewColumn = true
		splitCol = columns[colNum].Name.O

		min, max, err = dbutil.GetMinMaxValue(context.Background(), db, schema, table, splitCol, limitRange, utils.StringsToInterfaces(args), s.collation)
		if err != nil {
			if errors.Cause(err) == dbutil.ErrNoData {
				log.Info("no data found", zap.String("table", dbutil.TableName(schema, table)), zap.String("range", limitRange), zap.Reflect("args", args))
				return append(chunks, chunk), nil
			}
			return nil, errors.Trace(err)
		}

		symbolMin = gte
		symbolMax = lte
	}

	splitValues := make([]string, 0, count)
	valueCounts := make([]int, 0, count)

	// get random value as split value
	randomValues, randomValueCount, err := dbutil.GetRandomValues(context.Background(), db, schema, table, splitCol, count-1, limitRange, utils.StringsToInterfaces(args), s.collation)
	if err != nil {
		return nil, errors.Trace(err)
	}
	log.Debug("get split values by random values", zap.Stringer("chunk", chunk), zap.Reflect("random values", randomValues))

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

	/*
		for example:
		the splitCol is `a`;
		the splitValues is [1, 2, 3, 4, 5];
		the splitCounts is [1, 3, 1, 1, 1];

		this means you get 3 times value 2 by random, we can assume that there amolst be a lot of rows with value 2,
		so we need use another column `b` to split the chunk [`a` = 2] to 3 chunks.

		and then the splitCol is `b`;
		the splitValues is ['w', 'x', 'y', 'z'];
		the splitValues is [1, 1, 1, 1];
		the chunk [`a` = 2] will split to [`a` = 2 AND `b` < 'x'], [`a` = 2 AND `b` >= 'x' AND `b` < 'y'] and [`a` = 2 AND `b` >= 'y']
	*/

	lowerSymbol := symbolMin
	upperSymbol := lt

	for i := 0; i < len(splitValues); i++ {
		if i == 0 && useNewColumn {
			// create chunk less than min
			newChunk := chunk.copyAndUpdate(splitCol, "", "", splitValues[i], lt)
			chunks = append(chunks, newChunk)
		}

		if valueCounts[i] > 1 {
			// means should split it
			newChunk := chunk.copyAndUpdate(splitCol, splitValues[i], equal, "", "")
			splitChunks, err := s.splitRange(db, newChunk, valueCounts[i], schema, table, columns)
			if err != nil {
				return nil, errors.Trace(err)
			}
			chunks = append(chunks, splitChunks...)

			// already have the chunk [column = value], so next chunk should start with column > value
			lowerSymbol = gt
		}

		if i == len(splitValues)-2 && valueCounts[i+1] == 1 {
			upperSymbol = symbolMax
		}

		if i < len(splitValues)-1 {
			newChunk := chunk.copyAndUpdate(splitCol, splitValues[i], lowerSymbol, splitValues[i+1], upperSymbol)
			chunks = append(chunks, newChunk)
		}

		if i == len(splitValues)-1 && useNewColumn {
			// create chunk greater than max
			newChunk := chunk.copyAndUpdate(splitCol, splitValues[i], gt, "", "")
			chunks = append(chunks, newChunk)
		}

		lowerSymbol = gte
	}

	log.Debug("getChunksForTable cut table", zap.Int("count", count), zap.String("min", min), zap.String("max", max), zap.Int("chunk num", len(chunks)))
	return chunks, nil
}

type bucketSpliter struct {
	table     *TableInstance
	chunkSize int
	limits    string
	collation string
	buckets   map[string][]dbutil.Bucket
}

func (s *bucketSpliter) split(table *TableInstance, columns []*model.ColumnInfo, chunkSize int, limits string, collation string) ([]*ChunkRange, error) {
	s.table = table
	s.chunkSize = chunkSize
	s.limits = limits
	s.collation = collation

	buckets, err := dbutil.GetBucketsInfo(context.Background(), s.table.Conns.DB, s.table.Schema, s.table.Table, s.table.info)
	if err != nil {
		return nil, errors.Trace(err)
	}
	s.buckets = buckets

	return s.getChunksByBuckets()
}

func (s *bucketSpliter) getChunksByBuckets() ([]*ChunkRange, error) {
	chunks := make([]*ChunkRange, 0, 1000)

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
			lowerValues []string
			upperValues []string
			latestCount int64
			err         error
		)

		indexColumns := getColumnsFromIndex(index, s.table.info)

		for i, bucket := range buckets {
			upperValues, err = dbutil.AnalyzeValuesFromBuckets(bucket.UpperBound, indexColumns)
			if err != nil {
				return nil, errors.Trace(err)
			}

			if bucket.Count-latestCount > int64(s.chunkSize) || i == len(buckets)-1 {
				// create a new chunk
				chunk := NewChunkRange(bucketMode)
				var lower, upper, lowerSymbol, upperSymbol string
				for j, col := range index.Columns {
					if len(lowerValues) != 0 {
						lower = lowerValues[j]
						lowerSymbol = gt
					}
					if i != len(buckets)-1 {
						upper = upperValues[j]
						upperSymbol = lte
					}

					chunk.update(col.Name.O, lower, lowerSymbol, upper, upperSymbol)
				}

				chunks = append(chunks, chunk)
				lowerValues = upperValues
				latestCount = bucket.Count
			}
		}

		if len(chunks) != 0 {
			break
		}
	}

	return chunks, nil
}

func getChunksForTable(table *TableInstance, columns []*model.ColumnInfo, chunkSize int, limits string, collation string, useTiDBStatsInfo bool) ([]*ChunkRange, error) {
	if useTiDBStatsInfo {
		s := bucketSpliter{}
		chunks, err := s.split(table, columns, chunkSize, limits, collation)
		if err == nil && len(chunks) > 0 {
			return chunks, nil
		}

		log.Warn("use tidb bucket information to get chunks failed, will split chunk by random again", zap.Int("get chunk", len(chunks)), zap.Error(err))
	}

	// get chunks from tidb bucket information failed, use random.
	s := randomSpliter{}
	chunks, err := s.split(table, columns, chunkSize, limits, collation)
	return chunks, err
}

// getSplitFields returns fields to split chunks, order by pk, uk, index, columns.
func getSplitFields(table *model.TableInfo, splitFields []string) ([]*model.ColumnInfo, error) {
	cols := make([]*model.ColumnInfo, 0, len(table.Columns))
	colsMap := make(map[string]interface{})

	splitCols := make([]*model.ColumnInfo, 0, 2)
	for _, splitField := range splitFields {
		col := dbutil.FindColumnByName(table.Columns, splitField)
		if col == nil {
			return nil, errors.NotFoundf("column %s in table %s", splitField, table.Name)

		}
		splitCols = append(splitCols, col)
	}

	indexColumns := dbutil.FindAllColumnWithIndex(table)

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

// SplitChunks splits the table to some chunks.
func SplitChunks(ctx context.Context, table *TableInstance, splitFields, limits string, chunkSize int, collation string, useTiDBStatsInfo bool) (chunks []*ChunkRange, err error) {
	var splitFieldArr []string
	if len(splitFields) != 0 {
		splitFieldArr = strings.Split(splitFields, ",")
	}

	for i := range splitFieldArr {
		splitFieldArr[i] = strings.TrimSpace(splitFieldArr[i])
	}

	fields, err := getSplitFields(table.info, splitFieldArr)
	if err != nil {
		return nil, errors.Trace(err)
	}

	chunks, err = getChunksForTable(table, fields, chunkSize, limits, collation, useTiDBStatsInfo)
	if err != nil {
		return nil, errors.Trace(err)
	}

	if chunks == nil {
		return nil, nil
	}

	ctx1, cancel1 := context.WithTimeout(ctx, time.Duration(len(chunks))*dbutil.DefaultTimeout)
	defer cancel1()
	for i, chunk := range chunks {
		conditions, args := chunk.toString(collation)

		chunk.ID = i
		chunk.Where = fmt.Sprintf("(%s AND %s)", conditions, limits)
		chunk.Args = args
		chunk.State = notCheckedState

		err = saveChunk(ctx1, table.Conns.CpDB, i, table.InstanceID, table.Schema, table.Table, "", chunk)
		if err != nil {
			return nil, err
		}
	}

	return chunks, nil
}
