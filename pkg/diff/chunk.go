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
	"fmt"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb-tools/pkg/dbutil"
	log "github.com/sirupsen/logrus"
)

var (
	equal = "="
	lt    = "<"
	lte   = "<="
	gt    = ">"
	gte   = ">="

	bucketMode = "bucket"
	normalMode = "normalMode"
)

type bound struct {
	column      string
	lower       string
	lowerSymbol string
	upper       string
	upperSymbol string
}

// chunkRange represents chunk range
type chunkRange struct {
	bounds []*bound
}

// newChunkRange return a chunkRange.
func newChunkRange() *chunkRange {
	return &chunkRange{
		bounds: make([]*bound, 0, 2),
	}
}

func (c *chunkRange) toString(mode string, collation string) (string, []string) {
	if collation != "" {
		collation = fmt.Sprintf(" COLLATE '%s'", collation)
	}

	if mode != bucketMode {
		conditions := make([]string, 0, 2)
		args := make([]string, 0, 2)

		for _, bound := range c.bounds {
			if len(bound.lower) != 0 {
				conditions = append(conditions, fmt.Sprintf("`%s`%s %s ?", bound.column, collation, bound.lowerSymbol))
				args = append(args, bound.lower)
			}
			if len(bound.upper) != 0 {
				conditions = append(conditions, fmt.Sprintf("`%s`%s %s ?", bound.column, collation, bound.upperSymbol))
				args = append(args, bound.upper)
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

	for _, bound := range c.bounds {
		if len(bound.lower) != 0 {
			if len(preConditionForLower) > 0 {
				lowerCondition = append(lowerCondition, fmt.Sprintf("(%s AND `%s`%s %s ?)", strings.Join(preConditionForLower, " AND "), bound.column, collation, bound.lowerSymbol))
				lowerArgs = append(append(lowerArgs, preConditionArgsForLower...), bound.lower)
			} else {
				lowerCondition = append(lowerCondition, fmt.Sprintf("(`%s`%s %s ?)", bound.column, collation, bound.lowerSymbol))
				lowerArgs = append(lowerArgs, bound.lower)
			}
			preConditionForLower = append(preConditionForLower, fmt.Sprintf("`%s` = ?", bound.column))
			preConditionArgsForLower = append(preConditionArgsForLower, bound.lower)
		}

		if len(bound.upper) != 0 {
			if len(preConditionForUpper) > 0 {
				upperCondition = append(upperCondition, fmt.Sprintf("(%s AND `%s`%s %s ?)", strings.Join(preConditionForUpper, " AND "), bound.column, collation, bound.upperSymbol))
				upperArgs = append(append(upperArgs, preConditionArgsForUpper...), bound.upper)
			} else {
				upperCondition = append(upperCondition, fmt.Sprintf("(`%s`%s %s ?)", bound.column, collation, bound.upperSymbol))
				upperArgs = append(upperArgs, bound.upper)
			}
			preConditionForUpper = append(preConditionForUpper, fmt.Sprintf("`%s` = ?", bound.column))
			preConditionArgsForUpper = append(preConditionArgsForUpper, bound.upper)
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

func (c *chunkRange) update(column, lower, lowerSymbol, upper, upperSymbol string) {
	newBound := &bound{
		column:      column,
		lower:       lower,
		lowerSymbol: lowerSymbol,
		upper:       upper,
		upperSymbol: upperSymbol,
	}

	for i, b := range c.bounds {
		if b.column == column {
			// update the bound
			c.bounds[i] = newBound
			return
		}
	}

	// add a new bound
	c.bounds = append(c.bounds, newBound)
}

func (c *chunkRange) copy() *chunkRange {
	newChunk := &chunkRange{
		bounds: make([]*bound, len(c.bounds)),
	}
	copy(newChunk.bounds, c.bounds)

	return newChunk
}

func (c *chunkRange) copyAndUpdate(column, lower, lowerSymbol, upper, upperSymbol string) *chunkRange {
	newChunk := c.copy()
	newChunk.update(column, lower, lowerSymbol, upper, upperSymbol)
	return newChunk
}

func getChunksForTable(table *TableInstance, columns []*model.ColumnInfo, chunkSize int, limits string, collation string, useTiDBStatsInfo bool) ([]*chunkRange, string, error) {
	if useTiDBStatsInfo {
		s := bucketSpliter{}
		chunks, err := s.split(table, columns, chunkSize, limits, collation)
		if err == nil && len(chunks) > 0 {
			return chunks, bucketMode, nil
		}

		log.Warnf("use tidb bucket information to get chunks error: %v, chunks num: %d, will split chunk by random again", errors.Trace(err), len(chunks))
	}

	// get chunks from tidb bucket information failed, use random.
	s := randomSpliter{}
	chunks, err := s.split(table, columns, chunkSize, limits, collation)
	return chunks, normalMode, err
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

// CheckJob is the struct of job for check
type CheckJob struct {
	Schema string
	Table  string
	Where  string
	Args   []string
}

// GenerateCheckJob generates some CheckJobs.
func GenerateCheckJob(table *TableInstance, splitFields, limits string, chunkSize int, collation string, useTiDBStatsInfo bool) ([]*CheckJob, error) {
	jobBucket := make([]*CheckJob, 0, 10)
	var jobCnt int
	var err error

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

	chunks, mode, err := getChunksForTable(table, fields, chunkSize, limits, collation, useTiDBStatsInfo)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if chunks == nil {
		return nil, nil
	}

	jobCnt += len(chunks)

	for _, chunk := range chunks {
		conditions, args := chunk.toString(mode, collation)
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
