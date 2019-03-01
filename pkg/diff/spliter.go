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

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb-tools/pkg/dbutil"
	"github.com/pingcap/tidb-tools/pkg/utils"
	log "github.com/sirupsen/logrus"
)

type spliter interface {
	// split splits a table's data to several chunks.
	split(table *TableInstance, columns []*model.ColumnInfo, chunkSize int, limits string, collation string) ([]*chunkRange, error)
}

type randomSpliter struct {
	table     *TableInstance
	chunkSize int
	limits    string
	collation string
}

func (s *randomSpliter) split(table *TableInstance, columns []*model.ColumnInfo, chunkSize int, limits string, collation string) ([]*chunkRange, error) {
	s.table = table
	s.chunkSize = chunkSize
	s.limits = limits
	s.collation = collation

	// get the chunk count by data count and chunk size
	cnt, err := dbutil.GetRowCount(context.Background(), table.Conn, table.Schema, table.Table, limits)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if cnt == 0 {
		log.Infof("no data found in %s.%s", table.Schema, table.Table)
		return nil, nil
	}

	chunkCnt := (int(cnt) + chunkSize - 1) / chunkSize
	chunks, err := s.splitRange(table.Conn, newChunkRange(), chunkCnt, table.Schema, table.Table, columns)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return chunks, nil
}

// splitRange splits a chunk to multiple chunks.
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

	chunkLimits, args := chunk.toString(normalMode, s.collation)
	limitRange := fmt.Sprintf("%s AND %s", chunkLimits, s.limits)

	// if the last column's condition is not '=', continue use this column split data.
	colNum := len(chunk.bounds)
	if colNum != 0 && chunk.bounds[colNum-1].lowerSymbol != equal {
		splitCol = chunk.bounds[colNum-1].column
		min = chunk.bounds[colNum-1].lower
		max = chunk.bounds[colNum-1].upper
		symbolMin = chunk.bounds[colNum-1].lowerSymbol
		symbolMax = chunk.bounds[colNum-1].upperSymbol
	} else {
		if len(columns) <= colNum {
			log.Warnf("chunk %v can't be splited", chunk)
			return append(chunks, chunk), nil
		}

		// choose the next column to split data
		useNewColumn = true
		splitCol = columns[colNum].Name.O

		min, max, err = dbutil.GetMinMaxValue(context.Background(), db, schema, table, splitCol, limitRange, utils.StringsToInterfaces(args), s.collation)
		if err != nil {
			if errors.Cause(err) == dbutil.ErrNoData {
				log.Infof("no data found in %s.%s range %s, args %v", schema, table, limitRange, args)
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
	log.Debugf("split chunk %v, get split values from GetRandomValues: %v", chunk, randomValues)

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

	var lower, upper string
	lowerSymbol := gte
	upperSymbol := lt

	for i := 0; i < len(splitValues); i++ {
		if valueCounts[i] > 1 && useNewColumn {
			if i == 0 {
				// create chunk less than min %s", splitValues[0])
				newChunk := chunk.copyAndUpdate(splitCol, "", "", splitValues[i], lt)
				chunks = append(chunks, newChunk)
			}
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

		if i == 0 && valueCounts[i] == 1 {
			if useNewColumn {
				lower = ""
				lowerSymbol = ""
			} else {
				lower = splitValues[i]
				lowerSymbol = symbolMin
			}
		} else {
			lower = splitValues[i]
		}

		if i == len(splitValues)-2 {
			if useNewColumn && valueCounts[len(valueCounts)-1] == 1 {
				upper = ""
				upperSymbol = ""
			} else {
				upper = splitValues[i+1]
				if valueCounts[len(valueCounts)-1] == 1 {
					upperSymbol = symbolMax
				}
			}
		} else {
			if i == len(splitValues)-1 {
				break
			}

			upper = splitValues[i+1]
			upperSymbol = lt
		}

		newChunk := chunk.copyAndUpdate(splitCol, lower, lowerSymbol, upper, upperSymbol)
		chunks = append(chunks, newChunk)

		lowerSymbol = gte
	}

	log.Debugf("getChunksForTable cut table: cnt=%d min=%s max=%s chunk=%d", count, min, max, len(chunks))
	return chunks, nil
}

type bucketSpliter struct {
	table     *TableInstance
	chunkSize int
	limits    string
	collation string
	buckets   map[string][]dbutil.Bucket
}

func (s *bucketSpliter) split(table *TableInstance, columns []*model.ColumnInfo, chunkSize int, limits string, collation string) ([]*chunkRange, error) {
	s.table = table
	s.chunkSize = chunkSize
	s.limits = limits
	s.collation = collation

	buckets, err := dbutil.GetBucketsInfo(context.Background(), s.table.Conn, s.table.Schema, s.table.Table, s.table.info)
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
				chunk := newChunkRange()
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
