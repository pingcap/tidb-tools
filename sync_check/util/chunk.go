package util

import (
	"database/sql"
	"fmt"
	"reflect"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/siddontang/go-mysql/schema"
)

// Range represents chunk range
type chunkRange struct {
	begin interface{}
	end   interface{}
	// for example:
	// containB and containE is true, means [begin, end]
	// containB is true, containE is false is false, means [begin, end)
	containB bool
	containE bool
	notNil   bool
}

// dumpJob is the struct for job of dump
type dumpJob struct {
	dbName string
	table  string
	column *schema.TableColumn
	where  string
	chunk  chunkRange
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

func getChunksForTable(db *sql.DB, dbname string, table string, column *schema.TableColumn, lastTime string, chunkSize int, sample int) ([]chunkRange, error) {
	noChunks := []chunkRange{{}}
	if column == nil {
		log.Warnf("No suitable index found for %s.%s", dbname, table)
		return noChunks, nil
	}

	field := column.Name

	// fetch min, max
	query := fmt.Sprintf("SELECT %s MIN(`%s`) as MIN, MAX(`%s`) as MAX FROM `%s`.`%s` where `e` <= \"%s\"",
		"/*!40001 SQL_NO_CACHE */", field, field, dbname, table, lastTime)
	log.Debugf("[dumper] get max min query sql: %s", query)

	// get the chunk count
	cnt, err := GetCount(db, dbname, table, field, lastTime)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if cnt == 0 {
		log.Infof("no data found in %s.%s", dbname, table)
		return noChunks, nil
	}

	chunkCnt := cnt / int64(chunkSize)
	if sample != 100 {
		chunkCnt *= 10
	}

	var chunk chunkRange
	if column.Type == schema.TYPE_NUMBER {
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
	} else if column.Type == schema.TYPE_FLOAT {
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
		var min, max string
		err := db.QueryRow(query).Scan(&min, &max)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if min == "" || max == "" {
			// min is NULL, means that no table data.
			return []chunkRange{}, nil
		}
		chunk = newChunkRange(min, max, true, true, true)
	}
	return splitRange(db, &chunk, chunkCnt, dbname, table, column, lastTime)
}

func splitRange(db *sql.DB, chunk *chunkRange, count int64, dbname string, table string, column *schema.TableColumn, lastTime string) ([]chunkRange, error) {
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
		splitValues, err := GetRandomValues(db, dbname, table, column.Name, count-1, min, max, lastTime)
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

func findSuitableField(db *sql.DB, dbname string, table string) (*schema.TableColumn, error) {
	// first select the index with number type
	column, err := FindSuitableIndex(db, dbname, table, true)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if column != nil {
		return column, nil
	}
	log.Infof("%s.%s don't have index, will use a number column as split field", dbname, table)

	// select a number column
	column, err = FindNumberColumn(db, dbname, table)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if column != nil {
		return column, nil
	}
	log.Infof("%s.%s don't have number columns, will use the first column as split field", dbname, table)

	// use the first column
	column, err = GetFirstColumn(db, dbname, table)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if column != nil {
		return column, nil
	}

	return nil, errors.Errorf("no column find in table %s.%s", dbname, table)
}

func generateDumpJob(db *sql.DB, dbname string, table string, lastTime string, chunkSize int, sample int) ([]*dumpJob, error) {
	jobBucket := make([]*dumpJob, 0, 10)
	var jobCnt int

	// find a column for split data
	column, err := findSuitableField(db, dbname, table)
	if err != nil {
		return nil, errors.Trace(err)
	}

	chunks, err := getChunksForTable(db, dbname, table, column, lastTime, chunkSize, sample)
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
			where = fmt.Sprintf("%s AND e <= \"%s\"", where, lastTime)
		} else {
			where = ""
		}

		log.Debugf("%s.%s create dump job: where: %s", dbname, table, where)
		jobBucket = append(jobBucket, &dumpJob{
			dbName: dbname,
			table:  table,
			column: column,
			where:  where,
			chunk:  chunk,
		})
	}

	return jobBucket, nil
}
