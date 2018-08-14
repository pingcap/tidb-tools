package checksum

import (
	"context"
	"crypto/md5"
	"database/sql"
	"fmt"
	"hash/crc64"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb-tools/pkg/dbutil"
)

type chunkChecksums []chunkChecksum

func (c chunkChecksums) Len() int { return len(c) }

func (c chunkChecksums) Swap(i, j int) { c[i], c[j] = c[j], c[i] }

func (c chunkChecksums) Less(i, j int) bool {
	rangeI := c[i].chunkRange[0].(int64)
	rangeJ := c[j].chunkRange[0].(int64)
	return rangeI < rangeJ
}

type chunkChecksum struct {
	checksum   uint64
	chunkRange []interface{}
}

type table struct {
	schema string
	name   string
}

// TableChecksum calculates table checksum.
type TableChecksum struct {
	ecmaTable        *crc64.Table
	cfg              *Config
	sourceDB         *sql.DB
	sourceSchema     string
	sample           int
	chunkSize        int
	checkThreadCount int
	tables           []*TableCheckCfg

	chunkChecksumsMu struct {
		sync.RWMutex
		chunkChecksums map[table]chunkChecksums
	}

	tableChecksumsMu struct {
		sync.RWMutex
		checksums map[table]uint64
	}
}

// NewTableChecksum creates an instance of TableChecksum.
func NewTableChecksum(cfg *Config, sourceDB *sql.DB) (*TableChecksum, error) {
	tc := &TableChecksum{
		ecmaTable:        crc64.MakeTable(crc64.ECMA),
		cfg:              cfg,
		sourceDB:         sourceDB,
		chunkSize:        cfg.ChunkSize,
		sample:           cfg.Sample,
		checkThreadCount: cfg.CheckThreadCount,
		sourceSchema:     cfg.SourceDBCfg.Schema,
		tables:           cfg.Tables,
	}

	tc.tableChecksumsMu.checksums = make(map[table]uint64)
	tc.chunkChecksumsMu.chunkChecksums = make(map[table]chunkChecksums)

	return tc, nil
}

// Process calculates table checksum and then save it to local file named by table name.
func (tc *TableChecksum) Process(ctx context.Context) error {
	timer := time.Now()
	err := tc.initTables(ctx)
	if err != nil {
		return errors.Trace(err)
	}

	err = tc.doChecksums(ctx)
	if err != nil {
		return errors.Trace(err)
	}

	err = tc.saveChecksum()
	if err != nil {
		return errors.Trace(err)
	}

	// very useful for debugging
	err = tc.generateChunkChecksumFile()
	if err != nil {
		return errors.Trace(err)
	}

	log.Infof("process schema %s_%d_%s takes %v", tc.cfg.SourceDBCfg.Host, tc.cfg.SourceDBCfg.Port, tc.sourceSchema, time.Since(timer))

	// TODO: write another program to compare checksum(do it with XOR)
	return nil
}

func (tc *TableChecksum) saveChecksum() error {
	tc.tableChecksumsMu.RLock()
	defer tc.tableChecksumsMu.RUnlock()

	for table, checksum := range tc.tableChecksumsMu.checksums {
		fname := fmt.Sprintf("%s_%d_%s_%s.checksum", tc.cfg.SourceDBCfg.Host, tc.cfg.SourceDBCfg.Port, table.schema, table.name)
		fd, err := os.OpenFile(fname, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0755)
		if err != nil {
			return errors.Trace(err)
		}
		_, err = fd.WriteString(strconv.FormatUint(checksum, 10))
		if err != nil {
			log.Errorf("write checksum failed fname %s, checksum %d", fname, checksum)
		}
		fd.Write([]byte{'\n'})
		err = fd.Close()
		if err != nil {
			log.Errorf("close file %s err %v", fname, err)
		}
	}

	return nil
}

func (tc *TableChecksum) generateChunkChecksumFile() error {
	tc.chunkChecksumsMu.Lock()
	for table, tableChunkChecksums := range tc.chunkChecksumsMu.chunkChecksums {
		fname := fmt.Sprintf("%s_%d_%s_%s.chunkchecksums", tc.cfg.SourceDBCfg.Host, tc.cfg.SourceDBCfg.Port, table.schema, table.name)

		fd, err := os.OpenFile(fname, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0755)
		if err != nil {
			return errors.Trace(err)
		}
		sort.Sort(tableChunkChecksums)
		for _, chunkChecksum := range tableChunkChecksums {
			_, err = fd.WriteString(strconv.FormatUint(chunkChecksum.checksum, 10))
			if err != nil {
				log.Errorf("write chunk checksum failed fname %s, chunk checksum %d", fname, chunkChecksum.checksum)
			}
			fd.WriteString(" ")
			fd.WriteString(fmt.Sprintf("%v", chunkChecksum.chunkRange))
			fd.Write([]byte{'\n'})
		}

		err = fd.Close()
		if err != nil {
			log.Errorf("close file %s err %v", fname, err)
		}
	}

	tc.chunkChecksumsMu.Unlock()
	return nil
}

func (tc *TableChecksum) initTables(ctx context.Context) error {
	var err error
	for _, table := range tc.tables {
		table.Info, err = dbutil.GetTableInfo(ctx, tc.sourceDB, tc.sourceSchema, table.Name)
		if err != nil {
			return errors.Trace(err)
		}
		table.Schema = tc.sourceSchema
	}

	if len(tc.tables) == 0 {
		tbls1, err := dbutil.GetTables(ctx, tc.sourceDB, tc.sourceSchema)
		if err != nil {
			return errors.Trace(err)
		}

		tc.tables = make([]*TableCheckCfg, 0, len(tbls1))
		for _, name := range tbls1 {
			table := &TableCheckCfg{Name: name, Schema: tc.sourceSchema}
			table.Info, err = dbutil.GetTableInfo(ctx, tc.sourceDB, tc.sourceSchema, name)
			if err != nil {
				return errors.Trace(err)
			}
			table.Schema = tc.sourceSchema
			tc.tables = append(tc.tables, table)
		}
	}

	return nil
}

func (tc *TableChecksum) doChecksums(ctx context.Context) error {
	for _, table := range tc.tables {
		err := tc.ChecksumTableData(ctx, table)
		if err != nil {
			log.Errorf("equal table error %v", err)
			return errors.Trace(err)
		}

	}
	return nil
}

func generateRangeQuery(schemaName, tableName string, where string) string {
	return fmt.Sprintf("SELECT * FROM `%s`.`%s` WHERE %s", schemaName, tableName, where)
}

// GetCrc64ChecksumByQuery fetch data rows from the query and do md5 hash for each row, and finally XOR them with crc64 hash, returns a final crc64 checksum.
func GetCrc64ChecksumByQuery(ctx context.Context, db *sql.DB, ecmaTable *crc64.Table, query string, args []interface{}) (uint64, error) {
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return 0, errors.Trace(err)
	}
	defer rows.Close()

	var checksum uint64

	for rows.Next() {
		singleRow, err := ScanRow(rows)
		if err != nil {
			return 0, errors.Trace(err)
		}
		h := md5.New()
		for _, col := range singleRow {
			h.Write(col)
		}
		sum := crc64.Update(0, ecmaTable, h.Sum(nil))
		checksum ^= sum
	}

	return checksum, nil
}

// ScanRow scans rows into a map.
func ScanRow(rows *sql.Rows) ([][]byte, error) {
	cols, err := rows.Columns()
	if err != nil {
		return nil, errors.Trace(err)
	}

	colVals := make([][]byte, len(cols))
	colValsI := make([]interface{}, len(colVals))
	for i := range colValsI {
		colValsI[i] = &colVals[i]
	}

	err = rows.Scan(colValsI...)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return colVals, nil
}

// ChecksumTableData does checksum for table data.
func (tc *TableChecksum) ChecksumTableData(ctx context.Context, tableCfg *TableCheckCfg) error {
	// TODO: now only check data between source data's min and max, need check data less than min and greater than max.
	allJobs, err := GenerateCheckJob(tc.sourceDB, tc.sourceSchema, tableCfg.Info, tableCfg.Field, tableCfg.Range, tc.chunkSize, tc.sample)
	if err != nil {
		return errors.Trace(err)
	}

	checkNums := len(allJobs) * tc.sample / 100
	checkNumArr := getRandomN(len(allJobs), checkNums)

	var wg sync.WaitGroup
	for i := 0; i < tc.checkThreadCount; i++ {
		checkJobs := make([]*CheckJob, 0, len(checkNumArr))
		for j := len(checkNumArr) * i / tc.checkThreadCount; j < len(checkNumArr)*(i+1)/tc.checkThreadCount && j < len(checkNumArr); j++ {
			checkJobs = append(checkJobs, allJobs[checkNumArr[j]])
		}
		wg.Add(1)
		go func() {
			chunkChecksum, err := tc.checksumChunkData(ctx, checkJobs, tableCfg)
			if err != nil {
				log.Errorf("check chunk data equal failed, error %v", errors.ErrorStack(err))
			}

			tc.tableChecksumsMu.Lock()
			tc.tableChecksumsMu.checksums[table{tc.sourceSchema, tableCfg.Name}] ^= chunkChecksum
			tc.tableChecksumsMu.Unlock()

			wg.Done()
		}()
	}

	wg.Wait()

	return nil
}

func (tc *TableChecksum) checksumChunkData(ctx context.Context, checkJobs []*CheckJob, tableCfg *TableCheckCfg) (uint64, error) {
	if len(checkJobs) == 0 {
		return 0, nil
	}

	var checksum uint64
	for _, job := range checkJobs {
		query := generateRangeQuery(tc.sourceSchema, tableCfg.Name, job.Where)
		chunkChecksumValue, err := GetCrc64ChecksumByQuery(ctx, tc.sourceDB, tc.ecmaTable, query, job.Args)
		if err != nil {
			return 0, errors.Trace(err)
		}
		log.Infof("table `%s`.`%s` chunk checksum %v, query %s, range %v", tc.sourceSchema, tableCfg.Name, chunkChecksumValue, query, job.Args)

		tc.chunkChecksumsMu.Lock()
		key := table{tc.sourceSchema, tableCfg.Name}
		tc.chunkChecksumsMu.chunkChecksums[key] = append(
			tc.chunkChecksumsMu.chunkChecksums[key],
			chunkChecksum{
				checksum:   chunkChecksumValue,
				chunkRange: job.Args,
			})
		tc.chunkChecksumsMu.Unlock()

		checksum ^= chunkChecksumValue
	}

	return checksum, nil
}

func getRandomN(total, num int) []int {
	if num > total {
		log.Warnf("the num %d is greater than total %d", num, total)
		num = total
	}

	totalArray := make([]int, 0, total)
	for i := 0; i < total; i++ {
		totalArray = append(totalArray, i)
	}

	for j := 0; j < num; j++ {
		r := j + rand.Intn(total-j)
		totalArray[j], totalArray[r] = totalArray[r], totalArray[j]
	}

	return totalArray[:num]
}
