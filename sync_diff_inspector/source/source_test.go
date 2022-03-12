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

package source

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/pingcap/tidb-tools/pkg/dbutil"
	filter "github.com/pingcap/tidb-tools/pkg/table-filter"
	router "github.com/pingcap/tidb-tools/pkg/table-router"
	"github.com/pingcap/tidb-tools/sync_diff_inspector/chunk"
	"github.com/pingcap/tidb-tools/sync_diff_inspector/config"
	"github.com/pingcap/tidb-tools/sync_diff_inspector/source/common"
	"github.com/pingcap/tidb-tools/sync_diff_inspector/splitter"
	"github.com/pingcap/tidb/parser"
	"github.com/stretchr/testify/require"

	_ "github.com/go-sql-driver/mysql"
)

type tableCaseType struct {
	schema         string
	table          string
	createTableSQL string
	rangeColumns   []string
	rangeLeft      []string
	rangeRight     []string
	rangeInfo      *splitter.RangeInfo
	rowQuery       string
	rowColumns     []string
	rows           [][]driver.Value
}

type MockChunkIterator struct {
	ctx       context.Context
	tableDiff *common.TableDiff
	rangeInfo *splitter.RangeInfo
	index     *chunk.ChunkID
}

const CHUNKS = 5
const BUCKETS = 1

func equal(a, b *chunk.ChunkID) bool {
	return a.TableIndex == b.TableIndex && a.BucketIndexLeft == b.BucketIndexLeft && a.ChunkIndex == b.ChunkIndex
}

func next(a *chunk.ChunkID) {
	if a.ChunkIndex == a.ChunkCnt-1 {
		a.TableIndex++
		a.ChunkIndex = 0
	} else {
		a.ChunkIndex = a.ChunkIndex + 1
	}
}

func newIndex() *chunk.ChunkID {
	return &chunk.ChunkID{
		TableIndex:       0,
		BucketIndexLeft:  0,
		BucketIndexRight: 0,
		ChunkIndex:       0,
		ChunkCnt:         CHUNKS,
	}
}

func (m *MockChunkIterator) Next() (*chunk.Range, error) {
	if m.index.ChunkIndex == m.index.ChunkCnt-1 {
		return nil, nil
	}
	m.index.ChunkIndex = m.index.ChunkIndex + 1
	return &chunk.Range{
		Index: &chunk.ChunkID{
			TableIndex:       m.index.TableIndex,
			BucketIndexLeft:  m.index.BucketIndexLeft,
			BucketIndexRight: m.index.BucketIndexRight,
			ChunkIndex:       m.index.ChunkIndex,
			ChunkCnt:         m.index.ChunkCnt,
		},
	}, nil
}

func (m *MockChunkIterator) Close() {

}

type MockAnalyzer struct {
}

func (m *MockAnalyzer) AnalyzeSplitter(ctx context.Context, tableDiff *common.TableDiff, rangeInfo *splitter.RangeInfo) (splitter.ChunkIterator, error) {
	i := &chunk.ChunkID{
		TableIndex:       0,
		BucketIndexLeft:  0,
		BucketIndexRight: 0,
		ChunkIndex:       -1,
		ChunkCnt:         CHUNKS,
	}
	return &MockChunkIterator{
		ctx,
		tableDiff,
		rangeInfo,
		i,
	}, nil
}

func TestTiDBSource(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conn, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer conn.Close()

	tableCases := []*tableCaseType{
		{
			schema:         "source_test",
			table:          "test1",
			createTableSQL: "CREATE TABLE `source_test`.`test1` (`a` int, `b` varchar(24), `c` float, primary key(`a`, `b`))",
			rangeColumns:   []string{"a", "b"},
			rangeLeft:      []string{"3", "b"},
			rangeRight:     []string{"5", "f"},
			rowQuery:       "SELECT",
			rowColumns:     []string{"a", "b", "c"},
			rows: [][]driver.Value{
				{"1", "a", "1.2"},
				{"2", "b", "3.4"},
				{"3", "c", "5.6"},
				{"4", "d", "6.7"},
			},
		},
		{
			schema:         "source_test",
			table:          "test2",
			createTableSQL: "CREATE TABLE `source_test`.`test2` (`a` int, `b` varchar(24), `c` float, `d` datetime, primary key(`a`, `b`))",
			rangeColumns:   []string{"a", "b"},
			rangeLeft:      []string{"3", "b"},
			rangeRight:     []string{"5", "f"},
		},
	}

	tableDiffs := prepareTiDBTables(t, tableCases)

	mock.ExpectQuery("SHOW DATABASES").WillReturnRows(sqlmock.NewRows([]string{"Database"}).AddRow("mysql").AddRow("source_test"))
	mock.ExpectQuery("SHOW FULL TABLES*").WillReturnRows(sqlmock.NewRows([]string{"Table", "type"}).AddRow("test1", "base").AddRow("test2", "base"))

	f, err := filter.Parse([]string{"source_test.*"})
	require.NoError(t, err)
	tidb, err := NewTiDBSource(ctx, tableDiffs, &config.DataSource{Conn: conn}, 1, f)
	require.NoError(t, err)

	for n, tableCase := range tableCases {
		require.Equal(t, n, tableCase.rangeInfo.GetTableIndex())
		countRows := sqlmock.NewRows([]string{"CNT", "CHECKSUM"}).AddRow(123, 456)
		mock.ExpectQuery("SELECT COUNT.*").WillReturnRows(countRows)
		checksum := tidb.GetCountAndCrc32(ctx, tableCase.rangeInfo)
		require.NoError(t, checksum.Err)
		require.Equal(t, checksum.Count, int64(123))
		require.Equal(t, checksum.Checksum, int64(456))
	}

	// Test ChunkIterator
	iter, err := tidb.GetRangeIterator(ctx, tableCases[0].rangeInfo, &MockAnalyzer{})
	require.NoError(t, err)
	resRecords := [][]bool{
		{false, false, false, false, false},
		{false, false, false, false, false},
	}
	for {
		ch, err := iter.Next(ctx)
		require.NoError(t, err)
		if ch == nil {
			break
		}
		require.Equal(t, ch.ChunkRange.Index.ChunkCnt, 5)
		require.Equal(t, resRecords[ch.ChunkRange.Index.TableIndex][ch.ChunkRange.Index.ChunkIndex], false)
		resRecords[ch.ChunkRange.Index.TableIndex][ch.ChunkRange.Index.ChunkIndex] = true
	}
	iter.Close()
	require.Equal(t, resRecords, [][]bool{
		{true, true, true, true, true},
		{true, true, true, true, true},
	})

	// Test RowIterator
	tableCase := tableCases[0]
	dataRows := sqlmock.NewRows(tableCase.rowColumns)
	for _, row := range tableCase.rows {
		dataRows.AddRow(row...)
	}
	mock.ExpectQuery(tableCase.rowQuery).WillReturnRows(dataRows)
	rowIter, err := tidb.GetRowsIterator(ctx, tableCase.rangeInfo)
	require.NoError(t, err)

	row := 0
	var firstRow, secondRow map[string]*dbutil.ColumnData
	for {
		columns, err := rowIter.Next()
		require.NoError(t, err)
		if columns == nil {
			require.Equal(t, row, len(tableCase.rows))
			break
		}
		for j, value := range tableCase.rows[row] {
			require.Equal(t, columns[tableCase.rowColumns[j]].IsNull, false)
			require.Equal(t, columns[tableCase.rowColumns[j]].Data, []byte(value.(string)))
		}
		if row == 0 {
			firstRow = columns
		} else if row == 1 {
			secondRow = columns
		}
		row++
	}
	require.Equal(t, tidb.GenerateFixSQL(Insert, firstRow, secondRow, 0), "REPLACE INTO `source_test`.`test1`(`a`,`b`,`c`) VALUES (1,'a',1.2);")
	require.Equal(t, tidb.GenerateFixSQL(Delete, firstRow, secondRow, 0), "DELETE FROM `source_test`.`test1` WHERE `a` = 2 AND `b` = 'b' AND `c` = 3.4 LIMIT 1;")
	require.Equal(t, tidb.GenerateFixSQL(Replace, firstRow, secondRow, 0),
		"/*\n"+
			"  DIFF COLUMNS ╏ `A` ╏ `B` ╏ `C`  \n"+
			"╍╍╍╍╍╍╍╍╍╍╍╍╍╍╍╋╍╍╍╍╍╋╍╍╍╍╍╋╍╍╍╍╍╍\n"+
			"  source data  ╏ 1   ╏ 'a' ╏ 1.2  \n"+
			"╍╍╍╍╍╍╍╍╍╍╍╍╍╍╍╋╍╍╍╍╍╋╍╍╍╍╍╋╍╍╍╍╍╍\n"+
			"  target data  ╏ 2   ╏ 'b' ╏ 3.4  \n"+
			"╍╍╍╍╍╍╍╍╍╍╍╍╍╍╍╋╍╍╍╍╍╋╍╍╍╍╍╋╍╍╍╍╍╍\n"+
			"*/\n"+
			"REPLACE INTO `source_test`.`test1`(`a`,`b`,`c`) VALUES (1,'a',1.2);")

	rowIter.Close()

	analyze := tidb.GetTableAnalyzer()
	countRows := sqlmock.NewRows([]string{"Cnt"}).AddRow(0)
	mock.ExpectQuery("SELECT COUNT.*").WillReturnRows(countRows)
	chunkIter, err := analyze.AnalyzeSplitter(ctx, tableDiffs[0], tableCase.rangeInfo)
	require.NoError(t, err)
	chunkIter.Close()
	tidb.Close()
}

func TestMysqlShardSources(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	tableCases := []*tableCaseType{
		{
			schema:         "source_test",
			table:          "test1",
			createTableSQL: "CREATE TABLE `source_test`.`test1` (`a` int, `b` varchar(24), `c` float, primary key(`a`, `b`))",
			rangeColumns:   []string{"a", "b"},
			rangeLeft:      []string{"3", "b"},
			rangeRight:     []string{"5", "f"},
			rowQuery:       "SELECT.*",
			rowColumns:     []string{"a", "b", "c"},
			rows: [][]driver.Value{
				{"1", "a", "1.2"},
				{"2", "b", "2.2"},
				{"3", "c", "3.2"},
				{"4", "d", "4.2"},
				{"5", "e", "5.2"},
				{"6", "f", "6.2"},
				{"7", "g", "7.2"},
				{"8", "h", "8.2"},
				{"9", "i", "9.2"},
				{"10", "j", "10.2"},
				{"11", "k", "11.2"},
				{"12", "l", "12.2"},
			},
		},
		{
			schema:         "source_test",
			table:          "test2",
			createTableSQL: "CREATE TABLE `source_test`.`test2` (`a` int, `b` varchar(24), `c` float, `d` datetime, primary key(`a`, `b`))",
			rangeColumns:   []string{"a", "b"},
			rangeLeft:      []string{"3", "b"},
			rangeRight:     []string{"5", "f"},
		},
	}

	tableDiffs := prepareTiDBTables(t, tableCases)

	conn, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer conn.Close()

	dbs := []*sql.DB{
		conn, conn, conn, conn,
	}

	cs := make([]*config.DataSource, 4)
	for i := range dbs {
		mock.ExpectQuery("SHOW DATABASES").WillReturnRows(sqlmock.NewRows([]string{"Database"}).AddRow("mysql").AddRow("source_test"))
		mock.ExpectQuery("SHOW FULL TABLES*").WillReturnRows(sqlmock.NewRows([]string{"Table", "type"}).AddRow("test1", "base").AddRow("test2", "base"))
		cs[i] = &config.DataSource{Conn: conn}
	}

	f, err := filter.Parse([]string{"source_test.*"})
	require.NoError(t, err)
	shard, err := NewMySQLSources(ctx, tableDiffs, cs, 4, f)
	require.NoError(t, err)

	for i := 0; i < len(dbs); i++ {
		infoRows := sqlmock.NewRows([]string{"Table", "Create Table"}).AddRow("test_t", "CREATE TABLE `source_test`.`test1` (`a` int, `b` varchar(24), `c` float, primary key(`a`, `b`))")
		variableRows := sqlmock.NewRows([]string{"Variable_name", "Value"}).AddRow("sql_mode", "ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION")

		mock.ExpectQuery("SHOW CREATE TABLE.*").WillReturnRows(infoRows)
		mock.ExpectQuery("SHOW VARIABLE.*").WillReturnRows(variableRows)
	}
	info, err := shard.GetSourceStructInfo(ctx, 0)
	require.NoError(t, err)
	require.Equal(t, info[0].Name.O, "test1")

	for n, tableCase := range tableCases {
		require.Equal(t, n, tableCase.rangeInfo.GetTableIndex())
		var resChecksum int64 = 0
		for i := 0; i < len(dbs); i++ {
			resChecksum = resChecksum + 1<<i
			countRows := sqlmock.NewRows([]string{"CNT", "CHECKSUM"}).AddRow(1, 1<<i)
			mock.ExpectQuery("SELECT COUNT.*").WillReturnRows(countRows)
		}

		checksum := shard.GetCountAndCrc32(ctx, tableCase.rangeInfo)
		require.NoError(t, checksum.Err)
		require.Equal(t, checksum.Count, int64(len(dbs)))
		require.Equal(t, checksum.Checksum, resChecksum)
	}

	// Test RowIterator
	tableCase := tableCases[0]
	rowNums := len(tableCase.rows) / len(dbs)
	i := 0
	for j := 0; j < len(dbs); j++ {
		dataRows := sqlmock.NewRows(tableCase.rowColumns)
		for k := 0; k < rowNums; k++ {
			dataRows.AddRow(tableCase.rows[i]...)
			i++
		}
		mock.ExpectQuery(tableCase.rowQuery).WillReturnRows(dataRows)
	}

	rowIter, err := shard.GetRowsIterator(ctx, tableCase.rangeInfo)
	require.NoError(t, err)

	i = 0
	for {
		columns, err := rowIter.Next()
		require.NoError(t, err)
		if columns == nil {
			require.Equal(t, i, len(tableCase.rows))
			break
		}
		for j, value := range tableCase.rows[i] {
			//c.Log(j)
			require.Equal(t, columns[tableCase.rowColumns[j]].IsNull, false)
			require.Equal(t, columns[tableCase.rowColumns[j]].Data, []byte(value.(string)))
		}

		i++
	}
	rowIter.Close()

	shard.Close()
}

func TestMysqlRouter(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conn, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer conn.Close()

	tableCases := []*tableCaseType{
		{
			schema:         "source_test",
			table:          "test1",
			createTableSQL: "CREATE TABLE `source_test`.`test1` (`a` int, `b` varchar(24), `c` float, primary key(`a`, `b`))",
			rangeColumns:   []string{"a", "b"},
			rangeLeft:      []string{"3", "b"},
			rangeRight:     []string{"5", "f"},
			rowQuery:       "SELECT",
			rowColumns:     []string{"a", "b", "c"},
			rows: [][]driver.Value{
				{"1", "a", "1.2"},
				{"2", "b", "3.4"},
				{"3", "c", "5.6"},
				{"4", "d", "6.7"},
			},
		},
		{
			schema:         "source_test",
			table:          "test2",
			createTableSQL: "CREATE TABLE `source_test`.`test2` (`a` int, `b` varchar(24), `c` float, `d` datetime, primary key(`a`, `b`))",
			rangeColumns:   []string{"a", "b"},
			rangeLeft:      []string{"3", "b"},
			rangeRight:     []string{"5", "f"},
		},
	}

	tableDiffs := prepareTiDBTables(t, tableCases)

	routeRuleList := []*router.TableRule{
		{
			SchemaPattern: "source_test_t",
			TablePattern:  "test_t",
			TargetSchema:  "source_test",
			TargetTable:   "test1",
		},
	}
	router, err := router.NewTableRouter(false, routeRuleList)
	require.NoError(t, err)
	ds := &config.DataSource{
		Router: router,
		Conn:   conn,
	}

	databasesRows := sqlmock.NewRows([]string{"Database"}).AddRow("source_test").AddRow("source_test_t")
	mock.ExpectQuery("SHOW DATABASES").WillReturnRows(databasesRows)
	tablesRows := sqlmock.NewRows([]string{"Tables_in_test", "Table_type"}).AddRow("test2", "BASE TABLE")
	mock.ExpectQuery("SHOW FULL TABLES IN.*").WillReturnRows(tablesRows)
	tablesRows = sqlmock.NewRows([]string{"Tables_in_test", "Table_type"}).AddRow("test_t", "BASE TABLE")
	mock.ExpectQuery("SHOW FULL TABLES IN.*").WillReturnRows(tablesRows)

	f, err := filter.Parse([]string{"*.*"})
	require.NoError(t, err)
	mysql, err := NewMySQLSources(ctx, tableDiffs, []*config.DataSource{ds}, 4, f)
	require.NoError(t, err)

	// random splitter
	countRows := sqlmock.NewRows([]string{"Cnt"}).AddRow(0)
	mock.ExpectQuery("SELECT COUNT.*").WillReturnRows(countRows)
	rangeIter, err := mysql.GetRangeIterator(ctx, nil, mysql.GetTableAnalyzer())
	rangeIter.Next(ctx)
	require.NoError(t, err)
	rangeIter.Close()

	rangeIter, err = mysql.GetRangeIterator(ctx, tableCases[0].rangeInfo, mysql.GetTableAnalyzer())
	require.NoError(t, err)
	rangeIter.Close()

	// row Iterator
	dataRows := sqlmock.NewRows(tableCases[0].rowColumns)
	for k := 0; k < 2; k++ {
		dataRows.AddRow(tableCases[0].rows[k]...)
	}
	mock.ExpectQuery(tableCases[0].rowQuery).WillReturnRows(dataRows)

	rowIter, err := mysql.GetRowsIterator(ctx, tableCases[0].rangeInfo)
	require.NoError(t, err)
	firstRow, err := rowIter.Next()
	require.NoError(t, err)
	require.NotNil(t, firstRow)
	secondRow, err := rowIter.Next()
	require.NoError(t, err)
	require.NotNil(t, secondRow)
	require.Equal(t, mysql.GenerateFixSQL(Insert, firstRow, secondRow, 0), "REPLACE INTO `source_test`.`test1`(`a`,`b`,`c`) VALUES (1,'a',1.2);")
	require.Equal(t, mysql.GenerateFixSQL(Delete, firstRow, secondRow, 0), "DELETE FROM `source_test`.`test1` WHERE `a` = 2 AND `b` = 'b' AND `c` = 3.4 LIMIT 1;")
	require.Equal(t, mysql.GenerateFixSQL(Replace, firstRow, secondRow, 0),
		"/*\n"+
			"  DIFF COLUMNS ╏ `A` ╏ `B` ╏ `C`  \n"+
			"╍╍╍╍╍╍╍╍╍╍╍╍╍╍╍╋╍╍╍╍╍╋╍╍╍╍╍╋╍╍╍╍╍╍\n"+
			"  source data  ╏ 1   ╏ 'a' ╏ 1.2  \n"+
			"╍╍╍╍╍╍╍╍╍╍╍╍╍╍╍╋╍╍╍╍╍╋╍╍╍╍╍╋╍╍╍╍╍╍\n"+
			"  target data  ╏ 2   ╏ 'b' ╏ 3.4  \n"+
			"╍╍╍╍╍╍╍╍╍╍╍╍╍╍╍╋╍╍╍╍╍╋╍╍╍╍╍╋╍╍╍╍╍╍\n"+
			"*/\n"+
			"REPLACE INTO `source_test`.`test1`(`a`,`b`,`c`) VALUES (1,'a',1.2);")
	rowIter.Close()

	mysql.Close()
}

func TestTiDBRouter(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conn, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer conn.Close()

	tableCases := []*tableCaseType{
		{
			schema:         "source_test",
			table:          "test1",
			createTableSQL: "CREATE TABLE `source_test`.`test1` (`a` int, `b` varchar(24), `c` float, primary key(`a`, `b`))",
			rangeColumns:   []string{"a", "b"},
			rangeLeft:      []string{"3", "b"},
			rangeRight:     []string{"5", "f"},
			rowQuery:       "SELECT",
			rowColumns:     []string{"a", "b", "c"},
			rows: [][]driver.Value{
				{"1", "a", "1.2"},
				{"2", "b", "3.4"},
				{"3", "c", "5.6"},
				{"4", "d", "6.7"},
			},
		},
		{
			schema:         "source_test",
			table:          "test2",
			createTableSQL: "CREATE TABLE `source_test`.`test2` (`a` int, `b` varchar(24), `c` float, `d` datetime, primary key(`a`, `b`))",
			rangeColumns:   []string{"a", "b"},
			rangeLeft:      []string{"3", "b"},
			rangeRight:     []string{"5", "f"},
		},
	}

	tableDiffs := prepareTiDBTables(t, tableCases)

	routeRuleList := []*router.TableRule{
		{
			SchemaPattern: "source_test_t",
			TablePattern:  "test_t",
			TargetSchema:  "source_test",
			TargetTable:   "test1",
		},
	}
	router, err := router.NewTableRouter(false, routeRuleList)
	require.NoError(t, err)
	ds := &config.DataSource{
		Router: router,
		Conn:   conn,
	}

	databasesRows := sqlmock.NewRows([]string{"Database"}).AddRow("source_test_t").AddRow("source_test")
	mock.ExpectQuery("SHOW DATABASES").WillReturnRows(databasesRows)
	tablesRows := sqlmock.NewRows([]string{"Tables_in_test", "Table_type"}).AddRow("test_t", "BASE TABLE")
	mock.ExpectQuery("SHOW FULL TABLES IN.*").WillReturnRows(tablesRows)
	tablesRows = sqlmock.NewRows([]string{"Tables_in_test", "Table_type"}).AddRow("test2", "BASE TABLE")
	mock.ExpectQuery("SHOW FULL TABLES IN.*").WillReturnRows(tablesRows)

	f, err := filter.Parse([]string{"*.*"})
	require.NoError(t, err)
	tidb, err := NewTiDBSource(ctx, tableDiffs, ds, 1, f)
	require.NoError(t, err)
	infoRows := sqlmock.NewRows([]string{"Table", "Create Table"}).AddRow("test_t", "CREATE TABLE `source_test`.`test1` (`a` int, `b` varchar(24), `c` float, primary key(`a`, `b`))")
	mock.ExpectQuery("SHOW CREATE TABLE.*").WillReturnRows(infoRows)
	variableRows := sqlmock.NewRows([]string{"Variable_name", "Value"}).AddRow("sql_mode", "ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION")
	mock.ExpectQuery("SHOW VARIABLE.*").WillReturnRows(variableRows)
	info, err := tidb.GetSourceStructInfo(ctx, 0)
	require.NoError(t, err)
	require.Equal(t, info[0].Name.O, "test1")
}

func prepareTiDBTables(t *testing.T, tableCases []*tableCaseType) []*common.TableDiff {
	tableDiffs := make([]*common.TableDiff, 0, len(tableCases))
	for n, tableCase := range tableCases {
		tableInfo, err := dbutil.GetTableInfoBySQL(tableCase.createTableSQL, parser.New())
		require.NoError(t, err)
		tableDiffs = append(tableDiffs, &common.TableDiff{
			Schema: "source_test",
			Table:  fmt.Sprintf("test%d", n+1),
			Info:   tableInfo,
		})

		chunkRange := chunk.NewChunkRange()
		for i, column := range tableCase.rangeColumns {
			chunkRange.Update(column, tableCase.rangeLeft[i], tableCase.rangeRight[i], true, true)
		}

		chunk.InitChunk(chunkRange, chunk.Bucket, 0, 0, "", "")
		chunkRange.Index.TableIndex = n
		rangeInfo := &splitter.RangeInfo{
			ChunkRange: chunkRange,
		}
		tableCase.rangeInfo = rangeInfo
	}

	return tableDiffs
}

func TestSource(t *testing.T) {
	host, isExist := os.LookupEnv("MYSQL_HOST")
	if host == "" || !isExist {
		return
	}
	portstr, isExist := os.LookupEnv("MYSQL_PORT")
	if portstr == "" || !isExist {
		return
	}

	port, err := strconv.Atoi(portstr)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	router, err := router.NewTableRouter(false, nil)
	cfg := &config.Config{
		LogLevel:         "debug",
		CheckThreadCount: 4,
		ExportFixSQL:     true,
		CheckStructOnly:  false,
		DataSources: map[string]*config.DataSource{
			"mysql1": {
				Host: host,
				Port: port,
				User: "root",
			},
			"tidb": {
				Host: host,
				Port: port,
				User: "root",
			},
		},
		Routes: nil,
		TableConfigs: map[string]*config.TableConfig{
			"config1": {
				Schema:          "schama1",
				Table:           "tbl",
				IgnoreColumns:   []string{"", ""},
				Fields:          []string{""},
				Range:           "a > 10 AND a < 20",
				TargetTableInfo: nil,
				Collation:       "",
			},
		},
		Task: config.TaskConfig{
			Source:       []string{"mysql1"},
			Routes:       nil,
			Target:       "tidb",
			CheckTables:  []string{"schema*.tbl"},
			TableConfigs: []string{"config1"},
			OutputDir:    "./output",
			SourceInstances: []*config.DataSource{
				{
					Host:   host,
					Port:   port,
					User:   "root",
					Router: router,
				},
			},
			TargetInstance: &config.DataSource{
				Host: host,
				Port: port,
				User: "root",
			},
			TargetTableConfigs: []*config.TableConfig{
				{
					Schema:          "schema1",
					Table:           "tbl",
					IgnoreColumns:   []string{"", ""},
					Fields:          []string{""},
					Range:           "a > 10 AND a < 20",
					TargetTableInfo: nil,
					Collation:       "",
				},
			},
			TargetCheckTables: nil,
			FixDir:            "output/fix-on-tidb0",
			CheckpointDir:     "output/checkpoint",
			HashFile:          "",
		},
		ConfigFile:   "config.toml",
		PrintVersion: false,
	}
	cfg.Task.TargetCheckTables, err = filter.Parse([]string{"schema*.tbl"})
	require.NoError(t, err)

	// create table
	conn, err := sql.Open("mysql", fmt.Sprintf("root:@tcp(%s:%d)/?charset=utf8mb4", host, port))
	require.NoError(t, err)

	conn.Exec("CREATE DATABASE IF NOT EXISTS schema1")
	conn.Exec("CREATE TABLE IF NOT EXISTS `schema1`.`tbl` (`a` int, `b` varchar(24), `c` float, `d` datetime, primary key(`a`, `b`))")
	// create db connections refused.
	// TODO unit_test covers source.go
	_, _, err = NewSources(ctx, cfg)
	require.NoError(t, err)
}

func TestInitTables(t *testing.T) {
	ctx := context.Background()
	cfg := config.NewConfig()
	// Test case 1: test2.t2 will parse after filter.
	require.NoError(t, cfg.Parse([]string{"--config", "../config/config.toml"}))
	require.NoError(t, cfg.Init())

	conn, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer conn.Close()

	cfg.Task.TargetInstance.Conn = conn

	rows := sqlmock.NewRows([]string{"Database"}).AddRow("mysql").AddRow("test2")
	mock.ExpectQuery("SHOW DATABASES").WillReturnRows(rows)
	rows = sqlmock.NewRows([]string{"col1", "col2"}).AddRow("t1", "t1").AddRow("t2", "t2")
	mock.ExpectQuery("SHOW FULL TABLES*").WillReturnRows(rows)
	rows = sqlmock.NewRows([]string{"col1", "col2"}).AddRow("t2", "CREATE TABLE `t2` (\n\t\t\t`id` int(11) DEFAULT NULL,\n\t\t  \t`name` varchar(24) DEFAULT NULL\n\t\t\t) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin")
	mock.ExpectQuery("SHOW CREATE TABLE *").WillReturnRows(rows)
	rows = sqlmock.NewRows([]string{"col1", "col2"}).AddRow("", "")
	mock.ExpectQuery("SHOW VARIABLES LIKE*").WillReturnRows(rows)

	tablesToBeCheck, err := initTables(ctx, cfg)
	require.NoError(t, err)

	require.Len(t, tablesToBeCheck, 1)
	require.Equal(t, tablesToBeCheck[0].Schema, "test2")
	require.Equal(t, tablesToBeCheck[0].Table, "t2")
	// Range can be replaced during initTables
	require.Equal(t, tablesToBeCheck[0].Range, "age > 10 AND age < 20")

	require.NoError(t, mock.ExpectationsWereMet())

	// Test case 2: init failed due to conflict table config point to one table.
	cfg = config.NewConfig()
	require.NoError(t, cfg.Parse([]string{"--config", "../config/config_conflict.toml"}))
	require.NoError(t, cfg.Init())
	cfg.Task.TargetInstance.Conn = conn

	rows = sqlmock.NewRows([]string{"Database"}).AddRow("mysql").AddRow("test2")
	mock.ExpectQuery("SHOW DATABASES").WillReturnRows(rows)
	rows = sqlmock.NewRows([]string{"col1", "col2"}).AddRow("t1", "t1").AddRow("t2", "t2")
	mock.ExpectQuery("SHOW FULL TABLES*").WillReturnRows(rows)
	rows = sqlmock.NewRows([]string{"col1", "col2"}).AddRow("t2", "CREATE TABLE `t2` (\n\t\t\t`id` int(11) DEFAULT NULL,\n\t\t  \t`name` varchar(24) DEFAULT NULL\n\t\t\t) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin")
	mock.ExpectQuery("SHOW CREATE TABLE *").WillReturnRows(rows)
	rows = sqlmock.NewRows([]string{"col1", "col2"}).AddRow("", "")
	mock.ExpectQuery("SHOW VARIABLES LIKE*").WillReturnRows(rows)

	tablesToBeCheck, err = initTables(ctx, cfg)
	require.Contains(t, err.Error(), "different config matched to same target table")
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestCheckTableMatched(t *testing.T) {
	tmap := make(map[string]struct{})
	smap := make(map[string]struct{})

	tmap["1"] = struct{}{}
	tmap["2"] = struct{}{}

	smap["1"] = struct{}{}
	smap["2"] = struct{}{}
	require.NoError(t, checkTableMatched(tmap, smap))

	delete(smap, "1")
	require.Contains(t, checkTableMatched(tmap, smap).Error(), "the source has no table to be compared. target-table")

	delete(tmap, "1")
	smap["1"] = struct{}{}
	require.Contains(t, checkTableMatched(tmap, smap).Error(), "the target has no table to be compared. source-table")
}
