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
	"testing"
	"time"

	"github.com/pingcap/parser"
	"github.com/pingcap/tidb-tools/pkg/dbutil"
	filter "github.com/pingcap/tidb-tools/pkg/table-filter"
	router "github.com/pingcap/tidb-tools/pkg/table-router"
	"github.com/pingcap/tidb-tools/sync_diff_inspector/config"

	"github.com/DATA-DOG/go-sqlmock"
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb-tools/sync_diff_inspector/chunk"
	"github.com/pingcap/tidb-tools/sync_diff_inspector/source/common"
	"github.com/pingcap/tidb-tools/sync_diff_inspector/splitter"
)

func TestClient(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testSourceSuite{})

type testSourceSuite struct{}

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
	i         int
}

const MAXCHUNKS = 5

func (m *MockChunkIterator) Next() (*chunk.Range, error) {
	if m.i == MAXCHUNKS {
		return nil, nil
	}
	m.i = m.i + 1
	return &chunk.Range{
		ID: m.i,
	}, nil
}

func (m *MockChunkIterator) Close() {

}

type MockAnalyzer struct {
}

func (m *MockAnalyzer) AnalyzeSplitter(ctx context.Context, progressID string, tableDiff *common.TableDiff, rangeInfo *splitter.RangeInfo) (splitter.ChunkIterator, error) {
	i := 0
	return &MockChunkIterator{
		ctx,
		tableDiff,
		rangeInfo,
		i,
	}, nil
}

func (s *testSourceSuite) TestTiDBSource(c *C) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conn, mock, err := sqlmock.New()
	c.Assert(err, IsNil)
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

	tableDiffs := prepareTiDBTables(c, tableCases)

	tidb, err := NewTiDBSource(ctx, tableDiffs, &config.DataSource{Conn: conn})
	c.Assert(err, IsNil)

	for n, tableCase := range tableCases {
		c.Assert(n, Equals, tableCase.rangeInfo.TableIndex)
		countRows := sqlmock.NewRows([]string{"CNT", "CHECKSUM"}).AddRow(123, 456)
		mock.ExpectQuery("SELECT COUNT.*").WillReturnRows(countRows)
		checksum := tidb.GetCountAndCrc32(ctx, tableCase.rangeInfo)
		c.Assert(checksum.Err, IsNil)
		c.Assert(checksum.Count, Equals, int64(123))
		c.Assert(checksum.Checksum, Equals, int64(456))
		//c.Assert(checksum, Equals, tableCase.checksum)
	}

	// Test ChunkIterator
	iter, err := tidb.GetRangeIterator(ctx, tableCases[0].rangeInfo, &MockAnalyzer{})
	c.Assert(err, IsNil)
	i := 0
	for {
		chunk, err := iter.Next(ctx)
		c.Assert(err, IsNil)
		if chunk == nil {
			c.Assert(i, Equals, 5*len(tableCases))
			break
		}
		c.Assert(chunk.ChunkRange.ID, Equals, i+1)
		i++
	}
	iter.Close()

	// Test RowIterator
	tableCase := tableCases[0]
	dataRows := sqlmock.NewRows(tableCase.rowColumns)
	for _, row := range tableCase.rows {
		dataRows.AddRow(row...)
	}
	mock.ExpectQuery(tableCase.rowQuery).WillReturnRows(dataRows)
	rowIter, err := tidb.GetRowsIterator(ctx, tableCase.rangeInfo)
	c.Assert(err, IsNil)

	i = 0
	var firstRow, secondRow map[string]*dbutil.ColumnData
	for {
		columns, err := rowIter.Next()
		c.Assert(err, IsNil)
		if columns == nil {
			c.Assert(i, Equals, len(tableCase.rows))
			break
		}
		for j, value := range tableCase.rows[i] {
			c.Assert(columns[tableCase.rowColumns[j]].IsNull, Equals, false)
			c.Assert(columns[tableCase.rowColumns[j]].Data, DeepEquals, []byte(value.(string)))
		}
		if i == 0 {
			firstRow = columns
		} else if i == 1 {
			secondRow = columns
		}
		i++
	}
	c.Assert(tidb.GenerateFixSQL(Insert, firstRow, secondRow, 0), Equals, "REPLACE INTO `source_test`.`test1`(`a`,`b`,`c`) VALUES (1,'a',1.2);")
	c.Assert(tidb.GenerateFixSQL(Delete, firstRow, secondRow, 0), Equals, "DELETE FROM `source_test`.`test1` WHERE `a` = 2 AND `b` = 'b' AND `c` = 3.4;")
	c.Assert(tidb.GenerateFixSQL(Replace, firstRow, secondRow, 0), Equals, "-- diff column\t|\t`a`\t|\t`b`\t|\t`c`\n"+
		"-- source data\t|\t1\t|\t'a'\t|\t1.2\n"+
		"-- target data\t|\t2\t|\t'b'\t|\t3.4\n"+
		"REPLACE INTO `source_test`.`test1`(`a`,`b`,`c`) VALUES (1,'a',1.2);")

	rowIter.Close()

	analyze := tidb.GetTableAnalyzer()
	countRows := sqlmock.NewRows([]string{"Cnt"}).AddRow(0)
	mock.ExpectQuery("SELECT COUNT.*").WillReturnRows(countRows)
	chunkIter, err := analyze.AnalyzeSplitter(ctx, "", tableDiffs[0], tableCase.rangeInfo)
	c.Assert(err, IsNil)
	chunkIter.Close()
	tidb.Close()
}

func (s *testSourceSuite) TestMysqlShardSources(c *C) {
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

	tableDiffs := prepareTiDBTables(c, tableCases)

	conn, mock, err := sqlmock.New()
	c.Assert(err, IsNil)
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

	shard, err := NewMySQLSources(ctx, tableDiffs, cs, 4)
	c.Assert(err, IsNil)

	for i := 0; i < len(dbs); i++ {
		infoRows := sqlmock.NewRows([]string{"Table", "Create Table"}).AddRow("test_t", "CREATE TABLE `source_test`.`test1` (`a` int, `b` varchar(24), `c` float, primary key(`a`, `b`))")
		variableRows := sqlmock.NewRows([]string{"Variable_name", "Value"}).AddRow("sql_mode", "ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION")

		mock.ExpectQuery("SHOW CREATE TABLE.*").WillReturnRows(infoRows)
		mock.ExpectQuery("SHOW VARIABLE.*").WillReturnRows(variableRows)
	}
	info, err := shard.GetSourceStructInfo(ctx, 0)
	c.Assert(err, IsNil)
	c.Assert(info[0].Name.O, Equals, "test1")

	for n, tableCase := range tableCases {
		c.Assert(n, Equals, tableCase.rangeInfo.TableIndex)
		var resChecksum int64 = 0
		for i := 0; i < len(dbs); i++ {
			resChecksum = resChecksum + 1<<i
			countRows := sqlmock.NewRows([]string{"CNT", "CHECKSUM"}).AddRow(1, 1<<i)
			mock.ExpectQuery("SELECT COUNT.*").WillReturnRows(countRows)
		}

		checksum := shard.GetCountAndCrc32(ctx, tableCase.rangeInfo)
		c.Assert(checksum.Err, IsNil)
		c.Assert(checksum.Count, Equals, int64(len(dbs)))
		c.Assert(checksum.Checksum, Equals, resChecksum)
		//c.Assert(checksum, Equals, tableCase.checksum)
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
		c.Log(dataRows)
		mock.ExpectQuery(tableCase.rowQuery).WillReturnRows(dataRows)
	}

	rowIter, err := shard.GetRowsIterator(ctx, tableCase.rangeInfo)
	c.Assert(err, IsNil)

	i = 0
	for {
		columns, err := rowIter.Next()
		c.Assert(err, IsNil)
		if columns == nil {
			c.Assert(i, Equals, len(tableCase.rows))
			break
		}
		c.Log(i)
		for j, value := range tableCase.rows[i] {
			//c.Log(j)
			c.Assert(columns[tableCase.rowColumns[j]].IsNull, Equals, false)
			c.Assert(columns[tableCase.rowColumns[j]].Data, DeepEquals, []byte(value.(string)))
		}

		i++
	}
	rowIter.Close()

	shard.Close()
}

func (s *testSourceSuite) TestMysqlRouter(c *C) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conn, mock, err := sqlmock.New()
	c.Assert(err, IsNil)
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

	tableDiffs := prepareTiDBTables(c, tableCases)

	routeRuleList := []*router.TableRule{
		{
			SchemaPattern: "source_test_t",
			TablePattern:  "test_t",
			TargetSchema:  "source_test",
			TargetTable:   "test1",
		},
	}
	router, err := router.NewTableRouter(false, routeRuleList)
	c.Assert(err, IsNil)
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
	mysql, err := NewMySQLSources(ctx, tableDiffs, []*config.DataSource{ds}, 4)
	c.Assert(err, IsNil)

	// random splitter
	countRows := sqlmock.NewRows([]string{"Cnt"}).AddRow(0)
	mock.ExpectQuery("SELECT COUNT.*").WillReturnRows(countRows)
	rangeIter, err := mysql.GetRangeIterator(ctx, tableCases[0].rangeInfo, mysql.GetTableAnalyzer())
	c.Assert(err, IsNil)
	rangeIter.Close()

	// row Iterator
	dataRows := sqlmock.NewRows(tableCases[0].rowColumns)
	for k := 0; k < 2; k++ {
		dataRows.AddRow(tableCases[0].rows[k]...)
	}
	c.Log(dataRows)
	mock.ExpectQuery(tableCases[0].rowQuery).WillReturnRows(dataRows)

	rowIter, err := mysql.GetRowsIterator(ctx, tableCases[0].rangeInfo)
	c.Assert(err, IsNil)
	firstRow, err := rowIter.Next()
	c.Assert(err, IsNil)
	c.Assert(firstRow, NotNil)
	secondRow, err := rowIter.Next()
	c.Assert(err, IsNil)
	c.Assert(secondRow, NotNil)
	c.Assert(mysql.GenerateFixSQL(Insert, firstRow, secondRow, 0), Equals, "REPLACE INTO `source_test`.`test1`(`a`,`b`,`c`) VALUES (1,'a',1.2);")
	c.Assert(mysql.GenerateFixSQL(Delete, firstRow, secondRow, 0), Equals, "DELETE FROM `source_test`.`test1` WHERE `a` = 2 AND `b` = 'b' AND `c` = 3.4;")
	c.Assert(mysql.GenerateFixSQL(Replace, firstRow, secondRow, 0), Equals, "-- diff column\t|\t`a`\t|\t`b`\t|\t`c`\n"+
		"-- source data\t|\t1\t|\t'a'\t|\t1.2\n"+
		"-- target data\t|\t2\t|\t'b'\t|\t3.4\n"+
		"REPLACE INTO `source_test`.`test1`(`a`,`b`,`c`) VALUES (1,'a',1.2);")
	rowIter.Close()

	mysql.Close()
}

func (s *testSourceSuite) TestTiDBRouter(c *C) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conn, mock, err := sqlmock.New()
	c.Assert(err, IsNil)
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

	tableDiffs := prepareTiDBTables(c, tableCases)

	routeRuleList := []*router.TableRule{
		{
			SchemaPattern: "source_test_t",
			TablePattern:  "test_t",
			TargetSchema:  "source_test",
			TargetTable:   "test1",
		},
	}
	router, err := router.NewTableRouter(false, routeRuleList)
	c.Assert(err, IsNil)
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
	tidb, err := NewTiDBSource(ctx, tableDiffs, ds)
	c.Assert(err, IsNil)
	infoRows := sqlmock.NewRows([]string{"Table", "Create Table"}).AddRow("test_t", "CREATE TABLE `source_test`.`test1` (`a` int, `b` varchar(24), `c` float, primary key(`a`, `b`))")
	mock.ExpectQuery("SHOW CREATE TABLE.*").WillReturnRows(infoRows)
	variableRows := sqlmock.NewRows([]string{"Variable_name", "Value"}).AddRow("sql_mode", "ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION")
	mock.ExpectQuery("SHOW VARIABLE.*").WillReturnRows(variableRows)
	info, err := tidb.GetSourceStructInfo(ctx, 0)
	c.Assert(err, IsNil)
	c.Assert(info[0].Name.O, Equals, "test1")
}

func prepareTiDBTables(c *C, tableCases []*tableCaseType) []*common.TableDiff {
	tableDiffs := make([]*common.TableDiff, 0, len(tableCases))
	for n, tableCase := range tableCases {
		tableInfo, err := dbutil.GetTableInfoBySQL(tableCase.createTableSQL, parser.New())
		c.Assert(err, IsNil)
		tableDiffs = append(tableDiffs, &common.TableDiff{
			Schema: "source_test",
			Table:  fmt.Sprintf("test%d", n+1),
			Info:   tableInfo,
		})

		chunkRange := chunk.NewChunkRange()
		for i, column := range tableCase.rangeColumns {
			chunkRange.Update(column, tableCase.rangeLeft[i], tableCase.rangeRight[i], true, true)
		}

		chunk.InitChunk(chunkRange, chunk.Bucket, 0, "", "")
		rangeInfo := &splitter.RangeInfo{
			ChunkRange: chunkRange,
			TableIndex: n,
		}
		tableCase.rangeInfo = rangeInfo
	}

	return tableDiffs
}

func (s *testSourceSuite) TestSource(c *C) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conn, _, err := sqlmock.New()
	c.Assert(err, IsNil)
	defer conn.Close()

	cfg := &config.Config{
		LogLevel:          "info",
		Sample:            100,
		CheckThreadCount:  4,
		UseChecksum:       false,
		IgnoreStructCheck: false,
		IgnoreStats:       false,
		IgnoreDataCheck:   false,
		UseCheckpoint:     true,
		DataSources: map[string]*config.DataSource{
			"mysql1": {
				Conn: conn,
			},
			"tidb": {
				Conn: conn,
			},
		},
		Routes: nil,
		TableConfigs: map[string]*config.TableConfig{
			"config1": {
				Schema:          "schama1",
				Table:           "table",
				IgnoreColumns:   []string{"", ""},
				Fields:          "",
				Range:           "age > 10 AND age < 20",
				IsSharding:      false,
				TargetTableInfo: nil,
				Collation:       "",
			},
		},
		Task: config.TaskConfig{
			Source:       []string{"mysql1"},
			Routes:       nil,
			Target:       []string{"tidb"},
			CheckTables:  []string{"schema*.table*", "!c.*", "test2.t2"},
			TableConfigs: []string{"config1"},
			OutputDir:    "./output",
			SourceInstances: []*config.DataSource{
				{
					Host:       "127.0.0.1",
					Port:       4567,
					User:       "root",
					Password:   "",
					SqlMode:    "",
					Snapshot:   "",
					RouteRules: nil,
					Router:     nil,
					Conn:       nil,
				},
			},
			TargetInstance: &config.DataSource{
				Host:       "127.0.0.1",
				Port:       4567,
				User:       "root",
				Password:   "",
				SqlMode:    "",
				Snapshot:   "",
				RouteRules: nil,
				Router:     nil,
				Conn:       nil,
			},
			TargetTableConfigs: []*config.TableConfig{
				{
					Schema:          "schama1",
					Table:           "table",
					IgnoreColumns:   []string{"", ""},
					Fields:          "",
					Range:           "age > 10 AND age < 20",
					IsSharding:      false,
					TargetTableInfo: nil,
					Collation:       "",
				},
			},
			TargetCheckTables: nil,
			FixDir:            "output/e44ad7682cf25cc16041996127350c23/fix-on-tidb",
			CheckpointDir:     "output/e44ad7682cf25cc16041996127350c23/checkpoint",
			HashFile:          "",
		},
		ConfigFile:   "config.toml",
		PrintVersion: false,
	}
	cfg.Task.TargetCheckTables, err = filter.Parse([]string{"schema*.table*", "!c.*", "test2.t2"})
	c.Assert(err, IsNil)
	// create db connections refused.
	// TODO unit_test covers source.go
	_, _, err = NewSources(ctx, cfg)
	c.Assert(err, NotNil)
}
