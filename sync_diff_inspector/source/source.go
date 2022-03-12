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
	"sort"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb-tools/pkg/dbutil"
	"github.com/pingcap/tidb-tools/pkg/filter"
	tableFilter "github.com/pingcap/tidb-tools/pkg/table-filter"
	router "github.com/pingcap/tidb-tools/pkg/table-router"
	"github.com/pingcap/tidb-tools/sync_diff_inspector/config"
	"github.com/pingcap/tidb-tools/sync_diff_inspector/source/common"
	"github.com/pingcap/tidb-tools/sync_diff_inspector/splitter"
	"github.com/pingcap/tidb-tools/sync_diff_inspector/utils"
	"github.com/pingcap/tidb/parser/model"
	"go.uber.org/zap"
)

type DMLType int32

const (
	Insert DMLType = iota + 1
	Delete
	Replace
)

const UnifiedTimeZone string = "+0:00"

type ChecksumInfo struct {
	Checksum int64
	Count    int64
	Err      error
	Cost     time.Duration
}

// RowDataIterator represents the row data in source.
type RowDataIterator interface {
	// Next seeks the next row data, it used when compared rows.
	Next() (map[string]*dbutil.ColumnData, error)
	// Close release the resource.
	Close()
}

// TableAnalyzer represents the method in different source.
// each source has its own analyze function.
type TableAnalyzer interface {
	// AnalyzeSplitter picks the proper splitter.ChunkIterator according to table and source.
	AnalyzeSplitter(context.Context, *common.TableDiff, *splitter.RangeInfo) (splitter.ChunkIterator, error)
}

type Source interface {
	// GetTableAnalyzer pick the proper analyzer for different source.
	// the implement of this function is different in mysql/tidb.
	GetTableAnalyzer() TableAnalyzer

	// GetRangeIterator generates the range iterator with the checkpoint(*splitter.RangeInfo) and analyzer.
	// this is the mainly iterator across the whole sync diff.
	// One source has one range iterator to produce the range to channel.
	// there are many workers consume the range from the channel to compare.
	GetRangeIterator(context.Context, *splitter.RangeInfo, TableAnalyzer) (RangeIterator, error)

	// GetCountAndCrc32 gets the crc32 result and the count from given range.
	GetCountAndCrc32(context.Context, *splitter.RangeInfo) *ChecksumInfo

	// GetRowsIterator gets the row data iterator from given range.
	GetRowsIterator(context.Context, *splitter.RangeInfo) (RowDataIterator, error)

	// GenerateFixSQL generates the fix sql with given type.
	GenerateFixSQL(DMLType, map[string]*dbutil.ColumnData, map[string]*dbutil.ColumnData, int) string

	// GetTables represents the tableDiffs.
	GetTables() []*common.TableDiff

	// GetSourceStructInfo get the source table info from a given target table
	GetSourceStructInfo(context.Context, int) ([]*model.TableInfo, error)

	// GetDB represents the db connection.
	GetDB() *sql.DB

	// GetSnapshot represents the snapshot of source.
	// only TiDB source has the snapshot.
	// TODO refine the interface.
	GetSnapshot() string

	// Close ...
	Close()
}

func NewSources(ctx context.Context, cfg *config.Config) (downstream Source, upstream Source, err error) {
	// init db connection for upstream / downstream.
	err = initDBConn(ctx, cfg)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	tablesToBeCheck, err := initTables(ctx, cfg)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}

	tableDiffs := make([]*common.TableDiff, 0, len(tablesToBeCheck))
	for _, tableConfig := range tablesToBeCheck {
		newInfo, needUnifiedTimeZone := utils.ResetColumns(tableConfig.TargetTableInfo, tableConfig.IgnoreColumns)
		tableDiffs = append(tableDiffs, &common.TableDiff{
			Schema: tableConfig.Schema,
			Table:  tableConfig.Table,
			Info:   newInfo,
			// TODO: field `IgnoreColumns` can be deleted.
			IgnoreColumns:       tableConfig.IgnoreColumns,
			Fields:              strings.Join(tableConfig.Fields, ","),
			Range:               tableConfig.Range,
			NeedUnifiedTimeZone: needUnifiedTimeZone,
			Collation:           tableConfig.Collation,
			ChunkSize:           tableConfig.ChunkSize,
		})

		// When the router set case-sensitive false,
		// that add rule match itself will make table case unsensitive.
		for _, d := range cfg.Task.SourceInstances {
			if d.Router.AddRule(&router.TableRule{
				SchemaPattern: tableConfig.Schema,
				TablePattern:  tableConfig.Table,
				TargetSchema:  tableConfig.Schema,
				TargetTable:   tableConfig.Table,
			}) != nil {
				return nil, nil, errors.Errorf("set case unsensitive failed. The schema/table name cannot be parttern. [schema = %s] [table = %s]", tableConfig.Schema, tableConfig.Table)
			}
		}
	}

	if len(tableDiffs) == 0 {
		return nil, nil, errors.Errorf("no table need to be compared")
	}

	// Sort TableDiff is important!
	// because we compare table one by one.
	sort.Slice(tableDiffs, func(i, j int) bool {
		ti := utils.UniqueID(tableDiffs[i].Schema, tableDiffs[i].Table)
		tj := utils.UniqueID(tableDiffs[j].Schema, tableDiffs[j].Table)
		return strings.Compare(ti, tj) > 0
	})
	upstream, err = buildSourceFromCfg(ctx, tableDiffs, cfg.CheckThreadCount, cfg.Task.TargetCheckTables, cfg.Task.SourceInstances...)
	if err != nil {
		return nil, nil, errors.Annotate(err, "from upstream")
	}
	downstream, err = buildSourceFromCfg(ctx, tableDiffs, cfg.CheckThreadCount, cfg.Task.TargetCheckTables, cfg.Task.TargetInstance)
	if err != nil {
		return nil, nil, errors.Annotate(err, "from downstream")
	}
	return downstream, upstream, nil
}

func buildSourceFromCfg(ctx context.Context, tableDiffs []*common.TableDiff, checkThreadCount int, f tableFilter.Filter, dbs ...*config.DataSource) (Source, error) {
	if len(dbs) < 1 {
		return nil, errors.Errorf("no db config detected")
	}
	ok, err := dbutil.IsTiDB(ctx, dbs[0].Conn)
	if err != nil {
		return nil, errors.Annotatef(err, "connect to db failed")
	}

	if ok {
		if len(dbs) == 1 {
			return NewTiDBSource(ctx, tableDiffs, dbs[0], checkThreadCount, f)
		} else {
			log.Fatal("Don't support check table in multiple tidb instance, please specify one tidb instance.")
		}
	}
	return NewMySQLSources(ctx, tableDiffs, dbs, checkThreadCount, f)
}

func initDBConn(ctx context.Context, cfg *config.Config) error {
	// Unified time zone
	vars := map[string]string{
		"time_zone": UnifiedTimeZone,
	}
	// we had 3 producers and `cfg.CheckThreadCount` consumer to use db connections.
	// so the connection count need to be cfg.CheckThreadCount + 3.
	targetConn, err := common.CreateDB(ctx, cfg.Task.TargetInstance.ToDBConfig(), vars, cfg.CheckThreadCount+3)
	if err != nil {
		return errors.Trace(err)
	}

	cfg.Task.TargetInstance.Conn = targetConn

	for _, source := range cfg.Task.SourceInstances {
		// connect source db with target db time_zone
		conn, err := common.CreateDB(ctx, source.ToDBConfig(), vars, cfg.CheckThreadCount+1)
		if err != nil {
			return errors.Trace(err)
		}
		source.Conn = conn
	}
	return nil
}

func initTables(ctx context.Context, cfg *config.Config) (cfgTables []*config.TableConfig, err error) {
	downStreamConn := cfg.Task.TargetInstance.Conn
	TargetTablesList := make([]*common.TableSource, 0)
	targetSchemas, err := dbutil.GetSchemas(ctx, downStreamConn)
	if err != nil {
		return nil, errors.Annotatef(err, "get schemas from target source")
	}

	for _, schema := range targetSchemas {
		if filter.IsSystemSchema(schema) {
			continue
		}
		allTables, err := dbutil.GetTables(ctx, downStreamConn, schema)
		if err != nil {
			return nil, errors.Annotatef(err, "get tables from target source %s", schema)
		}
		for _, t := range allTables {
			TargetTablesList = append(TargetTablesList, &common.TableSource{
				OriginSchema: schema,
				OriginTable:  t,
			})
		}
	}

	// fill the table information.
	// will add default source information, don't worry, we will use table config's info replace this later.
	// cfg.Tables.Schema => cfg.Tables.Tables => target/source Schema.Table
	cfgTables = make([]*config.TableConfig, 0, len(TargetTablesList))
	for _, tables := range TargetTablesList {
		if cfg.Task.TargetCheckTables.MatchTable(tables.OriginSchema, tables.OriginTable) {
			log.Debug("match target table", zap.String("table", dbutil.TableName(tables.OriginSchema, tables.OriginTable)))
			tableInfo, err := dbutil.GetTableInfo(ctx, downStreamConn, tables.OriginSchema, tables.OriginTable)
			if err != nil {
				return nil, errors.Errorf("get table %s.%s's information error %s", tables.OriginSchema, tables.OriginTable, errors.ErrorStack(err))
			}
			// Initialize all the tables that matches the `target-check-tables`[config.toml] and appears in downstream.
			cfgTables = append(cfgTables, &config.TableConfig{
				Schema:          tables.OriginSchema,
				Table:           tables.OriginTable,
				TargetTableInfo: tableInfo,
				Range:           "TRUE",
			})
		}
	}

	// Reset fields of some tables of `cfgTables` according to `table-configs`[config.toml].
	// The table in `table-configs`[config.toml] should exist in both `target-check-tables`[config.toml] and tables from downstream.
	for i, table := range cfg.Task.TargetTableConfigs {
		// parse every config to find target table.
		cfgFilter, err := tableFilter.Parse(table.TargetTables)
		if err != nil {
			return nil, errors.Errorf("unable to parse target table for the %dth config", i)
		}
		// iterate all target tables to make sure
		// 1. one table only match at most one config.
		// 2. config can miss table.
		for _, cfgTable := range cfgTables {
			if cfgFilter.MatchTable(cfgTable.Schema, cfgTable.Table) {
				if cfgTable.HasMatched {
					return nil, errors.Errorf("different config matched to same target table %s.%s", cfgTable.Schema, cfgTable.Table)
				}
				if table.Range != "" {
					cfgTable.Range = table.Range
				}
				cfgTable.IgnoreColumns = table.IgnoreColumns
				cfgTable.Fields = table.Fields
				cfgTable.Collation = table.Collation
				cfgTable.ChunkSize = table.ChunkSize
				cfgTable.HasMatched = true
			}
		}
	}
	return cfgTables, nil
}

// RangeIterator generate next chunk for the whole tables lazily.
type RangeIterator interface {
	// Next seeks the next chunk, return nil if seeks to end.
	Next(context.Context) (*splitter.RangeInfo, error)

	Close()
}

func checkTableMatched(targetMap map[string]struct{}, sourceMap map[string]struct{}) error {
	// check target exists but source not found
	for tableDiff := range targetMap {
		// target table have all passed in tableFilter
		if _, ok := sourceMap[tableDiff]; !ok {
			return errors.Errorf("the source has no table to be compared. target-table is `%s`", tableDiff)
		}
	}
	// check source exists but target not found
	for tableDiff := range sourceMap {
		// need check source table have passd in tableFilter here
		if _, ok := targetMap[tableDiff]; !ok {
			return errors.Errorf("the target has no table to be compared. source-table is `%s`", tableDiff)
		}
	}
	log.Info("table match check passed!!")
	return nil
}
