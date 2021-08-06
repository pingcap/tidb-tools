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
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb-tools/pkg/dbutil"
	router "github.com/pingcap/tidb-tools/pkg/table-router"
	"github.com/pingcap/tidb-tools/sync_diff_inspector/config"
	"github.com/pingcap/tidb-tools/sync_diff_inspector/source/common"
	"github.com/pingcap/tidb-tools/sync_diff_inspector/splitter"
	"github.com/pingcap/tidb-tools/sync_diff_inspector/utils"
	"go.uber.org/zap"
)

type DMLType int32

const (
	Insert DMLType = iota + 1
	Delete
	Replace
)

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
	GetRangeIterator(*splitter.RangeInfo, TableAnalyzer) (RangeIterator, error)

	// GetCountAndCrc32 gets the crc32 result and the count from given range.
	GetCountAndCrc32(*splitter.RangeInfo, chan *ChecksumInfo)

	// GetRowsIterator gets the row data iterator from given range.
	GetRowsIterator(*splitter.RangeInfo) (RowDataIterator, error)

	// GenerateFixSQL generates the fix sql with given type.
	GenerateFixSQL(DMLType, map[string]*dbutil.ColumnData, int) string

	// GetTable represents the tableDiff of given index.
	GetTable(int) *common.TableDiff
	GetTables() []*common.TableDiff

	// GetDB represents the db connection.
	GetDB() *sql.DB
	// Close ...
	Close()
}

func NewSources(ctx context.Context, cfg *config.Config) (downstream Source, upstream Source, err error) {
	tablesToBeCheck, tableMaps, err := initTables(ctx, cfg)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}

	// init db connection for upstream / downstream.
	err = initDBConn(ctx, cfg)

	tableDiffs := make([]*common.TableDiff, 0, len(tablesToBeCheck))
	for _, tables := range tablesToBeCheck {
		for _, tableConfig := range tables {
			tableRowsQuery, tableOrderKeyCols := utils.GetTableRowsQueryFormat(tableConfig.Schema, tableConfig.Table, tableConfig.TargetTableInfo, tableConfig.Collation)
			tableDiffs = append(tableDiffs, &common.TableDiff{
				InstanceID: tableConfig.InstanceID,
				Schema:     tableConfig.Schema,
				Table:      tableConfig.Table,
				Info:       utils.IgnoreColumns(tableConfig.TargetTableInfo, tableConfig.IgnoreColumns),
				// TODO: field `IgnoreColumns` can be deleted.
				IgnoreColumns:     tableConfig.IgnoreColumns,
				Fields:            tableConfig.Fields,
				Range:             tableConfig.Range,
				Collation:         tableConfig.Collation,
				TableOrderKeyCols: tableOrderKeyCols,
				TableRowsQuery:    tableRowsQuery,
				TableMaps:         tableMaps,
			})
		}
	}

	upstream, err = buildSourceFromCfg(ctx, tableDiffs, cfg.SourceDBCfg...)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}

	downstream, err = buildSourceFromCfg(ctx, tableDiffs, cfg.TargetDBCfg)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	return upstream, downstream, nil
}

func buildSourceFromCfg(ctx context.Context, tableDiffs []*common.TableDiff, dbs ...*config.DBConfig) (Source, error) {
	if len(dbs) < 1 {
		return nil, errors.Errorf("no db config detected")
	}
	ok, err := dbutil.IsTiDB(ctx, dbs[0].Conn)
	if err != nil {
		return nil, errors.Annotatef(err, "connect to db failed")
	}

	if len(dbs) == 1 {
		if ok {
			// TiDB
			return NewTiDBSource(ctx, tableDiffs, dbs[0].Conn)
		} else {
			// Single Mysql
			return NewMySQLSource(ctx, tableDiffs, dbs[0].Conn)
		}
	} else {
		if ok {
			// TiDB
			log.Fatal("Don't support check table in multiple tidb instance, please specify one tidb instance.")
		} else {
			return NewMySQLSources(ctx, tableDiffs, dbs)
		}
	}
	// unreachable
	return nil, nil
}

func initDBConn(ctx context.Context, cfg *config.Config) error {
	var err error
	cfg.TargetDBCfg.Conn, err = common.CreateDB(ctx, &cfg.TargetDBCfg.DBConfig, nil, cfg.CheckThreadCount)
	if err != nil {
		return errors.Trace(err)
	}

	targetTZOffset, err := dbutil.GetTimeZoneOffset(ctx, cfg.TargetDBCfg.Conn)
	if err != nil {
		return errors.Annotatef(err, "fetch target db %s time zone offset failed", cfg.TargetDBCfg.DBConfig.String())
	}
	vars := map[string]string{
		"time_zone": dbutil.FormatTimeZoneOffset(targetTZOffset),
	}

	// upstream
	if len(cfg.SourceDBCfg) < 1 {
		return errors.New(" source config")
	}

	for _, source := range cfg.SourceDBCfg {
		// connect source db with target db time_zone
		source.Conn, err = common.CreateDB(ctx, &source.DBConfig, vars, cfg.CheckThreadCount)
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func initTables(ctx context.Context, cfg *config.Config) (cfgTables map[string]map[string]*config.TableConfig, tableMaps map[config.TableInstance][]config.TableInstance, err error) {
	tableRouter, err := router.NewTableRouter(false, cfg.TableRules)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}

	allTablesMap, err := utils.GetAllTables(ctx, cfg)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	tableMaps = make(map[config.TableInstance][]config.TableInstance)
	// get all source table's matched target table
	// target database name => target table name => all matched source table instance
	sourceTablesMap := make(map[string]map[string][]config.TableInstance)
	for instanceID, allSchemas := range allTablesMap {
		if instanceID == cfg.TargetDBCfg.InstanceID {
			for schema, allTables := range allSchemas {
				for table := range allTables {
					tableMaps[config.TableInstance{InstanceID: instanceID, Schema: schema, Table: table}] = make([]config.TableInstance, 0)
				}
			}
			continue
		}

		for schema, allTables := range allSchemas {
			for table := range allTables {
				targetSchema, targetTable, err := tableRouter.Route(schema, table)
				if err != nil {
					return nil, nil, errors.Errorf("get route result for %s.%s.%s failed, error %v", instanceID, schema, table, err)
				}
				for targetInstance := range tableMaps {
					if targetSchema == targetInstance.Schema && targetTable == targetInstance.Table {
						tableMaps[targetInstance] = append(tableMaps[targetInstance], config.TableInstance{
							InstanceID: instanceID,
							Schema:     schema,
							Table:      table,
						})
						break
					}
				}
				if _, ok := sourceTablesMap[targetSchema]; !ok {
					sourceTablesMap[targetSchema] = make(map[string][]config.TableInstance)
				}

				if _, ok := sourceTablesMap[targetSchema][targetTable]; !ok {
					sourceTablesMap[targetSchema][targetTable] = make([]config.TableInstance, 0, 1)
				}

				sourceTablesMap[targetSchema][targetTable] = append(sourceTablesMap[targetSchema][targetTable], config.TableInstance{
					InstanceID: instanceID,
					Schema:     schema,
					Table:      table,
				})
			}
		}
	}

	// fill the table information.
	// will add default source information, don't worry, we will use table config's info replace this later.
	// cfg.Tables.Schema => cfg.Tables.Tables => target/source Schema.Table
	cfgTables = make(map[string]map[string]*config.TableConfig)
	for _, schemaTables := range cfg.Tables {
		cfgTables[schemaTables.Schema] = make(map[string]*config.TableConfig)
		tables := make([]string, 0, len(schemaTables.Tables))
		allTables, ok := allTablesMap[cfg.TargetDBCfg.InstanceID][schemaTables.Schema]
		if !ok {
			return nil, nil, errors.NotFoundf("schema %s.%s", cfg.TargetDBCfg.InstanceID, schemaTables.Schema)
		}

		for _, table := range schemaTables.Tables {
			matchedTables, err := utils.GetMatchTable(cfg.TargetDBCfg, schemaTables.Schema, table, allTables)
			if err != nil {
				return nil, nil, errors.Trace(err)
			}

			//exclude those in "exclude-tables"
			for _, t := range matchedTables {
				if utils.InExcludeTables(schemaTables.ExcludeTables, t) {
					continue
				} else {
					tables = append(tables, t)
				}
			}
		}

		for _, tableName := range tables {
			tableInfo, err := dbutil.GetTableInfo(ctx, cfg.TargetDBCfg.Conn, schemaTables.Schema, tableName)
			if err != nil {
				return nil, nil, errors.Errorf("get table %s.%s's information error %s", schemaTables.Schema, tableName, errors.ErrorStack(err))
			}

			if _, ok := cfgTables[schemaTables.Schema][tableName]; ok {
				log.Error("duplicate config for one table", zap.String("table", dbutil.TableName(schemaTables.Schema, tableName)))
				continue
			}

			sourceTables := make([]config.TableInstance, 0, 1)
			if _, ok := sourceTablesMap[schemaTables.Schema][tableName]; ok {
				log.Info("find matched source tables", zap.Reflect("source tables", sourceTablesMap[schemaTables.Schema][tableName]), zap.String("target schema", schemaTables.Schema), zap.String("table", tableName))
				sourceTables = sourceTablesMap[schemaTables.Schema][tableName]
			} else {
				// use same database name and table name
				sourceTables = append(sourceTables, config.TableInstance{
					InstanceID: cfg.SourceDBCfg[0].InstanceID,
					Schema:     schemaTables.Schema,
					Table:      tableName,
				})
			}

			cfgTables[schemaTables.Schema][tableName] = &config.TableConfig{
				TableInstance: config.TableInstance{
					Schema: schemaTables.Schema,
					Table:  tableName,
				},
				TargetTableInfo: tableInfo,
				Range:           "TRUE",
				SourceTables:    sourceTables,
			}
		}
	}

	for _, table := range cfg.TableCfgs {
		if _, ok := cfgTables[table.Schema]; !ok {
			return nil, nil, errors.NotFoundf("schema %s in check tables", table.Schema)
		}
		if _, ok := cfgTables[table.Schema][table.Table]; !ok {
			return nil, nil, errors.NotFoundf("table %s.%s in check tables", table.Schema, table.Table)
		}

		if table.Range != "" {
			cfgTables[table.Schema][table.Table].Range = table.Range
		}
		cfgTables[table.Schema][table.Table].IgnoreColumns = table.IgnoreColumns
		cfgTables[table.Schema][table.Table].Fields = table.Fields
		cfgTables[table.Schema][table.Table].Collation = table.Collation
	}
	return cfgTables, tableMaps, nil
}

// RangeIterator generate next chunk for the whole tables lazily.
type RangeIterator interface {
	// Next seeks the next chunk, return nil if seeks to end.
	Next() (*splitter.RangeInfo, error)

	Close()
}

type BasicRowsIterator struct {
	rows *sql.Rows
}

func (s *BasicRowsIterator) Close() {
	s.rows.Close()
}

func (s *BasicRowsIterator) Next() (map[string]*dbutil.ColumnData, error) {
	if s.rows.Next() {
		return dbutil.ScanRow(s.rows)
	}
	return nil, nil
}
