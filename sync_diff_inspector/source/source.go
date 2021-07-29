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
	"strconv"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb-tools/pkg/dbutil"
	router "github.com/pingcap/tidb-tools/pkg/table-router"
	"github.com/pingcap/tidb-tools/sync_diff_inspector/checkpoints"
	"github.com/pingcap/tidb-tools/sync_diff_inspector/chunk"
	"github.com/pingcap/tidb-tools/sync_diff_inspector/config"
	"github.com/pingcap/tidb-tools/sync_diff_inspector/source/common"
	"github.com/pingcap/tidb-tools/sync_diff_inspector/utils"
	"github.com/pingcap/tidb/types"
	"go.uber.org/zap"
)

type DMLType int32
type SourceSide bool

const (
	Upstream   SourceSide = true
	Downstream            = false
)
const (
	Insert DMLType = iota + 1
	Delete
	Replace
)

type RowData struct {
	Data   map[string]*dbutil.ColumnData
	Source int
}

// RowDatas is a heap of MergeItems.
type RowDatas struct {
	Rows         []RowData
	OrderKeyCols []*model.ColumnInfo
}

func (r RowDatas) Len() int { return len(r.Rows) }
func (r RowDatas) Less(i, j int) bool {
	for _, col := range r.OrderKeyCols {
		col1, ok := r.Rows[i].Data[col.Name.O]
		if !ok {
			log.Fatal("data don't have column", zap.String("column", col.Name.O), zap.Reflect("data", r.Rows[i].Data))
		}
		col2, ok := r.Rows[j].Data[col.Name.O]
		if !ok {
			log.Fatal("data don't have column", zap.String("column", col.Name.O), zap.Reflect("data", r.Rows[j].Data))
		}

		if col1.IsNull {
			if col2.IsNull {
				continue
			}

			return true
		}
		if col2.IsNull {
			return false
		}

		strData1 := string(col1.Data)
		strData2 := string(col2.Data)

		if needQuotes(col.FieldType) {
			if strData1 == strData2 {
				continue
			}
			if strData1 > strData2 {
				return false
			}
			return true
		}

		num1, err1 := strconv.ParseFloat(strData1, 64)
		if err1 != nil {
			log.Fatal("convert string to float failed", zap.String("column", col.Name.O), zap.String("data", strData1), zap.Error(err1))
		}
		num2, err2 := strconv.ParseFloat(strData2, 64)
		if err2 != nil {
			log.Fatal("convert string to float failed", zap.String("column", col.Name.O), zap.String("data", strData2), zap.Error(err2))
		}

		if num1 == num2 {
			continue
		}
		if num1 > num2 {
			return false
		}
		return true

	}

	return false
}
func (r RowDatas) Swap(i, j int) { r.Rows[i], r.Rows[j] = r.Rows[j], r.Rows[i] }

// Push implements heap.Interface's Push function
func (r *RowDatas) Push(x interface{}) {
	r.Rows = append(r.Rows, x.(RowData))
}

// Pop implements heap.Interface's Pop function
func (r *RowDatas) Pop() interface{} {
	if len(r.Rows) == 0 {
		return nil
	}
	old := r.Rows
	n := len(old)
	x := old[n-1]
	r.Rows = old[0 : n-1]
	return x
}

func needQuotes(ft types.FieldType) bool {
	return !(dbutil.IsNumberType(ft.Tp) || dbutil.IsFloatType(ft.Tp))
}

type RowDataIterator interface {
	Next() (map[string]*dbutil.ColumnData, error)
	GenerateFixSQL(t DMLType) (string, error)
	Close()
}

type ChecksumInfo struct {
	Checksum int64
	Err      error
	Cost     time.Duration
}

type TableRange struct {
	ChunkRange *chunk.Range
	TableIndex int
	Schema     string
	Table      string
}

type Source interface {
	GenerateChunksIterator(node *checkpoints.Node, from SourceSide) (DBIterator, error)
	GetCrc32(context.Context, *TableRange, chan *ChecksumInfo)
	GetOrderKeyCols(int) []*model.ColumnInfo
	GetRowsIterator(context.Context, *TableRange) (RowDataIterator, error)
	GenerateReplaceDML(map[string]*dbutil.ColumnData, int) string
	GenerateDeleteDML(map[string]*dbutil.ColumnData, int) string
	GetTable(i int) *common.TableDiff
	Close()
}

func NewSources(ctx context.Context, cfg *config.Config) (downstream Source, upstream Source, err error) {
	// downstream only tidb now
	// TODO support mysql?
	sourceDBs, cfgTables, err := initTables(ctx, cfg)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}

	targetTableDiffs := make([]*common.TableDiff, 0, len(cfgTables))
	for _, tables := range cfgTables {
		for _, tableConfig := range tables {
			targetTableDiffs = append(targetTableDiffs, &common.TableDiff{
				Schema:        tableConfig.Schema,
				Table:         tableConfig.Table,
				Info:          tableConfig.TargetTableInfo,
				IgnoreColumns: tableConfig.IgnoreColumns,
				Fields:        tableConfig.Fields,
				Range:         tableConfig.Range,
				Collation:     tableConfig.Collation,
				// TODO table different?
				UseChecksum:       cfg.UseChecksum,
				OnlyUseChecksum:   cfg.OnlyUseChecksum,
				IgnoreStructCheck: cfg.IgnoreStructCheck,
				IgnoreDataCheck:   cfg.IgnoreDataCheck,
				UseCheckpoint:     cfg.UseCheckpoint,
			})
		}
	}

	downstream, err = NewTiDBSource(ctx, targetTableDiffs, cfg.TargetDBCfg.Conn)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}

	switch cfg.SourceDBCfg[0].DBType {
	case "TiDB":
		sourceTablesDiffs := make([]*common.TableDiff, 0, len(cfgTables))
		for _, tables := range cfgTables {
			for _, tableConfig := range tables {
				sourceTable := tableConfig.SourceTables[0]
				sourceTablesDiffs = append(sourceTablesDiffs, &common.TableDiff{
					Schema: sourceTable.Schema,
					Table:  sourceTable.Table,
				})
			}
		}
		upstream, err = NewTiDBSource(ctx, sourceTablesDiffs, cfg.SourceDBCfg[0].Conn)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
	case "mysql":
		sourceTablesDiffs := make([][]config.TableInstance, 0, len(cfgTables))
		for _, tables := range cfgTables {
			for _, tableConfig := range tables {
				sourceTablesDiffs = append(sourceTablesDiffs, tableConfig.SourceTables)
			}
		}
		upstream, err = NewMysqlSource(ctx, sourceTablesDiffs, sourceDBs)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}

	}

	return
}

func initDBConn(ctx context.Context, cfg *config.Config) (sourceDBs map[string]*config.DBConfig, err error) {
	cfg.TargetDBCfg.Conn, err = common.CreateDB(ctx, &cfg.TargetDBCfg.DBConfig, nil, cfg.CheckThreadCount)
	if err != nil {
		return nil, errors.Trace(err)
	}

	// TODO targetTZOffset?
	targetTZOffset, err := dbutil.GetTimeZoneOffset(ctx, cfg.TargetDBCfg.Conn)
	if err != nil {
		return nil, errors.Annotatef(err, "fetch target db %s time zone offset failed", cfg.TargetDBCfg.DBConfig.String())
	}
	vars := map[string]string{
		"time_zone": dbutil.FormatTimeZoneOffset(targetTZOffset),
	}

	// upstream
	if len(cfg.SourceDBCfg) < 1 {
		return nil, errors.New(" source config")
	}

	sourceDBs = make(map[string]*config.DBConfig)
	for _, source := range cfg.SourceDBCfg {
		// connect source db with target db time_zone
		source.Conn, err = common.CreateDB(ctx, &source.DBConfig, vars, cfg.CheckThreadCount)
		if err != nil {
			return nil, errors.Trace(err)
		}
		sourceDBs[source.InstanceID] = source
	}

	return
}

func initTables(ctx context.Context, cfg *config.Config) (connDBs map[string]*sql.DB, cfgTables map[string]map[string]*config.TableConfig, err error) {
	sourceDBs, err := initDBConn(ctx, cfg)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}

	tableRouter, err := router.NewTableRouter(false, cfg.TableRules)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}

	allTablesMap, err := utils.GetAllTables(ctx, cfg)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}

	// get all source table's matched target table
	// target database name => target table name => all matched source table instance
	sourceTablesMap := make(map[string]map[string][]config.TableInstance)
	for instanceID, allSchemas := range allTablesMap {
		if instanceID == cfg.TargetDBCfg.InstanceID {
			continue
		}

		for schema, allTables := range allSchemas {
			for table := range allTables {
				targetSchema, targetTable, err := tableRouter.Route(schema, table)
				if err != nil {
					return nil, nil, errors.Errorf("get route result for %s.%s.%s failed, error %v", instanceID, schema, table, err)
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

		sourceTables := make([]config.TableInstance, 0, len(table.SourceTables))
		for _, sourceTable := range table.SourceTables {
			if _, ok := sourceDBs[sourceTable.InstanceID]; !ok {
				return nil, nil, errors.Errorf("unkonwn database instance id %s", sourceTable.InstanceID)
			}

			allTables, ok := allTablesMap[sourceDBs[sourceTable.InstanceID].InstanceID][sourceTable.Schema]
			if !ok {
				return nil, nil, errors.Errorf("unknown schema %s in database %+v", sourceTable.Schema, sourceDBs[sourceTable.InstanceID])
			}

			tables, err := utils.GetMatchTable(sourceDBs[sourceTable.InstanceID], sourceTable.Schema, sourceTable.Table, allTables)
			if err != nil {
				return nil, nil, errors.Trace(err)
			}

			for _, table := range tables {
				sourceTables = append(sourceTables, config.TableInstance{
					InstanceID: sourceTable.InstanceID,
					Schema:     sourceTable.Schema,
					Table:      table,
				})
			}
		}

		if len(sourceTables) != 0 {
			cfgTables[table.Schema][table.Table].SourceTables = sourceTables
		}
		if table.Range != "" {
			cfgTables[table.Schema][table.Table].Range = table.Range
		}
		cfgTables[table.Schema][table.Table].IgnoreColumns = table.IgnoreColumns
		cfgTables[table.Schema][table.Table].Fields = table.Fields
		cfgTables[table.Schema][table.Table].Collation = table.Collation
	}

	// we need to increase max open connections for upstream, because one chunk needs accessing N shard tables in one
	// upstream, and there are `CheckThreadCount` processing chunks. At most we need N*`CheckThreadCount` connections
	// for an upstream
	// instanceID -> max number of upstream shard tables every target table
	maxNumShardTablesOneRun := map[string]int{}
	for _, targetTables := range cfgTables {
		for _, sourceCfg := range targetTables {
			upstreamCount := map[string]int{}
			for _, sourceTables := range sourceCfg.SourceTables {
				upstreamCount[sourceTables.InstanceID]++
			}
			for id, count := range upstreamCount {
				if count > maxNumShardTablesOneRun[id] {
					maxNumShardTablesOneRun[id] = count
				}
			}
		}
	}

	connDBs = make(map[string]*sql.DB)
	for instanceId, count := range maxNumShardTablesOneRun {
		db := sourceDBs[instanceId].Conn
		if db == nil {
			return nil, nil, errors.Errorf("didn't found sourceDB for instance %s", instanceId)
		}
		log.Info("will increase connection configurations for DB of instance",
			zap.String("instance id", instanceId),
			zap.Int("connection limit", count*cfg.CheckThreadCount))
		db.SetMaxOpenConns(count * cfg.CheckThreadCount)
		db.SetMaxIdleConns(count * cfg.CheckThreadCount)
		connDBs[instanceId] = db
	}

	return
}

// DBIterator generate next chunk for the whole tables lazily.
type DBIterator interface {
	// Next seeks the next chunk, return nil if seeks to end.
	Next() (*TableRange, error)
	Close()
}
