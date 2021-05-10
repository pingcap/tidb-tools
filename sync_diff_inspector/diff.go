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

package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/pingcap/dm/dm/config"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb-tools/pkg/dbutil"
	"github.com/pingcap/tidb-tools/pkg/diff"
	"github.com/pingcap/tidb-tools/pkg/filter"
	router "github.com/pingcap/tidb-tools/pkg/table-router"
	"github.com/pingcap/tidb-tools/pkg/utils"
	tidbconfig "github.com/pingcap/tidb/config"
	"go.uber.org/zap"
)

// Diff contains two sql DB, used for comparing.
type Diff struct {
	sourceDBs         map[string]DBConfig
	targetDB          DBConfig
	chunkSize         int
	sample            int
	checkThreadCount  int
	useChecksum       bool
	useCheckpoint     bool
	onlyUseChecksum   bool
	ignoreDataCheck   bool
	ignoreStructCheck bool
	ignoreStats       bool
	tables            map[string]map[string]*TableConfig
	fixSQLFile        *os.File

	report         *Report
	tidbInstanceID string
	tableRouter    *router.Table
	cpDB           *sql.DB

	// DM's subtask config
	subTaskCfgs []*config.SubTaskConfig

	ctx context.Context
}

// NewDiff returns a Diff instance.
func NewDiff(ctx context.Context, cfg *Config) (diff *Diff, err error) {
	diff = &Diff{
		sourceDBs:         make(map[string]DBConfig),
		chunkSize:         cfg.ChunkSize,
		sample:            cfg.Sample,
		checkThreadCount:  cfg.CheckThreadCount,
		useChecksum:       cfg.UseChecksum,
		useCheckpoint:     cfg.UseCheckpoint,
		onlyUseChecksum:   cfg.OnlyUseChecksum,
		ignoreDataCheck:   cfg.IgnoreDataCheck,
		ignoreStructCheck: cfg.IgnoreStructCheck,
		ignoreStats:       cfg.IgnoreStats,
		tables:            make(map[string]map[string]*TableConfig),
		report:            NewReport(),
		ctx:               ctx,
	}

	if err = diff.init(cfg); err != nil {
		diff.Close()
		return nil, errors.Trace(err)
	}

	return diff, nil
}

func (df *Diff) init(cfg *Config) (err error) {
	setTiDBCfg()

	if len(cfg.DMAddr) != 0 {
		subTaskCfgs, err := getDMTaskCfg(cfg.DMAddr, cfg.DMTask)
		if err != nil {
			return errors.Trace(err)
		}
		df.subTaskCfgs = subTaskCfgs

		err = df.adjustDBCfgByDMSubTasks(cfg)
		if err != nil {
			return err
		}
	}

	// create connection for source.
	if err = df.CreateDBConn(cfg); err != nil {
		return errors.Trace(err)
	}

	if err = df.AdjustTableConfig(cfg); err != nil {
		return errors.Trace(err)
	}

	df.fixSQLFile, err = os.Create(cfg.FixSQLFile)
	if err != nil {
		return errors.Trace(err)
	}

	return nil
}

// CreateDBConn creates db connections for source and target.
func (df *Diff) CreateDBConn(cfg *Config) (err error) {
	// create connection for target.
	cfg.TargetDBCfg.Conn, err = diff.CreateDB(df.ctx, cfg.TargetDBCfg.DBConfig, nil, cfg.CheckThreadCount)
	if err != nil {
		return errors.Errorf("create target db %s error %v", cfg.TargetDBCfg.DBConfig.String(), err)
	}
	df.targetDB = cfg.TargetDBCfg

	targetTZOffset, err := dbutil.GetTimeZoneOffset(df.ctx, cfg.TargetDBCfg.Conn)
	if err != nil {
		return errors.Annotatef(err, "fetch target db %s time zone offset failed", cfg.TargetDBCfg.DBConfig.String())
	}
	vars := map[string]string{
		"time_zone": dbutil.FormatTimeZoneOffset(targetTZOffset),
	}

	for _, source := range cfg.SourceDBCfg {
		// connect source db with target db time_zone
		source.Conn, err = diff.CreateDB(df.ctx, source.DBConfig, vars, cfg.CheckThreadCount)
		if err != nil {
			return errors.Annotatef(err, "create source db %s failed", source.DBConfig.String())
		}
		df.sourceDBs[source.InstanceID] = source
	}

	df.cpDB, err = diff.CreateDBForCP(df.ctx, cfg.TargetDBCfg.DBConfig)
	if err != nil {
		return errors.Errorf("create checkpoint db %s error %v", cfg.TargetDBCfg.DBConfig.String(), err)
	}

	return nil
}

func (df *Diff) adjustTableConfigBySubTask(cfg *Config) (err error) {
	allTablesMap, err := df.GetAllTables(cfg)
	if err != nil {
		return errors.Trace(err)
	}

	baLists := make(map[string]*filter.Filter)
	tableRouters := make(map[string]*router.Table)

	for _, subTaskCfg := range df.subTaskCfgs {
		baList, err := filter.New(subTaskCfg.CaseSensitive, subTaskCfg.BAList)
		if err != nil {
			return err
		}
		baLists[subTaskCfg.SourceID] = baList

		tableRouter, err := router.NewTableRouter(subTaskCfg.CaseSensitive, []*router.TableRule{})
		if err != nil {
			return err
		}
		for _, rule := range subTaskCfg.RouteRules {
			err := tableRouter.AddRule(rule)
			if err != nil {
				return err
			}
		}
		tableRouters[subTaskCfg.SourceID] = tableRouter
	}

	// get all source table's matched target table
	// target database name => target table name => all matched source table instance
	sourceTablesMap := make(map[string]map[string][]TableInstance)
	for instanceID, allSchemas := range allTablesMap {
		if instanceID == df.targetDB.InstanceID {
			continue
		}

		for schema, allTables := range allSchemas {
			if filter.IsSystemSchema(schema) {
				continue
			}

			for table := range allTables {
				if baLists[instanceID] != nil {
					tbs := []*filter.Table{{Schema: schema, Name: table}}
					tbs = baLists[instanceID].ApplyOn(tbs)
					if len(tbs) == 0 {
						continue
					}
				}

				targetSchema := schema
				targetTable := table
				if tableRouters[instanceID] != nil {
					targetSchema, targetTable, err = tableRouters[instanceID].Route(schema, table)
					if err != nil {
						return errors.Errorf("get route result for %s.%s.%s failed, error %v", instanceID, schema, table, err)
					}
				}

				if _, ok := sourceTablesMap[targetSchema]; !ok {
					sourceTablesMap[targetSchema] = make(map[string][]TableInstance)
				}

				if _, ok := sourceTablesMap[targetSchema][targetTable]; !ok {
					sourceTablesMap[targetSchema][targetTable] = make([]TableInstance, 0, 1)
				}

				sourceTablesMap[targetSchema][targetTable] = append(sourceTablesMap[targetSchema][targetTable], TableInstance{
					InstanceID: instanceID,
					Schema:     schema,
					Table:      table,
				})
			}
		}
	}

	for schema, tables := range sourceTablesMap {
		for table, sourceTables := range tables {
			if len(sourceTables) == 0 {
				continue
			}

			tableInfo, err := dbutil.GetTableInfo(df.ctx, df.targetDB.Conn, schema, table)
			if err != nil {
				return errors.Errorf("get table %s.%s's information error %s", schema, table, errors.ErrorStack(err))
			}

			if _, ok := df.tables[schema]; !ok {
				df.tables[schema] = make(map[string]*TableConfig)
			}

			df.tables[schema][table] = &TableConfig{
				TableInstance: TableInstance{
					Schema: schema,
					Table:  table,
				},
				TargetTableInfo: tableInfo,
				Range:           "TRUE",
				SourceTables:    sourceTables,
			}
		}
	}

	return nil
}

// AdjustTableConfig adjusts the table's config by check-tables and table-config.
func (df *Diff) AdjustTableConfig(cfg *Config) (err error) {
	if len(df.subTaskCfgs) != 0 {
		return df.adjustTableConfigBySubTask(cfg)
	}

	df.tableRouter, err = router.NewTableRouter(false, cfg.TableRules)
	if err != nil {
		return errors.Trace(err)
	}

	allTablesMap, err := df.GetAllTables(cfg)
	if err != nil {
		return errors.Trace(err)
	}

	// get all source table's matched target table
	// target database name => target table name => all matched source table instance
	sourceTablesMap := make(map[string]map[string][]TableInstance)
	for instanceID, allSchemas := range allTablesMap {
		if instanceID == df.targetDB.InstanceID {
			continue
		}

		for schema, allTables := range allSchemas {
			for table := range allTables {
				targetSchema, targetTable, err := df.tableRouter.Route(schema, table)
				if err != nil {
					return errors.Errorf("get route result for %s.%s.%s failed, error %v", instanceID, schema, table, err)
				}

				if _, ok := sourceTablesMap[targetSchema]; !ok {
					sourceTablesMap[targetSchema] = make(map[string][]TableInstance)
				}

				if _, ok := sourceTablesMap[targetSchema][targetTable]; !ok {
					sourceTablesMap[targetSchema][targetTable] = make([]TableInstance, 0, 1)
				}

				sourceTablesMap[targetSchema][targetTable] = append(sourceTablesMap[targetSchema][targetTable], TableInstance{
					InstanceID: instanceID,
					Schema:     schema,
					Table:      table,
				})
			}
		}
	}

	// fill the table information.
	// will add default source information, don't worry, we will use table config's info replace this later.
	for _, schemaTables := range cfg.Tables {
		df.tables[schemaTables.Schema] = make(map[string]*TableConfig)
		tables := make([]string, 0, len(schemaTables.Tables))
		allTables, ok := allTablesMap[df.targetDB.InstanceID][schemaTables.Schema]
		if !ok {
			return errors.NotFoundf("schema %s.%s", df.targetDB.InstanceID, schemaTables.Schema)
		}

		for _, table := range schemaTables.Tables {
			matchedTables, err := df.GetMatchTable(df.targetDB, schemaTables.Schema, table, allTables)
			if err != nil {
				return errors.Trace(err)
			}

			//exclude those in "exclude-tables"
			for _, t := range matchedTables {
				if df.InExcludeTables(schemaTables.ExcludeTables, t) {
					continue
				} else {
					tables = append(tables, t)
				}
			}
		}

		for _, tableName := range tables {
			tableInfo, err := dbutil.GetTableInfo(df.ctx, df.targetDB.Conn, schemaTables.Schema, tableName)
			if err != nil {
				return errors.Errorf("get table %s.%s's information error %s", schemaTables.Schema, tableName, errors.ErrorStack(err))
			}

			if _, ok := df.tables[schemaTables.Schema][tableName]; ok {
				log.Error("duplicate config for one table", zap.String("table", dbutil.TableName(schemaTables.Schema, tableName)))
				continue
			}

			sourceTables := make([]TableInstance, 0, 1)
			if _, ok := sourceTablesMap[schemaTables.Schema][tableName]; ok {
				log.Info("find matched source tables", zap.Reflect("source tables", sourceTablesMap[schemaTables.Schema][tableName]), zap.String("target schema", schemaTables.Schema), zap.String("table", tableName))
				sourceTables = sourceTablesMap[schemaTables.Schema][tableName]
			} else {
				// use same database name and table name
				sourceTables = append(sourceTables, TableInstance{
					InstanceID: cfg.SourceDBCfg[0].InstanceID,
					Schema:     schemaTables.Schema,
					Table:      tableName,
				})
			}

			df.tables[schemaTables.Schema][tableName] = &TableConfig{
				TableInstance: TableInstance{
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
		if _, ok := df.tables[table.Schema]; !ok {
			return errors.NotFoundf("schema %s in check tables", table.Schema)
		}
		if _, ok := df.tables[table.Schema][table.Table]; !ok {
			return errors.NotFoundf("table %s.%s in check tables", table.Schema, table.Table)
		}

		sourceTables := make([]TableInstance, 0, len(table.SourceTables))
		for _, sourceTable := range table.SourceTables {
			if _, ok := df.sourceDBs[sourceTable.InstanceID]; !ok {
				return errors.Errorf("unkonwn database instance id %s", sourceTable.InstanceID)
			}

			allTables, ok := allTablesMap[df.sourceDBs[sourceTable.InstanceID].InstanceID][sourceTable.Schema]
			if !ok {
				return errors.Errorf("unknown schema %s in database %+v", sourceTable.Schema, df.sourceDBs[sourceTable.InstanceID])
			}

			tables, err := df.GetMatchTable(df.sourceDBs[sourceTable.InstanceID], sourceTable.Schema, sourceTable.Table, allTables)
			if err != nil {
				return errors.Trace(err)
			}

			for _, table := range tables {
				sourceTables = append(sourceTables, TableInstance{
					InstanceID: sourceTable.InstanceID,
					Schema:     sourceTable.Schema,
					Table:      table,
				})
			}
		}

		if len(sourceTables) != 0 {
			df.tables[table.Schema][table.Table].SourceTables = sourceTables
		}
		if table.Range != "" {
			df.tables[table.Schema][table.Table].Range = table.Range
		}
		df.tables[table.Schema][table.Table].IgnoreColumns = table.IgnoreColumns
		df.tables[table.Schema][table.Table].Fields = table.Fields
		df.tables[table.Schema][table.Table].Collation = table.Collation
	}

	// we need to increase max open connections for upstream, because one chunk needs accessing N shard tables in one
	// upstream, and there are `CheckThreadCount` processing chunks. At most we need N*`CheckThreadCount` connections
	// for an upstream
	// instanceID -> max number of upstream shard tables every target table
	maxNumShardTablesOneRun := map[string]int{}
	for _, targetTables := range df.tables {
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

	for instanceId, count := range maxNumShardTablesOneRun {
		db := df.sourceDBs[instanceId].Conn
		if db == nil {
			return errors.Errorf("didn't found sourceDB for instance %s", instanceId)
		}
		log.Info("will increase connection configurations for DB of instance",
			zap.String("instance id", instanceId),
			zap.Int("connection limit", count*df.checkThreadCount))
		db.SetMaxOpenConns(count * df.checkThreadCount)
		db.SetMaxIdleConns(count * df.checkThreadCount)
	}

	return nil
}

func (df *Diff) adjustDBCfgByDMSubTasks(cfg *Config) error {
	// all subtask had same target, so use subTaskCfgs[0]
	cfg.TargetDBCfg = DBConfig{
		InstanceID: "target",
		DBConfig: dbutil.DBConfig{
			Host:     df.subTaskCfgs[0].To.Host,
			Port:     df.subTaskCfgs[0].To.Port,
			User:     df.subTaskCfgs[0].To.User,
			Password: df.subTaskCfgs[0].To.Password,
		},
	}

	for _, subTaskCfg := range df.subTaskCfgs {
		// fill source-db
		cfg.SourceDBCfg = append(cfg.SourceDBCfg, DBConfig{
			InstanceID: subTaskCfg.SourceID,
			DBConfig: dbutil.DBConfig{
				Host:     subTaskCfg.From.Host,
				Port:     subTaskCfg.From.Port,
				User:     subTaskCfg.From.User,
				Password: subTaskCfg.From.Password,
			},
		})
	}

	return nil
}

// GetAllTables get all tables in all databases.
func (df *Diff) GetAllTables(cfg *Config) (map[string]map[string]map[string]interface{}, error) {
	// instanceID => schema => table
	allTablesMap := make(map[string]map[string]map[string]interface{})

	allTablesMap[df.targetDB.InstanceID] = make(map[string]map[string]interface{})
	targetSchemas, err := dbutil.GetSchemas(df.ctx, df.targetDB.Conn)
	if err != nil {
		return nil, errors.Annotatef(err, "get schemas from %s", df.targetDB.InstanceID)
	}
	for _, schema := range targetSchemas {
		allTables, err := dbutil.GetTables(df.ctx, df.targetDB.Conn, schema)
		if err != nil {
			return nil, errors.Annotatef(err, "get tables from %s.%s", df.targetDB.InstanceID, schema)
		}
		allTablesMap[df.targetDB.InstanceID][schema] = utils.SliceToMap(allTables)
	}

	for _, source := range df.sourceDBs {
		allTablesMap[source.InstanceID] = make(map[string]map[string]interface{})
		sourceSchemas, err := dbutil.GetSchemas(df.ctx, source.Conn)
		if err != nil {
			return nil, errors.Annotatef(err, "get schemas from %s", source.InstanceID)
		}

		for _, schema := range sourceSchemas {
			allTables, err := dbutil.GetTables(df.ctx, source.Conn, schema)
			if err != nil {
				return nil, errors.Annotatef(err, "get tables from %s.%s", source.InstanceID, schema)
			}
			allTablesMap[source.InstanceID][schema] = utils.SliceToMap(allTables)
		}
	}

	return allTablesMap, nil
}

// GetMatchTable returns all the matched table.
func (df *Diff) GetMatchTable(db DBConfig, schema, table string, allTables map[string]interface{}) ([]string, error) {
	tableNames := make([]string, 0, 1)

	if table[0] == '~' {
		tableRegex := regexp.MustCompile(fmt.Sprintf("(?i)%s", table[1:]))
		for tableName := range allTables {
			if !tableRegex.MatchString(tableName) {
				continue
			}
			tableNames = append(tableNames, tableName)
		}
	} else {
		if _, ok := allTables[table]; ok {
			tableNames = append(tableNames, table)
		} else {
			return nil, errors.Errorf("%s.%s not found in %s", schema, table, db.InstanceID)
		}
	}

	return tableNames, nil
}

// Close closes file and database connection.
func (df *Diff) Close() {
	if df.fixSQLFile != nil {
		df.fixSQLFile.Close()
	}

	for _, db := range df.sourceDBs {
		if db.Conn != nil {
			db.Conn.Close()
		}
	}

	if df.targetDB.Conn != nil {
		df.targetDB.Conn.Close()
	}

	if df.cpDB != nil {
		df.cpDB.Close()
	}
}

// Equal tests whether two database have same data and schema.
func (df *Diff) Equal() (err error) {
	defer df.Close()

	for _, schema := range df.tables {
		for _, table := range schema {
			sourceTables := make([]*diff.TableInstance, 0, len(table.SourceTables))
			for _, sourceTable := range table.SourceTables {
				sourceTableInstance := &diff.TableInstance{
					Conn:       df.sourceDBs[sourceTable.InstanceID].Conn,
					Schema:     sourceTable.Schema,
					Table:      sourceTable.Table,
					InstanceID: sourceTable.InstanceID,
				}
				sourceTables = append(sourceTables, sourceTableInstance)
			}

			targetTableInstance := &diff.TableInstance{
				Conn:       df.targetDB.Conn,
				Schema:     table.Schema,
				Table:      table.Table,
				InstanceID: df.targetDB.InstanceID,
			}

			// find tidb instance for getting statistical information to split chunk
			var tidbStatsSource *diff.TableInstance
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			if !df.ignoreStats {
				log.Info("use tidb stats to split chunks")
				isTiDB, err := dbutil.IsTiDB(ctx, targetTableInstance.Conn)
				if err != nil {
					log.Warn("judge instance is tidb failed", zap.Error(err))
				} else if isTiDB {
					tidbStatsSource = targetTableInstance
				} else if len(sourceTables) == 1 {
					isTiDB, err := dbutil.IsTiDB(ctx, sourceTables[0].Conn)
					if err != nil {
						log.Warn("judge instance is tidb failed", zap.Error(err))
					} else if isTiDB {
						tidbStatsSource = sourceTables[0]
					}
				}
			} else {
				log.Info("ignore tidb stats because of user setting")
			}

			td := &diff.TableDiff{
				SourceTables: sourceTables,
				TargetTable:  targetTableInstance,

				IgnoreColumns: table.IgnoreColumns,

				Fields:            table.Fields,
				Range:             table.Range,
				Collation:         table.Collation,
				ChunkSize:         df.chunkSize,
				Sample:            df.sample,
				CheckThreadCount:  df.checkThreadCount,
				UseChecksum:       df.useChecksum,
				UseCheckpoint:     df.useCheckpoint,
				OnlyUseChecksum:   df.onlyUseChecksum,
				IgnoreStructCheck: df.ignoreStructCheck,
				IgnoreDataCheck:   df.ignoreDataCheck,
				TiDBStatsSource:   tidbStatsSource,
				CpDB:              df.cpDB,
			}

			structEqual, dataEqual, err := td.Equal(df.ctx, func(dml string) error {
				_, err := df.fixSQLFile.WriteString(fmt.Sprintf("%s\n", dml))
				return errors.Trace(err)
			})

			if err != nil {
				log.Error("check failed", zap.String("table", dbutil.TableName(table.Schema, table.Table)), zap.Error(err))
				df.report.SetTableMeetError(table.Schema, table.Table, err)
				df.report.FailedNum++
				continue
			}

			df.report.SetTableStructCheckResult(table.Schema, table.Table, structEqual)
			df.report.SetTableDataCheckResult(table.Schema, table.Table, dataEqual)
			if structEqual && dataEqual {
				df.report.PassNum++
			} else {
				df.report.FailedNum++
			}
		}
	}

	return
}

// Judge if a table is in "exclude-tables" list
func (df *Diff) InExcludeTables(exclude_tables []string, table string) bool {
	for _, exclude_table := range exclude_tables {
		if strings.EqualFold(exclude_table, table) {
			return true
		}
	}
	return false
}

func setTiDBCfg() {
	// to support long index key in TiDB
	tidbCfg := tidbconfig.GetGlobalConfig()
	// 3027 * 4 is the max value the MaxIndexLength can be set
	tidbCfg.MaxIndexLength = 3027 * 4
	tidbconfig.StoreGlobalConfig(tidbCfg)

	fmt.Println("set tidb cfg")
}
