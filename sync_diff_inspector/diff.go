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
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb-tools/pkg/dbutil"
	"github.com/pingcap/tidb-tools/pkg/diff"
	router "github.com/pingcap/tidb-tools/pkg/table-router"
	"github.com/pingcap/tidb-tools/pkg/utils"
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
	tables            map[string]map[string]*TableConfig
	fixSQLFile        *os.File

	report         *Report
	tidbInstanceID string
	tableRouter    *router.Table
	cpDB           *sql.DB

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
	for _, source := range cfg.SourceDBCfg {
		source.Conn, err = diff.CreateDB(df.ctx, source.DBConfig, cfg.CheckThreadCount)
		if err != nil {
			return errors.Errorf("create source db %+v error %v", source.DBConfig, err)
		}
		df.sourceDBs[source.InstanceID] = source
	}

	// create connection for target.
	cfg.TargetDBCfg.Conn, err = diff.CreateDB(df.ctx, cfg.TargetDBCfg.DBConfig, cfg.CheckThreadCount)
	if err != nil {
		return errors.Errorf("create target db %+v error %v", cfg.TargetDBCfg, err)
	}
	df.targetDB = cfg.TargetDBCfg

	df.cpDB, err = diff.CreateDBForCP(df.ctx, cfg.TargetDBCfg.DBConfig)
	if err != nil {
		return errors.Errorf("create checkpoint db %+v error %v", cfg.TargetDBCfg, err)
	}

	return nil
}

// AdjustTableConfig adjusts the table's config by check-tables and table-config.
func (df *Diff) AdjustTableConfig(cfg *Config) (err error) {
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
			tables = append(tables, matchedTables...)
		}

		for _, tableName := range tables {
			tableInfo, err := dbutil.GetTableInfo(df.ctx, df.targetDB.Conn, schemaTables.Schema, tableName, df.targetDB.SQLMode)
			if err != nil {
				return errors.Errorf("get table %s.%s's inforamtion error %s", schemaTables.Schema, tableName, errors.ErrorStack(err))
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
					SQLMode:    df.sourceDBs[sourceTable.InstanceID].SQLMode,
				}
				sourceTables = append(sourceTables, sourceTableInstance)
			}

			targetTableInstance := &diff.TableInstance{
				Conn:       df.targetDB.Conn,
				Schema:     table.Schema,
				Table:      table.Table,
				InstanceID: df.targetDB.InstanceID,
				SQLMode:    df.targetDB.SQLMode,
			}

			// find tidb instance for getting statistical information to split chunk
			var tidbStatsSource *diff.TableInstance
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

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
				return errors.Trace(err)
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
