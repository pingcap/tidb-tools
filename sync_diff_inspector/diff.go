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
	"fmt"
	"os"
	"regexp"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb-tools/pkg/dbutil"
	"github.com/pingcap/tidb-tools/pkg/diff"
)

// Diff contains two sql DB, used for comparing.
type Diff struct {
	sourceDBs         map[string]DBConfig
	targetDB          DBConfig
	chunkSize         int
	sample            int
	checkThreadCount  int
	useRowID          bool
	useChecksum       bool
	ignoreDataCheck   bool
	ignoreStructCheck bool
	tables            map[string]map[string]*TableConfig
	fixSQLFile        *os.File
	report            *Report

	ctx context.Context
}

// NewDiff returns a Diff instance.
func NewDiff(ctx context.Context, cfg *Config) (diff *Diff, err error) {
	diff = &Diff{
		sourceDBs:         make(map[string]DBConfig),
		chunkSize:         cfg.ChunkSize,
		sample:            cfg.Sample,
		checkThreadCount:  cfg.CheckThreadCount,
		useRowID:          cfg.UseRowID,
		useChecksum:       cfg.UseChecksum,
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

func (df *Diff) CreateDBConn(cfg *Config) (err error) {
	// SetMaxOpenConns and SetMaxIdleConns for connection to avoid error like
	// `dial tcp 10.26.2.1:3306: connect: cannot assign requested address`
	for _, source := range cfg.SourceDBCfg {
		source.Conn, err = dbutil.OpenDB(source.DBConfig)
		if err != nil {
			return errors.Errorf("create source db %+v error %v", source.DBConfig, err)
		}
		source.Conn.SetMaxOpenConns(cfg.CheckThreadCount)
		source.Conn.SetMaxIdleConns(cfg.CheckThreadCount)

		df.sourceDBs[source.InstanceID] = source
		if source.Snapshot != "" {
			err = dbutil.SetSnapshot(df.ctx, source.Conn, source.Snapshot)
			if err != nil {
				return errors.Errorf("set history snapshot %s for source db %+v error %v", source.Snapshot, source.DBConfig, err)
			}
		}
	}

	// create connection for target.
	cfg.TargetDBCfg.Conn, err = dbutil.OpenDB(cfg.TargetDBCfg.DBConfig)
	if err != nil {
		return errors.Errorf("create target db %+v error %v", cfg.TargetDBCfg, err)
	}
	cfg.TargetDBCfg.Conn.SetMaxOpenConns(cfg.CheckThreadCount)
	cfg.TargetDBCfg.Conn.SetMaxIdleConns(cfg.CheckThreadCount)

	df.targetDB = cfg.TargetDBCfg
	if cfg.TargetDBCfg.Snapshot != "" {
		err = dbutil.SetSnapshot(df.ctx, cfg.TargetDBCfg.Conn, cfg.TargetDBCfg.Snapshot)
		if err != nil {
			return errors.Errorf("set history snapshot %s for target db %+v error %v", cfg.TargetDBCfg.Snapshot, cfg.TargetDBCfg, err)
		}
	}

	return nil
}

// AdjustTableConfig adjusts the table's config by check-tables and table-config.
func (df *Diff) AdjustTableConfig(cfg *Config) error {
	allTablesMap, err := df.GetAllTables(cfg)
	if err != nil {
		return errors.Trace(err)
	}

	// fill the table information.
	// will add default source information, don't worry, we will use table config's info replace this later.
	for _, schemaTables := range cfg.Tables {
		df.tables[schemaTables.Schema] = make(map[string]*TableConfig)
		tables := make([]string, 0, len(schemaTables.Tables))
		allTables := allTablesMap[schemaName(df.targetDB.InstanceID, schemaTables.Schema)]

		for _, table := range schemaTables.Tables {
			matchedTables, err := df.GetMatchTable(df.targetDB, schemaTables.Schema, table, allTables)
			if err != nil {
				return errors.Trace(err)
			}
			tables = append(tables, matchedTables...)
		}

		for _, tableName := range tables {
			tableInfo, err := dbutil.GetTableInfoWithRowID(df.ctx, df.targetDB.Conn, schemaTables.Schema, tableName, cfg.UseRowID)
			if err != nil {
				return errors.Errorf("get table %s.%s's inforamtion error %v", schemaTables.Schema, tableName, errors.ErrorStack(err))
			}

			if _, ok := df.tables[schemaTables.Schema][tableName]; ok {
				log.Errorf("duplicate config for %s.%s", schemaTables.Schema, tableName)
				continue
			}

			df.tables[schemaTables.Schema][tableName] = &TableConfig{
				TableInstance: TableInstance{
					Schema: schemaTables.Schema,
					Table:  tableName,
				},
				IgnoreColumns:   make([]string, 0, 1),
				TargetTableInfo: tableInfo,
				Range:           "TRUE",
				SourceTables: []TableInstance{{
					InstanceID: cfg.SourceDBCfg[0].InstanceID,
					Schema:     schemaTables.Schema,
					Table:      tableName,
				}},
			}
		}
	}

	for _, table := range cfg.TableCfgs {
		if _, ok := df.tables[table.Schema]; !ok {
			return errors.Errorf("schema %s not found in check tables", table.Schema)
		}
		if _, ok := df.tables[table.Schema][table.Table]; !ok {
			return errors.Errorf("table %s.%s not found in check tables", table.Schema, table.Table)
		}

		sourceTables := make([]TableInstance, 0, len(table.SourceTables))
		for _, sourceTable := range table.SourceTables {
			if _, ok := df.sourceDBs[sourceTable.InstanceID]; !ok {
				return errors.Errorf("unkonwn database instance id %s", sourceTable.InstanceID)
			}

			allTables, ok := allTablesMap[schemaName(df.sourceDBs[sourceTable.InstanceID].InstanceID, sourceTable.Schema)]
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
		df.tables[table.Schema][table.Table].RemoveColumns = table.RemoveColumns
		df.tables[table.Schema][table.Table].Field = table.Field
		df.tables[table.Schema][table.Table].Collation = table.Collation
	}

	return nil
}

// GetAllTables get all tables in all databases.
func (df *Diff) GetAllTables(cfg *Config) (map[string]map[string]interface{}, error) {
	allTablesMap := make(map[string]map[string]interface{})

	for _, schemaTables := range cfg.Tables {
		if _, ok := allTablesMap[schemaName(cfg.TargetDBCfg.InstanceID, schemaTables.Schema)]; ok {
			continue
		}

		allTables, err := dbutil.GetTables(df.ctx, cfg.TargetDBCfg.Conn, schemaTables.Schema)
		if err != nil {
			return nil, errors.Errorf("get tables from %s.%s error %v", cfg.TargetDBCfg.InstanceID, schemaTables.Schema, errors.Trace(err))
		}
		allTablesMap[schemaName(cfg.TargetDBCfg.InstanceID, schemaTables.Schema)] = diff.SliceToMap(allTables)
	}

	for _, table := range cfg.TableCfgs {
		for _, sourceTable := range table.SourceTables {
			if _, ok := allTablesMap[schemaName(sourceTable.InstanceID, sourceTable.Schema)]; ok {
				continue
			}

			db, ok := df.sourceDBs[sourceTable.InstanceID]
			if !ok {
				return nil, errors.Errorf("unknown instance id %s", sourceTable.InstanceID)
			}

			allTables, err := dbutil.GetTables(df.ctx, db.Conn, sourceTable.Schema)
			if err != nil {
				return nil, errors.Errorf("get tables from %s.%s error %v", db.InstanceID, sourceTable.Schema, errors.Trace(err))
			}
			allTablesMap[schemaName(db.InstanceID, sourceTable.Schema)] = diff.SliceToMap(allTables)
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
}

// Equal tests whether two database have same data and schema.
func (df *Diff) Equal() (err error) {
	defer df.Close()

	for _, schema := range df.tables {
		for _, table := range schema {
			sourceTables := make([]*diff.TableInstance, 0, len(table.SourceTables))
			for _, sourceTable := range table.SourceTables {
				sourceTables = append(sourceTables, &diff.TableInstance{
					Conn:   df.sourceDBs[sourceTable.InstanceID].Conn,
					Schema: sourceTable.Schema,
					Table:  sourceTable.Table,
				})
			}

			td := &diff.TableDiff{
				SourceTables: sourceTables,
				TargetTable: &diff.TableInstance{
					Conn:   df.targetDB.Conn,
					Schema: table.Schema,
					Table:  table.Table,
				},

				IgnoreColumns: table.IgnoreColumns,
				RemoveColumns: table.RemoveColumns,

				Field:             table.Field,
				Range:             table.Range,
				Collation:         table.Collation,
				ChunkSize:         df.chunkSize,
				Sample:            df.sample,
				CheckThreadCount:  df.checkThreadCount,
				UseRowID:          df.useRowID,
				UseChecksum:       df.useChecksum,
				IgnoreStructCheck: df.ignoreStructCheck,
				IgnoreDataCheck:   df.ignoreDataCheck,
			}

			structEqual, dataEqual, err := td.Equal(df.ctx, func(dml string) error {
				_, err := df.fixSQLFile.WriteString(fmt.Sprintf("%s\n", dml))
				return err
			})
			if err != nil {
				log.Errorf("check %s.%s equal failed, error %v", table.Schema, table.Table, errors.ErrorStack(err))
				return err
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
