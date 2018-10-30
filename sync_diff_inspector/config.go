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
	"database/sql"
	"flag"
	"fmt"

	"github.com/BurntSushi/toml"
	"github.com/juju/errors"
	"github.com/pingcap/tidb-tools/pkg/dbutil"
	"github.com/pingcap/tidb/model"
	log "github.com/sirupsen/logrus"
)

const (
	percent0   = 0
	percent100 = 100
)

var sourceInstanceMap map[string]interface{} = make(map[string]interface{})

// DBConfig is the config of database, and keep the connection.
type DBConfig struct {
	dbutil.DBConfig

	InstanceID string `toml:"instance-id" json:"instance-id"`

	Snapshot string `toml:"snapshot" json:"snapshot"`

	Conn *sql.DB
}

// Valid returns true if database's config is valide.
func (c *DBConfig) Valid() bool {
	if c.InstanceID == "" {
		log.Error("must specify source database's instance id")
		return false
	}
	sourceInstanceMap[c.InstanceID] = struct{}{}

	return true
}

// CheckTables saves the tables need to check.
type CheckTables struct {
	// schema name
	Schema string `toml:"schema" json:"schema"`

	// table list
	Tables []string `toml:"tables" json:"tables"`
}

// TableConfig is the config of table.
type TableConfig struct {
	// table's origin information
	TableInstance
	// columns be ignored, will not check this column's data, but may use these columns as split field or order by key.
	IgnoreColumns []string `toml:"ignore-columns"`
	// columns be removed, will remove these columns from table info, and will not check these columns' data.
	RemoveColumns []string `toml:"remove-columns"`
	// field should be the primary key, unique key or field with index
	Field string `toml:"index-field"`
	// select range, for example: "age > 10 AND age < 20"
	Range string `toml:"range"`
	// set true if comparing sharding tables with target table, should have more than one source tables.
	IsSharding bool `toml:"is-sharding"`
	// saves the source tables's info.
	// may have more than one source for sharding tables.
	// or you want to compare table with different schema and table name.
	// SourceTables can be nil when source and target is one-to-one correspondence.
	SourceTables    []TableInstance `toml:"source-tables"`
	TargetTableInfo *model.TableInfo

	// collation config in mysql/tidb
	Collation string `toml:"collation"`
}

// Valid returns true if table's config is valide.
func (t *TableConfig) Valid() bool {
	if t.Schema == "" || t.Table == "" {
		log.Error("schema and table's name can't be empty")
		return false
	}

	if t.IsSharding {
		if len(t.SourceTables) <= 1 {
			log.Error("must have more than one source tables if comparing sharding tables")
			return false
		}

	} else {
		if len(t.SourceTables) > 1 {
			log.Error("have more than one source table in no sharding mode")
			return false
		}
	}

	for _, sourceTable := range t.SourceTables {
		if !sourceTable.Valid() {
			return false
		}
	}

	return true
}

// TableInstance saves the base information of table.
type TableInstance struct {
	// database's instance id
	InstanceID string `toml:"instance-id" json:"instance-id"`
	// schema name
	Schema string `toml:"schema"`
	// table name
	Table string `toml:"table"`
}

// Valid returns true if table instance's info is valide.
// should be executed after source database's check.
func (t *TableInstance) Valid() bool {
	if t.InstanceID == "" {
		log.Error("must specify the database's instance id for source table")
		return false
	}

	if _, ok := sourceInstanceMap[t.InstanceID]; !ok {
		log.Errorf("unknown database instance id %s", t.InstanceID)
		return false
	}

	if t.Schema == "" || t.Table == "" {
		log.Error("schema and table's name can't be empty")
		return false
	}

	return true
}

// Config is the configuration.
type Config struct {
	*flag.FlagSet `json:"-"`

	// log level
	LogLevel string `toml:"log-level" json:"log-level"`

	// source database's config
	SourceDBCfg []DBConfig `toml:"source-db" json:"source-db"`

	// target database's config
	TargetDBCfg DBConfig `toml:"target-db" json:"target-db"`

	// for example, the whole data is [1...100]
	// we can split these data to [1...10], [11...20], ..., [91...100]
	// the [1...10] is a chunk, and it's chunk size is 10
	// size of the split chunk
	ChunkSize int `toml:"chunk-size" json:"chunk-size"`

	// sampling check percent, for example 10 means only check 10% data
	Sample int `toml:"sample-percent" json:"sample-percent"`

	// how many goroutines are created to check data
	CheckThreadCount int `toml:"check-thread-count" json:"check-thread-count"`

	// set true if target-db and source-db all support tidb implicit column "_tidb_rowid"
	UseRowID bool `toml:"use-rowid" json:"use-rowid"`

	// set false if want to comapre the data directly
	UseChecksum bool `toml:"use-checksum" json:"use-checksum"`

	// the name of the file which saves sqls used to fix different data
	FixSQLFile string `toml:"fix-sql-file" json:"fix-sql-file"`

	// the tables to be checked
	Tables []*CheckTables `toml:"check-tables" json:"check-tables"`

	// the config of table
	TableCfgs []*TableConfig `toml:"table-config" json:"table-config"`

	// ignore check table's struct
	IgnoreStructCheck bool `toml:"ignore-struct-check" json:"ignore-struct-check"`

	// ignore check table's data
	IgnoreDataCheck bool `toml:"ignore-data-check" json:"ignore-data-check"`

	// config file
	ConfigFile string

	// print version if set true
	PrintVersion bool
}

// NewConfig creates a new config.
func NewConfig() *Config {
	cfg := &Config{}
	cfg.FlagSet = flag.NewFlagSet("diff", flag.ContinueOnError)
	fs := cfg.FlagSet

	fs.StringVar(&cfg.ConfigFile, "config", "", "Config file")
	fs.StringVar(&cfg.LogLevel, "L", "info", "log level: debug, info, warn, error, fatal")
	fs.IntVar(&cfg.ChunkSize, "chunk-size", 1000, "diff check chunk size")
	fs.IntVar(&cfg.Sample, "sample", 100, "the percent of sampling check")
	fs.IntVar(&cfg.CheckThreadCount, "check-thread-count", 1, "how many goroutines are created to check data")
	fs.BoolVar(&cfg.UseRowID, "use-rowid", false, "set true if target-db and source-db all support tidb implicit column _tidb_rowid")
	fs.BoolVar(&cfg.UseChecksum, "use-checksum", true, "set false if want to comapre the data directly")
	fs.StringVar(&cfg.FixSQLFile, "fix-sql-file", "fix.sql", "the name of the file which saves sqls used to fix different data")
	fs.BoolVar(&cfg.PrintVersion, "V", false, "print version of sync_diff_inspector")
	fs.BoolVar(&cfg.IgnoreDataCheck, "ignore-data-check", false, "ignore check table's data")
	fs.BoolVar(&cfg.IgnoreStructCheck, "ignore-struct-check", false, "ignore check table's struct")

	return cfg
}

// Parse parses flag definitions from the argument list.
func (c *Config) Parse(arguments []string) error {
	// Parse first to get config file.
	err := c.FlagSet.Parse(arguments)
	if err != nil {
		return errors.Trace(err)
	}

	// Load config file if specified.
	if c.ConfigFile != "" {
		err = c.configFromFile(c.ConfigFile)
		if err != nil {
			return errors.Trace(err)
		}
	}

	// Parse again to replace with command line options.
	err = c.FlagSet.Parse(arguments)
	if err != nil {
		return errors.Trace(err)
	}

	if len(c.FlagSet.Args()) != 0 {
		return errors.Errorf("'%s' is an invalid flag", c.FlagSet.Arg(0))
	}

	return nil
}

func (c *Config) String() string {
	if c == nil {
		return "<nil>"
	}
	return fmt.Sprintf("Config(%+v)", *c)
}

// configFromFile loads config from file.
func (c *Config) configFromFile(path string) error {
	_, err := toml.DecodeFile(path, c)
	return errors.Trace(err)
}

func (c *Config) checkConfig() bool {
	if c.Sample > percent100 || c.Sample < percent0 {
		log.Errorf("sample must be greater than 0 and less than or equal to 100!")
		return false
	}

	if c.CheckThreadCount <= 0 {
		log.Errorf("check-thcount must greater than 0!")
		return false
	}

	if c.TargetDBCfg.InstanceID == "" {
		c.TargetDBCfg.InstanceID = "target"
	}

	if len(c.SourceDBCfg) == 0 {
		log.Error("must have at least one source database")
		return false
	}

	for i := range c.SourceDBCfg {
		if !c.SourceDBCfg[i].Valid() {
			return false
		}
	}

	if len(c.Tables) == 0 {
		log.Error("must specify check tables")
		return false
	}

	for _, tableCfg := range c.TableCfgs {
		if !tableCfg.Valid() {
			return false
		}
	}

	return true
}
