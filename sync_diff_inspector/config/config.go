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

package config

import (
	"crypto/md5"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/BurntSushi/toml"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb-tools/pkg/dbutil"
	filter "github.com/pingcap/tidb-tools/pkg/table-filter"
	router "github.com/pingcap/tidb-tools/pkg/table-router"
	"go.uber.org/zap"
	"os"
	"path/filepath"
	"syscall"
)

const (
	percent0   = 0
	percent100 = 100

	LocalDirPerm  os.FileMode = 0o755
	LocalFilePerm os.FileMode = 0o644
)

// TableConfig is the config of table.
type TableConfig struct {
	// table's origin information
	Schema string `toml:"schema" json:"schema"`
	Table  string `toml:"table" json:"table"`
	// columns be ignored, will not check this column's data
	IgnoreColumns []string `toml:"ignore-columns"`
	// field should be the primary key, unique key or field with index
	Fields string `toml:"index-fields"`
	// select range, for example: "age > 10 AND age < 20"
	Range string `toml:"range"`
	// set true if comparing sharding tables with target table, should have more than one source tables.
	IsSharding bool `toml:"is-sharding"`

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

	return true
}

// DataSource represents the Source Config.
type DataSource struct {
	Host     string `toml:"host" json:"host"`
	Port     int    `toml:"port" json:"port"`
	User     string `toml:"user" json:"user"`
	Password string `toml:"password" json:"password"`
	SqlMode  string `toml:"sql-mode" json:"sql-mode"`
	Snapshot string `toml:"snapshot" json:"snapshot"`

	// SourceType string `toml:"source-type" json:"source-type"`
}

func (d *DataSource) ToDBConfig() *dbutil.DBConfig {
	return &dbutil.DBConfig{
		Host:     d.Host,
		Port:     d.Port,
		User:     d.User,
		Password: d.Password,
		Snapshot: d.Snapshot,
	}
}

type TaskConfig struct {
	Source map[string]map[string][]string `toml:"source" json:"source"`
	Target map[string]map[string][]string `toml:"target" json:"target"`
	// OutputDir include these
	// 1. checkpoint Dir
	// 2. fix-target-sql Dir
	// 3. summary file
	// 4. sync diff log file
	// 5. fix
	OutputDir string `toml:"output-dir" json:"output-dir"`

	SourceInstances    []*DataSource
	SourceRoute        *router.Table
	TargetInstance     *DataSource
	TargetCheckTables  filter.Filter
	TargetTableConfigs []*TableConfig

	FixDir        string
	CheckpointDir string
	HashFile      string
}

func (t *TaskConfig) Init(
	dataSources map[string]*DataSource,
	routes map[string]*router.TableRule,
	tableConfigs map[string]*TableConfig,
) (err error) {
	// Parse Source/Target
	sourceMap, ok := t.Source["source"]
	if !ok {
		log.Fatal("not found source, please correct the config")
	}
	sourceInstances, ok := sourceMap["instances"]
	if !ok {
		log.Fatal("not found source instances, please correct the config")
	}
	dataSourceList := make([]*DataSource, 0, len(sourceInstances))
	for _, si := range sourceInstances {
		ds, ok := dataSources[si]
		if !ok {
			log.Fatal("not found source instance, please correct the config", zap.String("instance", si))
		}
		dataSourceList = append(dataSourceList, ds)
	}
	t.SourceInstances = dataSourceList

	routeRules, ok := sourceMap["routes"]
	if ok {
		// if we had rules
		routeRuleList := make([]*router.TableRule, 0, len(routeRules))
		for _, r := range routeRules {
			rr, ok := routes[r]
			if !ok {
				log.Fatal("not found source routes, please correct the config", zap.String("rule", r))
			}
			routeRuleList = append(routeRuleList, rr)
		}
		// t.SourceRoute can be nil, the caller should check it.
		t.SourceRoute, err = router.NewTableRouter(false, routeRuleList)
	}

	targetMap, ok := t.Target["target"]
	if !ok {
		log.Fatal("not found target, please correct the config")
	}
	targetInstance, ok := targetMap["instance"]
	if !ok {
		log.Fatal("not found target instances, please correct the config")
	}
	if len(targetInstance) != 1 {
		log.Fatal("only support one target instance for now")
	}
	ts, ok := dataSources[targetInstance[0]]
	if !ok {
		log.Fatal("not found target instance, please correct the config", zap.String("instance", targetInstance[0]))
	}
	t.TargetInstance = ts

	targetCheckTables, ok := targetMap["check-tables"]
	if !ok {
		log.Fatal("not found target check tables, please correct the config")
	}
	t.TargetCheckTables, err = filter.Parse(targetCheckTables)
	if err != nil {
		log.Fatal("parse check tables failed", zap.Error(err))
	}

	targetConfigs, ok := targetMap["configs"]
	if ok {
		// table config can be nil
		tableConfigsList := make([]*TableConfig, 0, len(targetConfigs))
		for _, c := range targetConfigs {
			tc, ok := tableConfigs[c]
			if !ok {
				log.Fatal("not found table config", zap.String("config", c))
			}
			tableConfigsList = append(tableConfigsList, tc)
		}
		t.TargetTableConfigs = tableConfigsList
	}

	// Create output Dir if not exists
	ok, err = pathExists(t.OutputDir)
	if err != nil {
		return errors.Trace(err)
	}
	if !ok {
		err = mkdirAll(t.OutputDir)
		if err != nil {
			return errors.Trace(err)
		}
	}

	hash, err := t.ComputeConfigHash()
	if err != nil {
		return errors.Trace(err)
	}

	target := targetInstance[0]
	t.FixDir = filepath.Join(t.OutputDir, hash, fmt.Sprintf("fix-on-%s", target))
	t.CheckpointDir = filepath.Join(t.OutputDir, hash, "checkpoint")
	return nil
}

// ComputeConfigHash compute the hash according to the task
// if ConfigHash is as same as checkpoint.hash
// we think the second sync diff can use the checkpoint.
func (t *TaskConfig) ComputeConfigHash() (string, error) {
	hash := make([]byte, 0)
	// compute sources
	for _, c := range t.SourceInstances {
		configBytes, err := json.Marshal(c)
		if err != nil {
			return "", errors.Trace(err)
		}
		hash = append(hash, configBytes...)
	}
	// compute target
	configBytes, err := json.Marshal(t.TargetInstance)
	if err != nil {
		return "", errors.Trace(err)
	}
	hash = append(hash, configBytes...)
	// compute check-tables and table config
	for _, c := range t.TargetTableConfigs {
		configBytes, err = json.Marshal(c)
		if err != nil {
			return "", errors.Trace(err)
		}
		hash = append(hash, configBytes...)
	}
	targetCheckTables, ok := t.Target["target"]["check-tables"]
	if !ok {
		log.Fatal("not found target check tables, please correct the config")
	}
	for _, c := range targetCheckTables {
		hash = append(hash, []byte(c)...)
	}

	return fmt.Sprintf("%x", md5.Sum(hash)), nil
}

// Config is the configuration.
type Config struct {
	*flag.FlagSet `json:"-"`

	// log level
	LogLevel string `toml:"log-level" json:"log-level"`
	// sampling check percent, for example 10 means only check 10% data
	Sample int `toml:"sample-percent" json:"sample-percent"`
	// how many goroutines are created to check data
	CheckThreadCount int `toml:"check-thread-count" json:"check-thread-count"`
	// set false if want to compare the data directly
	UseChecksum bool `toml:"use-checksum" json:"use-checksum"`
	// ignore check table's struct
	IgnoreStructCheck bool `toml:"ignore-struct-check" json:"ignore-struct-check"`
	// ignore tidb stats only use randomSpliter to split chunks
	IgnoreStats bool `toml:"ignore-stats" json:"ignore-stats"`
	// ignore check table's data
	IgnoreDataCheck bool `toml:"ignore-data-check" json:"ignore-data-check"`
	// set true will continue check from the latest checkpoint
	UseCheckpoint bool `toml:"use-checkpoint" json:"use-checkpoint"`

	DataSources map[string]*DataSource `toml:"data-sources" json:"data-sources"`

	Routes map[string]*router.TableRule `toml:"routes" json:"routes"`

	Task TaskConfig `toml:"task" json:"task"`

	TableConfigs map[string]*TableConfig `toml:"table-configs" json:"table-configs"`

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
	fs.IntVar(&cfg.Sample, "sample", 100, "the percent of sampling check")
	fs.IntVar(&cfg.CheckThreadCount, "check-thread-count", 1, "how many goroutines are created to check data")
	fs.BoolVar(&cfg.UseChecksum, "use-checksum", true, "set false if want to comapre the data directly")
	fs.BoolVar(&cfg.PrintVersion, "V", false, "print version of sync_diff_inspector")
	fs.BoolVar(&cfg.IgnoreDataCheck, "ignore-data-check", false, "ignore check table's data")
	fs.BoolVar(&cfg.IgnoreStructCheck, "ignore-struct-check", false, "ignore check table's struct")
	fs.BoolVar(&cfg.IgnoreStats, "ignore-stats", false, "don't use tidb stats to split chunks")
	fs.BoolVar(&cfg.UseCheckpoint, "use-checkpoint", true, "set true will continue check from the latest checkpoint")

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
	cfg, err := json.Marshal(c)
	if err != nil {
		return "<nil>"
	}
	return string(cfg)
}

// configFromFile loads config from file.
func (c *Config) configFromFile(path string) error {
	meta, err := toml.DecodeFile(path, c)
	if err != nil {
		return errors.Trace(err)
	}
	if len(meta.Undecoded()) > 0 {
		return errors.Errorf("unknown keys in config file %s: %v", path, meta.Undecoded())
	}
	return nil
}

func (c *Config) CheckConfig() bool {
	if c.Sample > percent100 || c.Sample < percent0 {
		log.Error("sample must be greater than 0 and less than or equal to 100!")
		return false
	}

	if c.CheckThreadCount <= 0 {
		log.Error("check-thread-count must greater than 0!")
		return false
	}

	err := c.Task.Init(c.DataSources, c.Routes, c.TableConfigs)
	if err != nil {
		log.Error("Task init failed", zap.Error(err))
		return false
	}

	return true
}

func pathExists(_path string) (bool, error) {
	_, err := os.Stat(_path)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, errors.Trace(err)
	}
	return true, nil
}

func mkdirAll(base string) error {
	mask := syscall.Umask(0)
	err := os.MkdirAll(base, LocalDirPerm)
	syscall.Umask(mask)
	return errors.Trace(err)
}
