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

package config

import (
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"syscall"

	"github.com/BurntSushi/toml"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb-tools/pkg/dbutil"
	filter "github.com/pingcap/tidb-tools/pkg/table-filter"
	router "github.com/pingcap/tidb-tools/pkg/table-router"
	"github.com/pingcap/tidb/parser/model"
	flag "github.com/spf13/pflag"
	"go.uber.org/zap"
)

const (
	LocalDirPerm  os.FileMode = 0o755
	LocalFilePerm os.FileMode = 0o644

	LogFileName = "sync_diff.log"
)

// TableConfig is the config of table.
type TableConfig struct {
	// table's filter to tell us which table should adapt to this config.
	TargetTables []string `toml:"target-tables" json:"target-tables"`
	// Internally used to indicate which specified table in target is using this config.
	Schema string
	Table  string
	// Internally used to distinguish different config.
	ConfigIndex int
	// Internally used to valid config.
	HasMatched bool

	// columns be ignored, will not check this column's data
	IgnoreColumns []string `toml:"ignore-columns"`
	// field should be the primary key, unique key or field with index
	Fields []string `toml:"index-fields"`
	// select range, for example: "age > 10 AND age < 20"
	Range string `toml:"range"`

	TargetTableInfo *model.TableInfo

	// collation config in mysql/tidb
	Collation string `toml:"collation"`

	// specify the chunksize for the table
	ChunkSize int64 `toml:"chunk-size" json:"chunk-size"`
}

// Valid returns true if table's config is valide.
func (t *TableConfig) Valid() bool {
	if len(t.TargetTables) == 0 {
		log.Error("target tables can't be empty in TableConfig")
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

	RouteRules []string `toml:"route-rules" json:"route-rules"`
	Router     *router.Table

	Conn *sql.DB
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
	Source       []string `toml:"source-instances" json:"source-instances"`
	Routes       []string `toml:"source-routes" json:"source-routes"`
	Target       string   `toml:"target-instance" json:"target-instance"`
	CheckTables  []string `toml:"target-check-tables" json:"target-check-tables"`
	TableConfigs []string `toml:"target-configs" json:"target-configs"`
	// OutputDir include these
	// 1. checkpoint Dir
	// 2. fix-target-sql Dir
	// 3. summary file
	// 4. sync diff log file
	// 5. fix
	OutputDir string `toml:"output-dir" json:"output-dir"`

	SourceInstances    []*DataSource
	TargetInstance     *DataSource
	TargetTableConfigs []*TableConfig
	TargetCheckTables  filter.Filter

	FixDir        string
	CheckpointDir string
	HashFile      string
}

func (t *TaskConfig) Init(
	dataSources map[string]*DataSource,
	tableConfigs map[string]*TableConfig,
) (err error) {
	// Parse Source/Target
	dataSourceList := make([]*DataSource, 0, len(t.Source))
	for _, si := range t.Source {
		ds, ok := dataSources[si]
		if !ok {
			log.Error("not found source instance, please correct the config", zap.String("instance", si))
			return errors.Errorf("not found source instance, please correct the config. instance is `%s`", si)
		}
		dataSourceList = append(dataSourceList, ds)
	}
	t.SourceInstances = dataSourceList

	ts, ok := dataSources[t.Target]
	if !ok {
		log.Error("not found target instance, please correct the config", zap.String("instance", t.Target))
		return errors.Errorf("not found target instance, please correct the config. instance is `%s`", t.Target)
	}
	t.TargetInstance = ts

	t.TargetCheckTables, err = filter.Parse(t.CheckTables)
	if err != nil {
		log.Error("parse check tables failed", zap.Error(err))
		return errors.Annotate(err, "parse check tables failed")
	}

	targetConfigs := t.TableConfigs
	if targetConfigs != nil {
		// table config can be nil
		tableConfigsList := make([]*TableConfig, 0, len(targetConfigs))
		for configIndex, c := range targetConfigs {
			tc, ok := tableConfigs[c]
			if !ok {
				log.Error("not found table config", zap.String("config", c))
				return errors.Errorf("not found table config. config is `%s`", c)
			}
			tc.ConfigIndex = configIndex
			tableConfigsList = append(tableConfigsList, tc)
		}
		t.TargetTableConfigs = tableConfigsList
	}

	hash, err := t.ComputeConfigHash()
	if err != nil {
		return errors.Trace(err)
	}

	// Create output Dir if not exists
	if t.OutputDir == "" {
		return errors.New("output-dir is missing from the task configuration")
	}

	ok, err = pathExists(t.OutputDir)
	if err != nil {
		return errors.Trace(err)
	}
	if !ok {
		if err = mkdirAll(t.OutputDir); err != nil {
			return errors.Trace(err)
		}
	}
	// outputDir exists, we need to check the config hash for checkpoint.
	t.CheckpointDir = filepath.Join(t.OutputDir, "checkpoint")
	ok, err = pathExists(t.CheckpointDir)
	if err != nil {
		return errors.Trace(err)
	}
	if !ok {
		// no checkpoint, we can use this outputDir directly.
		if err = mkdirAll(t.CheckpointDir); err != nil {
			return errors.Trace(err)
		}
		// create config hash in checkpointDir.
		err = os.WriteFile(filepath.Join(t.CheckpointDir, hash), []byte{}, LocalFilePerm)
		if err != nil {
			return errors.Trace(err)
		}
	} else {
		// checkpoint exists, we need compare the config hash.
		ok, err = pathExists(filepath.Join(t.CheckpointDir, hash))
		if err != nil {
			return errors.Trace(err)
		}
		if !ok {
			// not match, raise error
			return errors.Errorf("config changes breaking the checkpoint, please use another outputDir and start over again!")
		}
	}

	t.FixDir = filepath.Join(t.OutputDir, fmt.Sprintf("fix-on-%s", t.Target))
	if err = mkdirAll(t.FixDir); err != nil {
		return errors.Trace(err)
	}

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
	targetCheckTables := t.CheckTables
	for _, c := range targetCheckTables {
		hash = append(hash, []byte(c)...)
	}

	return fmt.Sprintf("%x", sha256.Sum256(hash)), nil
}

// Config is the configuration.
type Config struct {
	*flag.FlagSet `json:"-"`

	// log level
	LogLevel string `toml:"-" json:"-"`
	// how many goroutines are created to check data
	CheckThreadCount int `toml:"check-thread-count" json:"check-thread-count"`
	// set true if want to compare rows
	// set false won't compare rows.
	ExportFixSQL bool `toml:"export-fix-sql" json:"export-fix-sql"`
	// only check table struct without table data.
	CheckStructOnly bool `toml:"check-struct-only" json:"check-struct-only"`
	// DMAddr is dm-master's address, the format should like "http://127.0.0.1:8261"
	DMAddr string `toml:"dm-addr" json:"dm-addr"`
	// DMTask string `toml:"dm-task" json:"dm-task"`
	DMTask string `toml:"dm-task" json:"dm-task"`

	DataSources map[string]*DataSource `toml:"data-sources" json:"data-sources"`

	Routes map[string]*router.TableRule `toml:"routes" json:"routes"`

	TableConfigs map[string]*TableConfig `toml:"table-configs" json:"table-configs"`

	Task TaskConfig `toml:"task" json:"task"`
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

	fs.BoolVarP(&cfg.PrintVersion, "version", "V", false, "print version of sync_diff_inspector")
	fs.StringVarP(&cfg.LogLevel, "log-level", "L", "info", "log level: debug, info, warn, error, fatal")
	fs.StringVarP(&cfg.ConfigFile, "config", "C", "", "Config file")
	fs.StringVar(&cfg.DMAddr, "dm-addr", "", "the address of DM")
	fs.StringVar(&cfg.DMTask, "dm-task", "", "identifier of dm task")
	fs.IntVar(&cfg.CheckThreadCount, "check-thread-count", 1, "how many goroutines are created to check data")
	fs.BoolVar(&cfg.ExportFixSQL, "export-fix-sql", true, "set true if want to compare rows or set to false will only compare checksum")
	fs.BoolVar(&cfg.CheckStructOnly, "check-struct-only", false, "ignore check table's data")

	fs.SortFlags = false
	return cfg
}

// Parse parses flag definitions from the argument list.
func (c *Config) Parse(arguments []string) error {
	// Parse first to get config file.
	err := c.FlagSet.Parse(arguments)
	if err != nil {
		return errors.Trace(err)
	}

	if c.PrintVersion {
		return nil
	}

	// Load config file if specified.
	if c.ConfigFile == "" {
		return errors.Errorf("argument --config is required")
	}
	err = c.configFromFile(c.ConfigFile)
	if err != nil {
		return errors.Trace(err)
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

func (c *Config) adjustConfigByDMSubTasks() (err error) {
	// DM's subtask config
	subTaskCfgs, err := getDMTaskCfg(c.DMAddr, c.DMTask)
	if err != nil {
		log.Warn("failed to get config from DM tasks")
		return errors.Trace(err)
	}
	sqlMode := ""
	if subTaskCfgs[0].EnableANSIQuotes {
		sqlMode = "ANSI_QUOTES"
	}
	dataSources := make(map[string]*DataSource)
	dataSources["target"] = &DataSource{
		Host:     subTaskCfgs[0].To.Host,
		Port:     subTaskCfgs[0].To.Port,
		User:     subTaskCfgs[0].To.User,
		Password: subTaskCfgs[0].To.Password,
		SqlMode:  sqlMode,
	}
	for _, subTaskCfg := range subTaskCfgs {
		tableRouter, err := router.NewTableRouter(subTaskCfg.CaseSensitive, []*router.TableRule{})
		if err != nil {
			return errors.Trace(err)
		}
		for _, rule := range subTaskCfg.RouteRules {
			err := tableRouter.AddRule(rule)
			if err != nil {
				return errors.Trace(err)
			}
		}
		dataSources[subTaskCfg.SourceID] = &DataSource{
			Host:     subTaskCfg.From.Host,
			Port:     subTaskCfg.From.Port,
			User:     subTaskCfg.From.User,
			Password: subTaskCfg.From.Password,
			SqlMode:  sqlMode,
			Router:   tableRouter,
		}
	}
	c.DataSources = dataSources
	c.Task.Target = "target"
	for id := range dataSources {
		if id == "target" {
			continue
		}
		c.Task.Source = append(c.Task.Source, id)
	}
	return nil
}

func (c *Config) Init() (err error) {
	if len(c.DMAddr) > 0 {
		err := c.adjustConfigByDMSubTasks()
		if err != nil {
			return errors.Annotate(err, "failed to init Task")
		}
		err = c.Task.Init(c.DataSources, c.TableConfigs)
		if err != nil {
			return errors.Annotate(err, "failed to init Task")
		}
		return nil
	}
	for _, d := range c.DataSources {
		routeRuleList := make([]*router.TableRule, 0, len(c.Routes))
		// if we had rules
		for _, r := range d.RouteRules {
			rr, ok := c.Routes[r]
			if !ok {
				return errors.Errorf("not found source routes for rule %s, please correct the config", r)
			}
			routeRuleList = append(routeRuleList, rr)
		}
		// t.SourceRoute can be nil, the caller should check it.
		d.Router, err = router.NewTableRouter(false, routeRuleList)
		if err != nil {
			return errors.Annotate(err, "failed to build route config")
		}
	}

	err = c.Task.Init(c.DataSources, c.TableConfigs)
	if err != nil {
		return errors.Annotate(err, "failed to init Task")
	}
	return nil
}

func (c *Config) CheckConfig() bool {
	if c.CheckThreadCount <= 0 {
		log.Error("check-thread-count must greater than 0!")
		return false
	}
	if len(c.DMAddr) != 0 {
		u, err := url.Parse(c.DMAddr)
		if err != nil || u.Scheme == "" || u.Host == "" {
			log.Error("dm-addr's format should like 'http://127.0.0.1:8261'")
			return false
		}

		if len(c.DMTask) == 0 {
			log.Error("must set the `dm-task` if set `dm-addr`")
			return false
		}
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
