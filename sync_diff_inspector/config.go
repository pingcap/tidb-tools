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
	"flag"
	"fmt"

	"github.com/BurntSushi/toml"
	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb-tools/pkg/dbutil"
	"github.com/pingcap/tidb/model"
)

const (
	percent0   = 0
	percent100 = 100
)

// TableCheckCfg is the config of table to be checked.
type TableCheckCfg struct {
	// table name
	Name string `toml:"name"`
	// Schema is seted in SourceDBCfg
	Schema string
	// field should be the primary key, unique key or field with index
	Field string `toml:"index-field"`
	// select range, for example: "age > 10 AND age < 20"
	Range string `toml:"range"`
	Info  *model.TableInfo
}

// Config is the configuration.
type Config struct {
	*flag.FlagSet `json:"-"`

	// log level
	LogLevel string `toml:"log-level" json:"log-level"`

	// source database's config
	SourceDBCfg dbutil.DBConfig `toml:"source-db" json:"source-db"`

	// target database's config
	TargetDBCfg dbutil.DBConfig `toml:"target-db" json:"target-db"`

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

	// the name of the file which saves sqls used to fix different data
	FixSQLFile string `toml:"fix-sql-file" json:"fix-sql-file"`

	// the config of table to be checked
	Tables []*TableCheckCfg `toml:"check-table" json:"check-table"`

	// the snapshot config of source database
	SourceSnapshot string `toml:"source-snapshot" json:"source-snapshot"`

	// the snapshot config of target database
	TargetSnapshot string `toml:"target-snapshot" json:"target-snapshot"`

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
	fs.StringVar(&cfg.FixSQLFile, "fix-sql-file", "fix.sql", "the name of the file which saves sqls used to fix different data")
	fs.StringVar(&cfg.SourceSnapshot, "source-snapshot", "", "source database's snapshot config")
	fs.StringVar(&cfg.TargetSnapshot, "target-snapshot", "", "target database's snapshot config")
	fs.BoolVar(&cfg.PrintVersion, "V", false, "print version of sync_diff_inspector")

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

	return true
}
