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
	"github.com/pingcap/tidb-tools/sync_check/util"
	"github.com/pingcap/tidb/model"
)

const (
	percent0   = 0
	percent100 = 100
)

// TableCheckCfg is the config of table to be checked.
type TableCheckCfg struct {
	Name   string `toml:"name"`
	Schema string
	Field  string `toml:"field"`
	Range  string `toml:"range"`
	Info   *model.TableInfo
}

// Config is the configuration.
type Config struct {
	*flag.FlagSet `json:"-"`

	LogLevel string `toml:"log-level" json:"log-level"`

	SourceDBCfg util.DBConfig `toml:"source-db" json:"source-db"`

	TargetDBCfg util.DBConfig `toml:"target-db" json:"target-db"`

	ChunkSize int `toml:"chunk-size" json:"chunk-size"`

	Sample int `toml:"sample" json:"sample"`

	CheckThCount int `toml:"check-thcount" json:"check-thcount"`

	UseRowID bool `toml:"use-rowid" json:"use-rowid"`

	FixSQLFile string `toml:"fix-sql-file" json:"fix-sql-file"`

	Tables []*TableCheckCfg `toml:"check-table" json:"check-table"`

	ConfigFile string
}

// NewConfig creates a new config.
func NewConfig() *Config {
	cfg := &Config{}
	cfg.FlagSet = flag.NewFlagSet("sync-check", flag.ContinueOnError)
	fs := cfg.FlagSet

	fs.StringVar(&cfg.ConfigFile, "config", "", "Config file")
	fs.StringVar(&cfg.LogLevel, "L", "info", "log level: debug, info, warn, error, fatal")
	fs.IntVar(&cfg.ChunkSize, "chunk-size", 1000, "diff check chunk size")
	fs.IntVar(&cfg.Sample, "sample", 100, "the percent of sampling check")
	fs.IntVar(&cfg.CheckThCount, "check-thcount", 1, "the count of check thread count")
	fs.BoolVar(&cfg.UseRowID, "use-rowid", false, "set true if target-db and source-db all support tidb implicit column _tidb_rowid")
	fs.StringVar(&cfg.FixSQLFile, "fix-sql-file", "fix.sql", "the name of file which saves sqls used to fix different data")

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

	if c.CheckThCount <= 0 {
		log.Errorf("check-thcount must greater than 0!")
		return false
	}

	return true
}
