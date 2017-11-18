// Copyright 2016 PingCAP, Inc.
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
	"os"

	"github.com/juju/errors"
)

const (
	defaultEtcdURLs = "http://127.0.0.1:2379"
	defaultDataDir  = "binlog_position"
)

// Config holds the configuration of drainer
type Config struct {
	*flag.FlagSet
	DataDir  string `toml:"data-dir" json:"data-dir"`
	EtcdURLs string `toml:"pd-urls" json:"pd-urls"`
}

// NewConfig return an instance of configuration
func NewConfig() *Config {
	cfg := &Config{}
	cfg.FlagSet = flag.NewFlagSet("generate_binlog_position", flag.ContinueOnError)
	fs := cfg.FlagSet
	fs.StringVar(&cfg.DataDir, "data-dir", defaultDataDir, "binlog position data directory path (default data.drainer)")
	fs.StringVar(&cfg.EtcdURLs, "pd-urls", defaultEtcdURLs, "a comma separated list of PD endpoints")
	return cfg
}

// Parse parses all config from command-line flags, environment vars or the configuration file
func (cfg *Config) Parse(args []string) error {
	// parse first to get config file
	perr := cfg.FlagSet.Parse(args)
	switch perr {
	case nil:
	case flag.ErrHelp:
		os.Exit(0)
	default:
		os.Exit(2)
	}

	// parse command line options
	cfg.FlagSet.Parse(args)
	if len(cfg.FlagSet.Args()) > 0 {
		return errors.Errorf("'%s' is not a valid flag", cfg.FlagSet.Arg(0))
	}
	// adjust configuration
	adjustString(&cfg.DataDir, defaultDataDir)
	return cfg.validate()
}

func adjustString(v *string, defValue string) {
	if len(*v) == 0 {
		*v = defValue
	}
}

// validate checks whether the configuration is valid
func (cfg *Config) validate() error {
	// check EtcdEndpoints
	_, err := NewURLsValue(cfg.EtcdURLs)
	if err != nil {
		return errors.Errorf("parse EtcdURLs error: %s, %v", cfg.EtcdURLs, err)
	}
	return nil
}
