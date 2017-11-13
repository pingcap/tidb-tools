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
	"fmt"
	"net"
	"net/url"
	"os"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/juju/errors"
)

const (
	defaultListenAddr     = "127.0.0.1:8249"
	defaultDataDir        = "data.drainer"
	defaultDetectInterval = 10
	defaultEtcdURLs       = "http://127.0.0.1:2379"
	// defaultEtcdTimeout defines the timeout of dialing or sending request to etcd.
	defaultEtcdTimeout = 5 * time.Second
	defaultPumpTimeout = 5 * time.Second
)

// SyncerConfig is the Syncer's configuration.
type SyncerConfig struct {
	IgnoreSchemas    string   `toml:"ignore-schemas" json:"ignore-schemas"`
	TxnBatch         int      `toml:"txn-batch" json:"txn-batch"`
	WorkerCount      int      `toml:"worker-count" json:"worker-count"`
	DoDBs            []string `toml:"replicate-do-db" json:"replicate-do-db"`
	DestDBType       string   `toml:"db-type" json:"db-type"`
	DisableDispatch  bool     `toml:"disable-dispatch" json:"disable-dispatch"`
	SafeMode         bool     `toml:"safe-mode" json:"safe-mode"`
	DisableCausality bool     `toml:"disable-detect" json:"disable-detect"`
}

// Config holds the configuration of drainer
type Config struct {
	*flag.FlagSet
	LogLevel        string        `toml:"log-level" json:"log-level"`
	ListenAddr      string        `toml:"addr" json:"addr"`
	DataDir         string        `toml:"data-dir" json:"data-dir"`
	DetectInterval  int           `toml:"detect-interval" json:"detect-interval"`
	EtcdURLs        string        `toml:"pd-urls" json:"pd-urls"`
	LogFile         string        `toml:"log-file" json:"log-file"`
	LogRotate       string        `toml:"log-rotate" json:"log-rotate"`
	SyncerCfg       *SyncerConfig `toml:"syncer" json:"sycner"`
	EtcdTimeout     time.Duration
	PumpTimeout     time.Duration
	MetricsAddr     string
	MetricsInterval int
	configFile      string
	printVersion    bool
	GenSavepoint    bool
}

// NewConfig return an instance of configuration
func NewConfig() *Config {
	cfg := &Config{
		EtcdTimeout: defaultEtcdTimeout,
		PumpTimeout: defaultPumpTimeout,
	}
	cfg.FlagSet = flag.NewFlagSet("drainer", flag.ContinueOnError)
	fs := cfg.FlagSet
	fs.Usage = func() {
		fmt.Fprintln(os.Stderr, "Usage of drainer:")
		fs.PrintDefaults()
	}
	fs.StringVar(&cfg.ListenAddr, "addr", defaultListenAddr, "addr (i.e. 'host:port') to listen on for drainer connections")
	fs.StringVar(&cfg.DataDir, "data-dir", defaultDataDir, "drainer data directory path (default data.drainer)")
	fs.IntVar(&cfg.DetectInterval, "detect-interval", defaultDetectInterval, "the interval time (in seconds) of detect pumps' status")
	fs.StringVar(&cfg.EtcdURLs, "pd-urls", defaultEtcdURLs, "a comma separated list of PD endpoints")
	fs.StringVar(&cfg.LogLevel, "L", "info", "log level: debug, info, warn, error, fatal")
	fs.StringVar(&cfg.configFile, "config", "", "path to the configuration file")
	fs.BoolVar(&cfg.printVersion, "V", false, "print version info")
	fs.BoolVar(&cfg.GenSavepoint, "gen-savepoint", false, "generate savepoint from cluster")
	fs.StringVar(&cfg.MetricsAddr, "metrics-addr", "", "prometheus pushgateway address, leaves it empty will disable prometheus push")
	fs.IntVar(&cfg.MetricsInterval, "metrics-interval", 15, "prometheus client push interval in second, set \"0\" to disable prometheus push")
	fs.StringVar(&cfg.LogFile, "log-file", "", "log file path")
	fs.StringVar(&cfg.LogRotate, "log-rotate", "", "log file rotate type, hour/day")
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

	// load config file if specified
	if cfg.configFile != "" {
		if err := cfg.configFromFile(cfg.configFile); err != nil {
			return errors.Trace(err)
		}
	}
	// parse again to replace with command line options
	cfg.FlagSet.Parse(args)
	if len(cfg.FlagSet.Args()) > 0 {
		return errors.Errorf("'%s' is not a valid flag", cfg.FlagSet.Arg(0))
	}
	// adjust configuration
	adjustString(&cfg.ListenAddr, defaultListenAddr)
	cfg.ListenAddr = "http://" + cfg.ListenAddr // add 'http:' scheme to facilitate parsing
	adjustString(&cfg.DataDir, defaultDataDir)
	adjustInt(&cfg.DetectInterval, defaultDetectInterval)

	return cfg.validate()
}

func (cfg *Config) configFromFile(path string) error {
	_, err := toml.DecodeFile(path, cfg)
	return errors.Trace(err)
}

func adjustString(v *string, defValue string) {
	if len(*v) == 0 {
		*v = defValue
	}
}

func adjustInt(v *int, defValue int) {
	if *v == 0 {
		*v = defValue
	}
}

// validate checks whether the configuration is valid
func (cfg *Config) validate() error {
	// check ListenAddr
	urllis, err := url.Parse(cfg.ListenAddr)
	if err != nil {
		return errors.Errorf("parse ListenAddr error: %s, %v", cfg.ListenAddr, err)
	}
	if _, _, err = net.SplitHostPort(urllis.Host); err != nil {
		return errors.Errorf("bad ListenAddr host format: %s, %v", urllis.Host, err)
	}
	// check EtcdEndpoints
	_, err = NewURLsValue(cfg.EtcdURLs)
	if err != nil {
		return errors.Errorf("parse EtcdURLs error: %s, %v", cfg.EtcdURLs, err)
	}
	return nil
}
