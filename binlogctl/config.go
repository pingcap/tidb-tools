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
	"crypto/tls"
	"flag"
	"os"

	"github.com/juju/errors"
	"github.com/pingcap/tidb-tools/generate_binlog_position/pkg"
)

const (
	defaultEtcdURLs = "http://127.0.0.1:2379"
	defaultDataDir  = "binlog_position"
)

const (
	generateSavepoint = "generate_savepoint"
	queryPumps        = "pumps"
	queryDrainer      = "drainers"
	unregisterPumps   = "delete-pump"
	unregisterDrainer = "delete-drainer"
)

// Config holds the configuration of drainer
type Config struct {
	*flag.FlagSet

	Command  string `toml:"cmd" json:"cmd"`
	NodeID   string `toml:"node-id" json:"node-id"`
	DataDir  string `toml:"data-dir" json:"data-dir"`
	TimeZone string `toml:"time-zone" json:"time-zone"`
	EtcdURLs string `toml:"pd-urls" json:"pd-urls"`
	SSLCA    string `toml:"ssl-ca" json:"ssl-ca"`
	SSLCert  string `toml:"ssl-cert" json:"ssl-cert"`
	SSLKey   string `toml:"ssl-key" json:"ssl-key"`
	tls      *tls.Config
}

// NewConfig return an instance of configuration
func NewConfig() *Config {
	cfg := &Config{}
	cfg.FlagSet = flag.NewFlagSet("generate_binlog_position", flag.ContinueOnError)
	fs := cfg.FlagSet

	fs.StringVar(&cfg.Command, "cmd", "pumps", "operator: \"generate_savepoint\", \"pumps\", \"drainers\", \"delete-pump\", \"delete-drainer\"")
	fs.StringVar(&cfg.NodeID, "node-id", "", "name of service, use to delete some service with operation delete-pump and delete-drainer")
	fs.StringVar(&cfg.DataDir, "data-dir", defaultDataDir, "binlog position data directory path (default data.drainer)")
	fs.StringVar(&cfg.EtcdURLs, "pd-urls", defaultEtcdURLs, "a comma separated list of PD endpoints")
	fs.StringVar(&cfg.SSLCA, "ssl-ca", "", "Path of file that contains list of trusted SSL CAs for connection with cluster components.")
	fs.StringVar(&cfg.SSLCert, "ssl-cert", "", "Path of file that contains X509 certificate in PEM format for connection with cluster components.")
	fs.StringVar(&cfg.SSLKey, "ssl-key", "", "Path of file that contains X509 key in PEM format for connection with cluster components.")
	fs.StringVar(&cfg.TimeZone, "time-zone", "", "set time zone if you want save time info in savepoint file, for example `Asia/Shanghai` for CST time, `Local` for local time")

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

	var err error
	// transfore tls config
	cfg.tls, err = pkg.ToTLSConfig(cfg.SSLCA, cfg.SSLCert, cfg.SSLKey)
	if err != nil {
		return errors.Errorf("tls config error %v", err)
	}

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
	_, err := pkg.NewURLsValue(cfg.EtcdURLs)
	if err != nil {
		return errors.Errorf("parse EtcdURLs error: %s, %v", cfg.EtcdURLs, err)
	}
	return nil
}
