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
	"crypto/tls"
	"flag"
	"fmt"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb-tools/pkg/utils"
)

const (
	defaultEtcdURLs = "http://127.0.0.1:2379"
	defaultDataDir  = "binlog_position"
)

const (
	generateMeta   = "generate_meta"
	queryPumps     = "pumps"
	queryDrainers  = "drainers"
	updatePump     = "update-pump"
	updateDrainer  = "update-drainer"
	pausePump      = "pause-pump"
	offlinePump    = "offline-pump"
	pauseDrainer   = "pause-drainer"
	offlineDrainer = "offline-drainer"
)

// Config holds the configuration of drainer
type Config struct {
	*flag.FlagSet

	Command      string `toml:"cmd" json:"cmd"`
	NodeID       string `toml:"node-id" json:"node-id"`
	DataDir      string `toml:"data-dir" json:"data-dir"`
	TimeZone     string `toml:"time-zone" json:"time-zone"`
	EtcdURLs     string `toml:"pd-urls" json:"pd-urls"`
	SSLCA        string `toml:"ssl-ca" json:"ssl-ca"`
	SSLCert      string `toml:"ssl-cert" json:"ssl-cert"`
	SSLKey       string `toml:"ssl-key" json:"ssl-key"`
	State        string `toml:"state" json:"state"`
	tls          *tls.Config
	printVersion bool
}

// NewConfig returns an instance of configuration
func NewConfig() *Config {
	cfg := &Config{}
	cfg.FlagSet = flag.NewFlagSet("binlogctl", flag.ContinueOnError)

	cfg.FlagSet.StringVar(&cfg.Command, "cmd", "pumps", "operator: \"generate_meta\", \"pumps\", \"drainers\", \"update-pump\", \"update-drainer\", \"pause-pump\", \"pause-drainer\", \"offline-pump\", \"offline-drainer\"")
	cfg.FlagSet.StringVar(&cfg.NodeID, "node-id", "", "id of node, use to update some node with operation update-pump, update-drainer, pause-pump, pause-drainer, offline-pump and offline-drainer")
	cfg.FlagSet.StringVar(&cfg.DataDir, "data-dir", defaultDataDir, "meta directory path")
	cfg.FlagSet.StringVar(&cfg.EtcdURLs, "pd-urls", defaultEtcdURLs, "a comma separated list of PD endpoints")
	cfg.FlagSet.StringVar(&cfg.SSLCA, "ssl-ca", "", "Path of file that contains list of trusted SSL CAs for connection with cluster components.")
	cfg.FlagSet.StringVar(&cfg.SSLCert, "ssl-cert", "", "Path of file that contains X509 certificate in PEM format for connection with cluster components.")
	cfg.FlagSet.StringVar(&cfg.SSLKey, "ssl-key", "", "Path of file that contains X509 key in PEM format for connection with cluster components.")
	cfg.FlagSet.StringVar(&cfg.TimeZone, "time-zone", "", "set time zone if you want save time info in savepoint file, for example `Asia/Shanghai` for CST time, `Local` for local time")
	cfg.FlagSet.StringVar(&cfg.State, "state", "", "set node's state, can set to online, pausing, paused, closing or offline.")
	cfg.FlagSet.BoolVar(&cfg.printVersion, "V", false, "prints version and exit")

	return cfg
}

// Parse parses all config from command-line flags, environment vars or the configuration file
func (cfg *Config) Parse(args []string) error {
	// parse first to get config file
	err := cfg.FlagSet.Parse(args)
	if err != nil {
		return errors.Trace(err)
	}
	// parse command line options
	if len(cfg.FlagSet.Args()) > 0 {
		return errors.Errorf("'%s' is not a valid flag", cfg.FlagSet.Arg(0))
	}

	if cfg.printVersion {
		fmt.Printf(utils.GetRawInfo("binlogctl"))
		return flag.ErrHelp
	}

	// adjust configuration
	adjustString(&cfg.DataDir, defaultDataDir)

	// transfore tls config
	cfg.tls, err = utils.ToTLSConfig(cfg.SSLCA, cfg.SSLCert, cfg.SSLKey)
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
	_, err := utils.ParseHostPortAddr(cfg.EtcdURLs)
	if err != nil {
		return errors.Errorf("parse EtcdURLs error: %s, %v", cfg.EtcdURLs, err)
	}
	return nil
}
