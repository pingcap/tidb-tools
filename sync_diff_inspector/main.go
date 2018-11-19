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
	"flag"
	"os"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb-tools/pkg/utils"
	log "github.com/sirupsen/logrus"
)

func main() {
	cfg := NewConfig()
	err := cfg.Parse(os.Args[1:])
	switch errors.Cause(err) {
	case nil:
	case flag.ErrHelp:
		os.Exit(0)
	default:
		log.Errorf("parse cmd flags err %s\n", errors.ErrorStack(err))
		os.Exit(2)
	}

	if cfg.PrintVersion {
		log.Infof("version: \n%s", utils.GetRawInfo("sync_diff_inspector"))
		return
	}

	logLevel, err := log.ParseLevel(cfg.LogLevel)
	if err != nil {
		log.Warnf("invalide log level %s", cfg.LogLevel)
		log.SetLevel(log.InfoLevel)
	} else {
		log.SetLevel(logLevel)
	}

	ok := cfg.checkConfig()
	if !ok {
		log.Error("there is something wrong with your config, please check it!")
		return
	}

	ctx := context.Background()

	if !checkSyncState(ctx, cfg) {
		log.Fatal("sourceDB don't equal targetDB")
	}
	log.Info("test pass!!!")
}

func checkSyncState(ctx context.Context, cfg *Config) bool {
	beginTime := time.Now()
	defer func() {
		log.Infof("check data finished, all cost %v", time.Since(beginTime))
	}()

	d, err := NewDiff(ctx, cfg)
	if err != nil {
		log.Fatalf("fail to initialize diff process %v", errors.ErrorStack(err))
	}

	err = d.Equal()
	if err != nil {
		log.Fatalf("check data difference error %v", errors.ErrorStack(err))
	}

	log.Info(d.report)

	return d.report.Result == Pass
}
