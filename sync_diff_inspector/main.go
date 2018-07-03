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
	"database/sql"
	"flag"
	"os"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb-tools/pkg/dbutil"
	"github.com/pingcap/tidb-tools/pkg/utils"
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

	log.SetLevelByString(cfg.LogLevel)

	ok := cfg.checkConfig()
	if !ok {
		log.Error("there is something wrong with your config, please check it!")
		return
	}

	ctx := context.Background()

	sourceDB, err := dbutil.OpenDB(cfg.SourceDBCfg)
	if err != nil {
		log.Fatalf("create source db %+v error %v", cfg.SourceDBCfg, err)
	}
	defer dbutil.CloseDB(sourceDB)
	if cfg.SourceSnapshot != "" {
		err = dbutil.SetSnapshot(ctx, sourceDB, cfg.SourceSnapshot)
		if err != nil {
			log.Fatalf("set history snapshot %s for source db %+v error %v", cfg.SourceSnapshot, cfg.SourceDBCfg, err)
		}
	}

	targetDB, err := dbutil.OpenDB(cfg.TargetDBCfg)
	if err != nil {
		log.Fatalf("create target db %+v error %v", cfg.TargetDBCfg, err)
	}
	defer dbutil.CloseDB(targetDB)
	if cfg.TargetSnapshot != "" {
		err = dbutil.SetSnapshot(ctx, targetDB, cfg.TargetSnapshot)
		if err != nil {
			log.Fatalf("set history snapshot %s for target db %+v error %v", cfg.TargetSnapshot, cfg.TargetDBCfg, err)
		}
	}

	if !checkSyncState(ctx, sourceDB, targetDB, cfg) {
		log.Fatal("sourceDB don't equal targetDB")
	}
	log.Info("test pass!!!")
}

func checkSyncState(ctx context.Context, sourceDB, targetDB *sql.DB, cfg *Config) bool {
	beginTime := time.Now()
	defer func() {
		log.Infof("check data finished, all cost %v", time.Since(beginTime))
	}()

	d, err := NewDiff(ctx, sourceDB, targetDB, cfg)
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
