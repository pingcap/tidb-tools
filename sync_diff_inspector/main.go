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
	"database/sql"
	"flag"
	"os"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb-tools/pkg/db"
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

	sourceDB, err := pkgdb.CreateDB(cfg.SourceDBCfg, cfg.SourceSnapshot)
	if err != nil {
		log.Fatal(err)
	}
	defer pkgdb.CloseDB(sourceDB)

	targetDB, err := pkgdb.CreateDB(cfg.TargetDBCfg, cfg.TargetSnapshot)
	if err != nil {
		log.Fatal(err)
	}
	defer pkgdb.CloseDB(targetDB)

	if !checkSyncState(sourceDB, targetDB, cfg) {
		log.Fatal("sourceDB don't equal targetDB")
	}
	log.Info("test pass!!!")
}

func checkSyncState(sourceDB, targetDB *sql.DB, cfg *Config) bool {
	beginTime := time.Now()
	defer func() {
		log.Infof("check data finished, all cost %v", time.Since(beginTime))
	}()

	d, err := NewDiff(sourceDB, targetDB, cfg)
	if err != nil {
		log.Fatalf("fail to initialize diff process %v", errors.ErrorStack(err))
	}

	ok, err := d.Equal()
	if err != nil {
		log.Fatalf("check data difference error %v", errors.ErrorStack(err))
	}

	return ok
}
