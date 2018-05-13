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
	"database/sql"
	"flag"
	"os"
	//"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb-tools/sync_check/util"
)

const dateTimeFormat = "2006-01-02 15:04:05"

func main() {
	cfg := NewConfig()
	err := cfg.Parse(os.Args[1:])
	switch errors.Cause(err) {
	case nil:
	case flag.ErrHelp:
		os.Exit(0)
	default:
		log.Errorf("parse cmd flags err %s\n", errors.Trace(err))
		os.Exit(2)
	}

	ok := cfg.checkConfig()
	if !ok {
		log.Error("there is something wrong with your config, please check it!")
		return
	}

	sourceDB, err := util.CreateDB(cfg.SourceDBCfg)
	if err != nil {
		log.Fatal(err)
	}
	defer util.CloseDB(sourceDB)

	targetDB, err := util.CreateDB(cfg.TargetDBCfg)
	if err != nil {
		log.Fatal(err)
	}
	defer util.CloseDB(targetDB)

	if !checkSyncState(sourceDB, targetDB, cfg) {
		log.Fatal("sourceDB don't equal targetDB")
	}
	log.Info("test pass!!!")
}

func checkSyncState(sourceDB, targetDB *sql.DB, cfg *Config) bool {
	/*
		beginTime := ""
		endTime := ""

		if cfg.Delay != 0 {
			endTime = time.Now().Add(time.Duration(-cfg.Delay) * time.Second).Format(dateTimeFormat)
		} else {
			if cfg.EndTime != "" {
				endTime = cfg.EndTime
			}
			if cfg.BeginTime != "" {
				beginTime = cfg.BeginTime
			}
		}
	*/

	d := NewDiff(sourceDB, targetDB, cfg.SourceDBCfg.Name, cfg.ChunkSize, cfg.Sample, cfg.CheckThCount, cfg.UseRowID, cfg.Tables)
	ok, err := d.Equal()
	if err != nil {
		log.Fatal(err)
	}

	return ok
}
