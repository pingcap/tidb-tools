// Copyright 2021 PingCAP, Inc.
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
	"fmt"
	"os"
	"path/filepath"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb-tools/pkg/utils"
	"github.com/pingcap/tidb-tools/sync_diff_inspector/config"
	flag "github.com/spf13/pflag"
	"go.uber.org/zap"
)

func main() {
	cfg := config.NewConfig()
	err := cfg.Parse(os.Args[1:])
	switch errors.Cause(err) {
	case nil:
	case flag.ErrHelp:
		os.Exit(0)
	default:
		log.Error("parse cmd flags", zap.Error(err))
		os.Exit(2)
	}

	if cfg.PrintVersion {
		fmt.Printf("version: \n%s", utils.GetRawInfo("sync_diff_inspector"))
		return
	}

	conf := new(log.Config)
	conf.Level = cfg.LogLevel

	conf.File.Filename = filepath.Join(cfg.Task.OutputDir, config.LogFileName)
	lg, p, e := log.InitLogger(conf)
	if e != nil {
		log.Error("Log init failed!", zap.String("error", e.Error()))
		return
	}
	log.ReplaceGlobals(lg, p)

	utils.PrintInfo("sync_diff_inspector")

	// Initial config
	err = cfg.Init()
	if err != nil {
		fmt.Printf("Fail to initialize config.\n%s\n", err.Error())
		return
	}

	ok := cfg.CheckConfig()
	if !ok {
		fmt.Printf("There is something wrong with your config, please check log info in %s\n", conf.File.Filename)
		return
	}

	log.Info("", zap.Stringer("config", cfg))

	ctx := context.Background()
	if !checkSyncState(ctx, cfg) {
		log.Warn("check failed!!!")
		os.Exit(1)
	}
	log.Info("check pass!!!")
}

func checkSyncState(ctx context.Context, cfg *config.Config) bool {
	beginTime := time.Now()
	defer func() {
		log.Info("check data finished", zap.Duration("cost", time.Since(beginTime)))
	}()

	d, err := NewDiff(ctx, cfg)
	if err != nil {
		fmt.Printf("There is something error when initialize diff, please check log info in %s\n", filepath.Join(cfg.Task.OutputDir, config.LogFileName))
		log.Fatal("failed to initialize diff process", zap.Error(err))
		return false
	}
	defer d.Close()

	if !d.ignoreStructCheck {
		err = d.StructEqual(ctx)
		if err != nil {
			fmt.Printf("There is something error when compare structure of table, please check log info in %s\n", filepath.Join(cfg.Task.OutputDir, config.LogFileName))
			log.Fatal("failed to check structure difference", zap.Error(err))
			return false
		}
	}
	if !d.ignoreDataCheck {
		err = d.Equal(ctx)
		if err != nil {
			fmt.Printf("There is something error when compare data of table, please check log info in %s\n", filepath.Join(cfg.Task.OutputDir, config.LogFileName))
			log.Fatal("failed to check data difference", zap.Error(err))
			return false
		}
	}
	return d.PrintSummary(ctx)
}
