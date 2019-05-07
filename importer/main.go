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

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb-tools/pkg/importer"
	"go.uber.org/zap"
)

func main() {
	cfg := NewConfig()
	err := cfg.Parse(os.Args[1:])
	switch errors.Cause(err) {
	case nil:
	case flag.ErrHelp:
		os.Exit(0)
	default:
		log.Error("parse cmd flags failed", zap.Error(err))
		os.Exit(2)
	}

	importerCfg := &importer.Config{
		TableSQL:    cfg.TableSQL,
		IndexSQL:    cfg.IndexSQL,
		LogLevel:    cfg.LogLevel,
		WorkerCount: cfg.WorkerCount,
		JobCount:    cfg.JobCount,
		Batch:       cfg.Batch,
		DBCfg:       cfg.DBCfg,
	}

	importer.DoProcess(importerCfg)
}
