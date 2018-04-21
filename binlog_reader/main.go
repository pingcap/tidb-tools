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
	"github.com/ngaut/log"
	"github.com/pingcap/tidb-tools/binlog_reader/util"
)

func main() {
	cfg := NewConfig()
	err := cfg.Parse(os.Args[1:])
	switch errors.Cause(err) {
	case nil:
	case flag.ErrHelp:
		os.Exit(0)
	default:
		log.Errorf("parse cmd flags err %s\n", err)
		os.Exit(2)
	}

	/*
	sql := "CREATE TABLE `ptest` (`a` int(11) NOT NULL,`b` double NOT NULL DEFAULT '2',`c` varchar(10) NOT NULL,`d` time DEFAULT NULL,PRIMARY KEY (`a`),UNIQUE KEY `d` (`d`))"
	tableInfo, err := util.GetSchemaTable(sql, "test", "ptest")
	if err != nil {
		log.Errorf("get table info failed %v", err)
	}
	log.Infof("table info: %v", tableInfo)
	log.Infof("columns[0]: %+v", tableInfo.Columns[0])
	log.Infof("columns[1]: %+v", tableInfo.Columns[1])
	*/
	tableInfo, err := util.GetSchemaTable(cfg.CreateTable, "test", "ptest")
	if err != nil {
		log.Errorf("get table info failed %v", err)
		return
	}
	err = util.Walk(cfg.Filename, tableInfo)
	if err != nil {
		log.Errorf("read binlog file failed %v", err)
		return
	}
}
