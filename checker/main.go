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
	"context"
	"flag"
	"fmt"
	"os"

	_ "github.com/go-sql-driver/mysql"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb-tools/pkg/check"
	"github.com/pingcap/tidb-tools/pkg/dbutil"
	"github.com/pingcap/tidb-tools/pkg/utils"
)

var (
	host         = flag.String("host", "127.0.0.1", "MySQL host")
	port         = flag.Int("port", 3306, "MySQL port")
	username     = flag.String("user", "root", "User name")
	password     = flag.String("password", "", "Password")
	printVersion = flag.Bool("V", false, "prints version and exit")
)

func main() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
		fmt.Fprint(os.Stderr, "./bin/checker command-line-flags dbname [tablename list]\n")
		fmt.Fprint(os.Stderr, "Command line flags:\n")
		flag.PrintDefaults()
	}

	flag.Parse()

	if *printVersion {
		fmt.Printf(utils.GetRawInfo("checker"))
		return
	}

	if len(flag.Args()) == 0 {
		log.Error("Miss database name")
		return
	}

	schema := flag.Args()[0]
	tables := flag.Args()[1:]
	checkTables(schema, tables)
	log.Infof("complete checking!!")
}

func checkTables(schema string, tables []string) {
	db, err := dbutil.OpenDB(dbutil.DBConfig{
		User:     *username,
		Password: *password,
		Host:     *host,
		Port:     *port,
		Schema:   schema,
	})
	if err != nil {
		log.Fatal("create database connection failed:", err)
	}
	defer dbutil.CloseDB(db)

	result := check.NewTablesChecker(db, map[string][]string{schema: tables}).Check(context.Background())
	if result.State == check.StateSuccess {
		log.Infof("check schema %s successfully!", schema)
	} else if result.State == check.StateWarning {
		log.Warningf("check schema %s and find warnings.\n%s", schema, result.ErrorMsg)
	} else {
		log.Errorf("check schema %s and failed.\n%s", schema, result.ErrorMsg)
	}
}
