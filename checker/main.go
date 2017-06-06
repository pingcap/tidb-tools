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
	"fmt"
	"os"

	_ "github.com/go-sql-driver/mysql"
	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb-tools/pkg/utils"
)

var (
	logLevel     = flag.String("L", "info", "log level: info, debug, warn, error, fatal")
	host         = flag.String("host", "127.0.0.1", "MySQL host")
	port         = flag.Int("port", 3306, "MySQL port")
	username     = flag.String("user", "root", "User name")
	password     = flag.String("password", "", "Password")
	printVersion = flag.Bool("V", false, "prints version and exit")
)

func openDB(dbName string) (*sql.DB, error) {
	dbDSN := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8", *username, *password, *host, *port, dbName)
	log.Infof("Database DSN: %s", dbDSN)
	db, err := sql.Open("mysql", dbDSN)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return db, nil
}

func closeDB(db *sql.DB) error {
	if db == nil {
		return nil
	}
	return errors.Trace(db.Close())
}

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
	log.SetLevelByString(*logLevel)
	c := &checker{
		dbName: flag.Args()[0],
		tbls:   flag.Args()[1:],
	}
	err := c.check()
	if err != nil {
		log.Errorf("Check database error: %s", err)
		os.Exit(2)
	} else if c.warnings > 0 || c.errs > 0 {
		log.Errorf("Check database %s with %d errors and %d warnings.", c.dbName, c.errs, c.warnings)
		os.Exit(2)
	} else {
		fmt.Println("Check database succ!")
	}
	err = c.close()
	if err != nil {
		log.Errorf("Close db with error %s", err)
	}
}
