/*************************************************************************
 *
 * PingCAP CONFIDENTIAL
 * __________________
 *
 *  [2015] - [2018] PingCAP Incorporated
 *  All Rights Reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of PingCAP Incorporated and its suppliers,
 * if any.  The intellectual and technical concepts contained
 * herein are proprietary to PingCAP Incorporated
 * and its suppliers and may be covered by P.R.China and Foreign Patents,
 * patents in process, and are protected by trade secret or copyright law.
 * Dissemination of this information or reproduction of this material
 * is strictly forbidden unless prior written permission is obtained
 * from PingCAP Incorporated.
 */

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
	dbInfo := &dbutil.DBConfig{
		User:     *username,
		Password: *password,
		Host:     *host,
		Port:     *port,
		Schema:   schema,
	}

	db, err := dbutil.OpenDB(*dbInfo)
	if err != nil {
		log.Fatal("create database connection failed:", err)
	}
	defer dbutil.CloseDB(db)

	result := check.NewTablesChecker(db, dbInfo, map[string][]string{schema: tables}).Check(context.Background())
	if result.State == check.StateSuccess {
		log.Infof("check schema %s successfully!", schema)
	} else if result.State == check.StateWarning {
		log.Warningf("check schema %s and find warnings.\n%s", schema, result.ErrorMsg)
	} else {
		log.Errorf("check schema %s and failed.\n%s", schema, result.ErrorMsg)
	}
}
