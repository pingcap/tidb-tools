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
	"flag"
	"os"

	"github.com/juju/errors"
	"github.com/ngaut/log"
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

	table := newTable()
	err = parseTableSQL(table, cfg.TableSQL)
	if err != nil {
		log.Fatal(err)
	}

	err = parseIndexSQL(table, cfg.IndexSQL)
	if err != nil {
		log.Fatal(err)
	}

	dbs, err := createDBs(cfg.DBCfg, cfg.WorkerCount)
	if err != nil {
		log.Fatal(err)
	}
	defer closeDBs(dbs)

	err = execSQL(dbs[0], cfg.TableSQL)
	if err != nil {
		log.Fatal(err)
	}

	err = execSQL(dbs[0], cfg.IndexSQL)
	if err != nil {
		log.Fatal(err)
	}

	doProcess(table, dbs, cfg.JobCount, cfg.WorkerCount, cfg.Batch)
}
