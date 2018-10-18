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
	"os"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/juju/errors"
	"github.com/ngaut/log"
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

	if !checkSyncState(ctx, cfg) {
		log.Fatal("sourceDB don't equal targetDB")
	}
	log.Info("test pass!!!")
}

func checkSyncState(ctx context.Context, cfg *Config) bool {
	beginTime := time.Now()
	defer func() {
		log.Infof("check data finished, all cost %v", time.Since(beginTime))
	}()

	d, err := NewDiff(ctx, cfg)
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
