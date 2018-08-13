package main

import (
	"context"
	"flag"
	"fmt"
	"os"

	_ "github.com/go-sql-driver/mysql"
	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb-tools/checksum"
	"github.com/pingcap/tidb-tools/pkg/dbutil"

	"net/http"
	_ "net/http/pprof"
)

func main() {
	cfg := checksum.NewConfig()
	err := cfg.Parse(os.Args[1:])
	switch errors.Cause(err) {
	case nil:
	case flag.ErrHelp:
		os.Exit(0)
	default:
		log.Errorf("parse cmd flags err %s\n", errors.ErrorStack(err))
		os.Exit(2)
	}

	go func() {
		http.ListenAndServe(fmt.Sprintf(":%d", cfg.ProfilePort), nil)
	}()

	log.SetLevelByString(cfg.LogLevel)
	if len(cfg.LogFile) > 0 {
		log.SetOutputByName(cfg.LogFile)
		log.SetHighlighting(false)
	}

	log.Infof("sourcedb %+v", cfg.SourceDBCfg)

	sourceDB, err := dbutil.OpenDB(cfg.SourceDBCfg)
	if err != nil {
		log.Fatalf("create source db %+v error %v", cfg.SourceDBCfg, err)
	}
	defer dbutil.CloseDB(sourceDB)

	// targetDB, err := dbutil.OpenDB(cfg.TargetDBCfg)
	// if err != nil {
	// 	log.Fatalf("create target db %+v error %v", cfg.TargetDBCfg, err)
	// }
	// defer dbutil.CloseDB(targetDB)

	tc, err := checksum.NewTableChecksum(cfg, sourceDB)
	if err != nil {
		log.Fatal(errors.ErrorStack(err))
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	err = tc.Process(ctx)
	if err != nil {
		log.Fatal(errors.ErrorStack(err))
	}

	// log.Info("test pass!!!")

}
