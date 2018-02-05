package main

import (
	"database/sql"
	"flag"
	"os"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb-tools/sync_check/test"
	"github.com/pingcap/tidb-tools/sync_check/util"
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
	lastTime := time.Now().Add(time.Duration(-cfg.Delay) * time.Second).Format("2006-01-02 15:04:05")

	d := util.NewDiff(cfg.SourceDBCfg.Name, sourceDB, targetDB, lastTime, cfg.ChunkSize, cfg.Sample, cfg.CheckThCount)
	ok, err := d.Equal()
	if err != nil {
		log.Fatal(err)
	}

	return ok
}
