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

const (
	// CREATEDATATUBE means create datatube for wormhole
	CREATEDATATUBE = "createDataTube"

	// LOADDATA means load data into mysql
	LOADDATA = "loadData"

	// INCREASEDATA means increase data into mysql, include ddl sql
	INCREASEDATA = "increaseData"

	// TRUNCATE means truncate data in mysql
	TRUNCATE = "truncate"

	// CLEAR means drop database in mysql
	CLEAR = "clear"

	// CHECK means check data between mysql and tidb
	CHECK = "check"
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

	TableSQLs := []string{`
create table if not exists ptest(
	a int primary key, 
	b double NOT NULL DEFAULT 2.0, 
	c varchar(10) NOT NULL, 
	d time unique,
	e datetime,
	KEY(e)
);
`,
		`
create table if not exists itest(
	a int, 
	b double NOT NULL DEFAULT 2.0, 
	c varchar(10) NOT NULL, 
	d time unique,
	e datetime, 
	PRIMARY KEY(a, b),
	KEY(e)
);
`,
		`
create table if not exists ntest(
	a int, 
	b double NOT NULL DEFAULT 2.0, 
	c varchar(10) NOT NULL, 
	d time unique,
	e datetime, 
	KEY(e)
);
`,
		`
create table if not exists mtest(
	a varchar(10) NOT NULL,
	b varchar(10) NOT NULL,
	c varchar(10) NOT NULL,
	d time unique,
	e datetime,
	PRIMARY KEY(d),
	KEY(e)
);
`}

	log.Infof("jobType: %s", cfg.JobType)

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

	if cfg.JobType == LOADDATA {
		// generate insert/update/delete sqls and execute
		dailytest.RunDailyTest(cfg.SourceDBCfg, TableSQLs, cfg.WorkerCount, cfg.JobCount, cfg.Batch, false)
	}

	if cfg.JobType == TRUNCATE {
		// truncate test data
		dailytest.TruncateTestTable(cfg.SourceDBCfg, TableSQLs)
	}

	if cfg.JobType == CHECK {
		// diff the test schema
		if !checkSyncState(sourceDB, targetDB, cfg) {
			log.Fatal("sourceDB don't equal targetDB")
		}
		log.Info("test pass!!!")
	}

	if cfg.JobType == CLEAR {
		dailytest.DropTestTable(cfg.SourceDBCfg, TableSQLs)
	}
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
