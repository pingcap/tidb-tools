package dailytest

import (
	"fmt"
	"sync"

	"github.com/ngaut/log"
	"github.com/pingcap/tidb-tools/sync_check/util"
)

// RunDailyTest generates insert/update/delete sqls and execute
func RunDailyTest(dbCfg util.DBConfig, tableSQLs []string, workerCount int, jobCount int, batch int, hasDDL bool) {
	var wg sync.WaitGroup
	wg.Add(len(tableSQLs))

	for i := range tableSQLs {
		go func(i int) {
			defer wg.Done()

			table := newTable()
			err := parseTableSQL(table, tableSQLs[i])
			if err != nil {
				log.Fatal(err)
			}

			dbs, err := createDBs(dbCfg, workerCount)
			if err != nil {
				log.Fatal(err)
			}
			defer closeDBs(dbs)

			err = execSQL(dbs[0], tableSQLs[i])
			if err != nil {
				log.Fatal(err)
			}

			doProcess(table, dbs, jobCount, workerCount, batch, hasDDL)
		}(i)
	}

	wg.Wait()
}

// TruncateTestTable truncates test data
func TruncateTestTable(dbCfg util.DBConfig, tableSQLs []string) {
	db, err := util.CreateDB(dbCfg)
	if err != nil {
		log.Fatal(err)
	}
	defer util.CloseDB(db)

	for i := range tableSQLs {
		table := newTable()
		err := parseTableSQL(table, tableSQLs[i])
		if err != nil {
			log.Fatal(err)
		}

		err = execSQL(db, fmt.Sprintf("truncate table %s", table.name))
		if err != nil {
			log.Fatal(err)
		}
	}
}

// DropTestTable drops test table
func DropTestTable(dbCfg util.DBConfig, tableSQLs []string) {
	db, err := util.CreateDB(dbCfg)
	if err != nil {
		log.Fatal(err)
	}
	defer util.CloseDB(db)

	for i := range tableSQLs {
		table := newTable()
		err := parseTableSQL(table, tableSQLs[i])
		if err != nil {
			log.Fatal(err)
		}

		err = execSQL(db, fmt.Sprintf("drop table %s", table.name))
		if err != nil {
			log.Fatal(err)
		}
	}
}
