// Copyright 2020 PingCAP, Inc.
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
	"database/sql"
	"fmt"
	"net/http"
	"net/http/httptest"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	. "github.com/pingcap/check"
)

type getDMTaskCfgSuite struct{}

var _ = Suite(&getDMTaskCfgSuite{})

func (s *getDMTaskCfgSuite) TestGetDMTaskCfg(c *C) {
	mockServer := httptest.NewServer(http.HandlerFunc(testHandler))
	defer mockServer.Close()

	dmTaskCfg, err := getDMTaskCfg(mockServer.URL, "test")
	c.Assert(err, IsNil)
	c.Assert(dmTaskCfg, HasLen, 2)
	c.Assert(dmTaskCfg[0].SourceID, Equals, "mysql-replica-01")
	c.Assert(dmTaskCfg[1].SourceID, Equals, "mysql-replica-02")

	sourceDB1, sourceDB2, targetDB := mockDB(c)
	diff := &Diff{
		sourceDBs: map[string]DBConfig{
			"mysql-replica-01": {
				InstanceID: "mysql-replica-01",
				Conn:       sourceDB1,
			},
			"mysql-replica-02": {
				InstanceID: "mysql-replica-02",
				Conn:       sourceDB2,
			},
		},
		targetDB: DBConfig{
			InstanceID: "target",
			Conn:       targetDB,
		},
		subTaskCfgs: dmTaskCfg,
		ctx:         context.Background(),
		tables:      make(map[string]map[string]*TableConfig),
	}

	cfg := NewConfig()
	err = diff.adjustTableConfigBySubTask(cfg)
	c.Assert(err, IsNil)

	// after adjust config, will generate source tables for target table
	c.Assert(diff.tables, HasLen, 1)
	c.Assert(diff.tables["db_target"], HasLen, 1)
	c.Assert(diff.tables["db_target"]["t_target"].SourceTables, HasLen, 4)

	c.Assert(hasTableInstance(diff.tables["db_target"]["t_target"].SourceTables, TableInstance{
		InstanceID: "mysql-replica-01",
		Schema:     "sharding1",
		Table:      "t1",
	}), IsTrue)

	c.Assert(hasTableInstance(diff.tables["db_target"]["t_target"].SourceTables, TableInstance{
		InstanceID: "mysql-replica-01",
		Schema:     "sharding1",
		Table:      "t2",
	}), IsTrue)

	c.Assert(hasTableInstance(diff.tables["db_target"]["t_target"].SourceTables, TableInstance{
		InstanceID: "mysql-replica-02",
		Schema:     "sharding2",
		Table:      "t3",
	}), IsTrue)

	c.Assert(hasTableInstance(diff.tables["db_target"]["t_target"].SourceTables, TableInstance{
		InstanceID: "mysql-replica-02",
		Schema:     "sharding2",
		Table:      "t4",
	}), IsTrue)
}

func hasTableInstance(instances []TableInstance, instance TableInstance) bool {
	for _, ins := range instances {
		if ins == instance {
			return true
		}
	}

	return false
}

func mockDB(c *C) (*sql.DB, *sql.DB, *sql.DB) {
	sourceDB1, sourceMock1, err := sqlmock.New()
	c.Assert(err, IsNil)

	sourceDB2, sourceMock2, err := sqlmock.New()
	c.Assert(err, IsNil)

	targetDB, targetMock, err := sqlmock.New()
	c.Assert(err, IsNil)

	/*
		schemas and tables in mysql-replica-01:
		- sharding1
			- t1
			- t2
		- other
			- ta
			- tb
	*/
	rows := sqlmock.NewRows([]string{"Database"}).AddRow("sharding1").AddRow("other")
	sourceMock1.ExpectQuery("SHOW DATABASES").WillReturnRows(rows)
	rows = sqlmock.NewRows([]string{"Tables_in_sharding1", "Table_type"}).AddRow("t1", "BASE TABLE").AddRow("t2", "BASE TABLE")
	sourceMock1.ExpectQuery("SHOW FULL TABLES.*").WillReturnRows(rows)
	rows = sqlmock.NewRows([]string{"Tables_in_other", "Table_type"}).AddRow("ta", "BASE TABLE").AddRow("tb", "BASE TABLE")
	sourceMock1.ExpectQuery("SHOW FULL TABLES.*").WillReturnRows(rows)

	/*
		schemas and tables in mysql-replica-02:
		- sharding2
			- t3
			- t4
		- other
			- tc
	*/
	rows = sqlmock.NewRows([]string{"Database"}).AddRow("sharding2").AddRow("other")
	sourceMock2.ExpectQuery("SHOW DATABASES").WillReturnRows(rows)
	rows = sqlmock.NewRows([]string{"Tables_in_sharding2", "Table_type"}).AddRow("t3", "BASE TABLE").AddRow("t4", "BASE TABLE")
	sourceMock2.ExpectQuery("SHOW FULL TABLES.*").WillReturnRows(rows)
	rows = sqlmock.NewRows([]string{"Tables_in_other", "Table_type"}).AddRow("tc", "BASE TABLE")
	sourceMock2.ExpectQuery("SHOW FULL TABLES.*").WillReturnRows(rows)

	/*
		schemas and tables in target:
		- db_target
			- t_target
	*/
	rows = sqlmock.NewRows([]string{"Database"}).AddRow("db_target")
	targetMock.ExpectQuery("SHOW DATABASES").WillReturnRows(rows)
	rows = sqlmock.NewRows([]string{"Tables_in_db_target", "Table_type"}).AddRow("t_target", "BASE TABLE")
	targetMock.ExpectQuery("SHOW FULL TABLES.*").WillReturnRows(rows)

	rows = sqlmock.NewRows([]string{"Table", "Create Table"}).AddRow("t_target", "CREATE TABLE t_target (id int primary key, name varchar(24))")
	targetMock.ExpectQuery("SHOW CREATE TABLE.*").WillReturnRows(rows)

	rows = sqlmock.NewRows([]string{"Variable_name", "Value"}).AddRow("sql_mode", "ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION")
	targetMock.ExpectQuery("SHOW VARIABLES LIKE 'sql_mode'").WillReturnRows(rows)

	return sourceDB1, sourceDB2, targetDB
}

func testHandler(w http.ResponseWriter, req *http.Request) {
	fmt.Fprintln(w, "{\"result\":true,\"cfgs\":[\"is-sharding = true\\nshard-mode = \\\"pessimistic\\\"\\nonline-ddl-scheme = \\\"\\\"\\ncase-sensitive = false\\nname = \\\"test\\\"\\nmode = \\\"all\\\"\\nsource-id = \\\"mysql-replica-01\\\"\\nserver-id = 0\\nflavor = \\\"\\\"\\nmeta-schema = \\\"dm_meta\\\"\\nheartbeat-update-interval = 1\\nheartbeat-report-interval = 10\\nenable-heartbeat = false\\ntimezone = \\\"Asia/Shanghai\\\"\\nrelay-dir = \\\"\\\"\\nuse-relay = false\\nfilter-rules = []\\nmydumper-path = \\\"./bin/mydumper\\\"\\nthreads = 4\\nchunk-filesize = \\\"64\\\"\\nstatement-size = 0\\nrows = 0\\nwhere = \\\"\\\"\\nskip-tz-utc = true\\nextra-args = \\\"\\\"\\npool-size = 16\\ndir = \\\"./dumped_data.test\\\"\\nmeta-file = \\\"\\\"\\nworker-count = 16\\nbatch = 100\\nqueue-size = 1024\\ncheckpoint-flush-interval = 30\\nmax-retry = 0\\nauto-fix-gtid = false\\nenable-gtid = false\\ndisable-detect = false\\nsafe-mode = false\\nenable-ansi-quotes = false\\nlog-level = \\\"\\\"\\nlog-file = \\\"\\\"\\nlog-format = \\\"\\\"\\nlog-rotate = \\\"\\\"\\npprof-addr = \\\"\\\"\\nstatus-addr = \\\"\\\"\\nclean-dump-file = true\\n\\n[from]\\n  host = \\\"127.0.0.1\\\"\\n  port = 3306\\n  user = \\\"root\\\"\\n  password = \\\"/Q7B9DizNLLTTfiZHv9WoEAKamfpIUs=\\\"\\n  max-allowed-packet = 67108864\\n\\n[to]\\n  host = \\\"127.0.0.1\\\"\\n  port = 4000\\n  user = \\\"root\\\"\\n  password = \\\"\\\"\\n  max-allowed-packet = 67108864\\n\\n[[route-rules]]\\n  schema-pattern = \\\"sharding*\\\"\\n  table-pattern = \\\"t*\\\"\\n  target-schema = \\\"db_target\\\"\\n  target-table = \\\"t_target\\\"\\n\\n[[route-rules]]\\n  schema-pattern = \\\"sharding*\\\"\\n  table-pattern = \\\"\\\"\\n  target-schema = \\\"db_target\\\"\\n  target-table = \\\"\\\"\\n\\n[[mapping-rule]]\\n  schema-pattern = \\\"sharding*\\\"\\n  table-pattern = \\\"t*\\\"\\n  source-column = \\\"id\\\"\\n  target-column = \\\"id\\\"\\n  expression = \\\"partition id\\\"\\n  arguments = [\\\"1\\\", \\\"sharding\\\", \\\"t\\\"]\\n  create-table-query = \\\"\\\"\\n\\n[block-allow-list]\\n  do-dbs = [\\\"~^sharding[\\\\\\\\d]+\\\"]\\n\\n  [[block-allow-list.do-tables]]\\n    db-name = \\\"~^sharding[\\\\\\\\d]+\\\"\\n    tbl-name = \\\"~^t[\\\\\\\\d]+\\\"\\n\",\"is-sharding = true\\nshard-mode = \\\"pessimistic\\\"\\nonline-ddl-scheme = \\\"\\\"\\ncase-sensitive = false\\nname = \\\"test\\\"\\nmode = \\\"all\\\"\\nsource-id = \\\"mysql-replica-02\\\"\\nserver-id = 0\\nflavor = \\\"\\\"\\nmeta-schema = \\\"dm_meta\\\"\\nheartbeat-update-interval = 1\\nheartbeat-report-interval = 10\\nenable-heartbeat = false\\ntimezone = \\\"Asia/Shanghai\\\"\\nrelay-dir = \\\"\\\"\\nuse-relay = false\\nfilter-rules = []\\nmydumper-path = \\\"./bin/mydumper\\\"\\nthreads = 4\\nchunk-filesize = \\\"64\\\"\\nstatement-size = 0\\nrows = 0\\nwhere = \\\"\\\"\\nskip-tz-utc = true\\nextra-args = \\\"\\\"\\npool-size = 16\\ndir = \\\"./dumped_data.test\\\"\\nmeta-file = \\\"\\\"\\nworker-count = 16\\nbatch = 100\\nqueue-size = 1024\\ncheckpoint-flush-interval = 30\\nmax-retry = 0\\nauto-fix-gtid = false\\nenable-gtid = false\\ndisable-detect = false\\nsafe-mode = false\\nenable-ansi-quotes = false\\nlog-level = \\\"\\\"\\nlog-file = \\\"\\\"\\nlog-format = \\\"\\\"\\nlog-rotate = \\\"\\\"\\npprof-addr = \\\"\\\"\\nstatus-addr = \\\"\\\"\\nclean-dump-file = true\\n\\n[from]\\n  host = \\\"127.0.0.1\\\"\\n  port = 3307\\n  user = \\\"root\\\"\\n  password = \\\"/Q7B9DizNLLTTfiZHv9WoEAKamfpIUs=\\\"\\n  max-allowed-packet = 67108864\\n\\n[to]\\n  host = \\\"127.0.0.1\\\"\\n  port = 4000\\n  user = \\\"root\\\"\\n  password = \\\"\\\"\\n  max-allowed-packet = 67108864\\n\\n[[route-rules]]\\n  schema-pattern = \\\"sharding*\\\"\\n  table-pattern = \\\"t*\\\"\\n  target-schema = \\\"db_target\\\"\\n  target-table = \\\"t_target\\\"\\n\\n[[route-rules]]\\n  schema-pattern = \\\"sharding*\\\"\\n  table-pattern = \\\"\\\"\\n  target-schema = \\\"db_target\\\"\\n  target-table = \\\"\\\"\\n\\n[[mapping-rule]]\\n  schema-pattern = \\\"sharding*\\\"\\n  table-pattern = \\\"t*\\\"\\n  source-column = \\\"id\\\"\\n  target-column = \\\"id\\\"\\n  expression = \\\"partition id\\\"\\n  arguments = [\\\"2\\\", \\\"sharding\\\", \\\"t\\\"]\\n  create-table-query = \\\"\\\"\\n\\n[block-allow-list]\\n  do-dbs = [\\\"~^sharding[\\\\\\\\d]+\\\"]\\n\\n  [[block-allow-list.do-tables]]\\n    db-name = \\\"~^sharding[\\\\\\\\d]+\\\"\\n    tbl-name = \\\"~^t[\\\\\\\\d]+\\\"\\n\"]}")
}
