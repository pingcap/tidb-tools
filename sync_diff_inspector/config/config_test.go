// Copyright 2021 PingCAP, Inc.
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

package config

import (
	"os"
	"testing"

	. "github.com/pingcap/check"
)

func TestClient(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testConfigSuite{})

type testConfigSuite struct{}

func (s *testConfigSuite) TestParseConfig(c *C) {
	cfg := NewConfig()
	c.Assert(cfg.Parse([]string{"-L", "info"}), IsNil)

	unknownFlag := []string{"-LL", "info"}
	err := cfg.Parse(unknownFlag)
	c.Assert(err, ErrorMatches, ".*LL.*")

	c.Assert(cfg.Parse([]string{"-config", "config.toml"}), IsNil)
	c.Assert(cfg.Init(), IsNil)
	c.Assert(cfg.Task.Init(cfg.DataSources, cfg.TableConfigs), IsNil)

	c.Assert(cfg.Parse([]string{"-config", "config_sharding.toml"}), IsNil)
	c.Assert(cfg.Init(), IsNil)
	c.Assert(cfg.Task.Init(cfg.DataSources, cfg.TableConfigs), IsNil)

	c.Assert(cfg.CheckConfig(), Equals, true)

	c.Assert(cfg.String(), Equals, "{\"log-level\":\"info\",\"sample-percent\":100,\"check-thread-count\":4,\"use-checksum\":false,\"ignore-struct-check\":false,\"ignore-stats\":false,\"ignore-data-check\":false,\"use-checkpoint\":true,\"data-sources\":{\"mysql1\":{\"host\":\"127.0.0.1\",\"port\":3306,\"user\":\"root\",\"password\":\"\",\"sql-mode\":\"\",\"snapshot\":\"\",\"route-rules\":[\"rule1\",\"rule2\"],\"Router\":{\"Selector\":{}},\"Conn\":null},\"mysql2\":{\"host\":\"127.0.0.1\",\"port\":3306,\"user\":\"root\",\"password\":\"\",\"sql-mode\":\"\",\"snapshot\":\"\",\"route-rules\":[\"rule1\",\"rule2\"],\"Router\":{\"Selector\":{}},\"Conn\":null},\"mysql3\":{\"host\":\"127.0.0.1\",\"port\":3306,\"user\":\"root\",\"password\":\"\",\"sql-mode\":\"\",\"snapshot\":\"\",\"route-rules\":[\"rule1\",\"rule3\"],\"Router\":{\"Selector\":{}},\"Conn\":null},\"tidb\":{\"host\":\"127.0.0.1\",\"port\":4000,\"user\":\"root\",\"password\":\"\",\"sql-mode\":\"\",\"snapshot\":\"\",\"route-rules\":null,\"Router\":{\"Selector\":{}},\"Conn\":null}},\"routes\":{\"rule1\":{\"schema-pattern\":\"test_*\",\"table-pattern\":\"t_*\",\"target-schema\":\"test\",\"target-table\":\"t\"},\"rule2\":{\"schema-pattern\":\"test2_*\",\"table-pattern\":\"t2_*\",\"target-schema\":\"test2\",\"target-table\":\"t2\"},\"rule3\":{\"schema-pattern\":\"test2_*\",\"table-pattern\":\"t2_*\",\"target-schema\":\"test\",\"target-table\":\"t\"}},\"table-configs\":{\"config1\":{\"schema\":\"schema1\",\"table\":\"table\",\"IgnoreColumns\":[\"\",\"\"],\"Fields\":\"\",\"Range\":\"age \\u003e 10 AND age \\u003c 20\",\"IsSharding\":false,\"TargetTableInfo\":null,\"Collation\":\"\"}},\"task\":{\"source-instances\":[\"mysql1\",\"mysql2\",\"mysql3\"],\"source-routes\":null,\"target-instance\":[\"tidb\"],\"target-check-tables\":[\"schema*.table*\",\"!c.*\",\"test2.t2\"],\"target-configs\":[\"config1\"],\"output-dir\":\"./output\",\"SourceInstances\":[{\"host\":\"127.0.0.1\",\"port\":3306,\"user\":\"root\",\"password\":\"\",\"sql-mode\":\"\",\"snapshot\":\"\",\"route-rules\":[\"rule1\",\"rule2\"],\"Router\":{\"Selector\":{}},\"Conn\":null},{\"host\":\"127.0.0.1\",\"port\":3306,\"user\":\"root\",\"password\":\"\",\"sql-mode\":\"\",\"snapshot\":\"\",\"route-rules\":[\"rule1\",\"rule2\"],\"Router\":{\"Selector\":{}},\"Conn\":null},{\"host\":\"127.0.0.1\",\"port\":3306,\"user\":\"root\",\"password\":\"\",\"sql-mode\":\"\",\"snapshot\":\"\",\"route-rules\":[\"rule1\",\"rule3\"],\"Router\":{\"Selector\":{}},\"Conn\":null}],\"TargetInstance\":{\"host\":\"127.0.0.1\",\"port\":4000,\"user\":\"root\",\"password\":\"\",\"sql-mode\":\"\",\"snapshot\":\"\",\"route-rules\":null,\"Router\":{\"Selector\":{}},\"Conn\":null},\"TargetTableConfigs\":[{\"schema\":\"schema1\",\"table\":\"table\",\"IgnoreColumns\":[\"\",\"\"],\"Fields\":\"\",\"Range\":\"age \\u003e 10 AND age \\u003c 20\",\"IsSharding\":false,\"TargetTableInfo\":null,\"Collation\":\"\"}],\"TargetCheckTables\":[{},{},{}],\"FixDir\":\"output/3e71d9319a0becab2f1692424a088f23/fix-on-tidb\",\"CheckpointDir\":\"output/3e71d9319a0becab2f1692424a088f23/checkpoint\",\"HashFile\":\"\"},\"ConfigFile\":\"config_sharding.toml\",\"PrintVersion\":false}")
	hash, err := cfg.Task.ComputeConfigHash()
	c.Assert(err, IsNil)
	c.Assert(hash, Equals, "3e71d9319a0becab2f1692424a088f23")

	c.Assert(cfg.TableConfigs["config1"].Valid(), Equals, true)
	c.Assert(cfg.DataSources["mysql1"].HashCode(), Equals, "6679e7fccd4375f236259a3b57220a30")

	c.Assert(os.RemoveAll(cfg.Task.OutputDir), IsNil)

}

func (s *testConfigSuite) TestError(c *C) {
	tableConfig := &TableConfig{}
	c.Assert(tableConfig.Valid(), IsFalse)
	tableConfig.Schema = "123"
	c.Assert(tableConfig.Valid(), IsFalse)
	tableConfig.Table = "234"
	c.Assert(tableConfig.Valid(), IsTrue)
}
