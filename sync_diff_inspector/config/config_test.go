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

	"github.com/stretchr/testify/require"
)

func TestParseConfig(t *testing.T) {
	cfg := NewConfig()
	require.Nil(t, cfg.Parse([]string{"--L", "info"}))

	unknownFlag := []string{"--LL", "info"}
	err := cfg.Parse(unknownFlag)
	require.Contains(t, err.Error(), "LL")

	require.Nil(t, cfg.Parse([]string{"--config", "config.toml"}))
	require.Nil(t, cfg.Init())
	require.Nil(t, cfg.Task.Init(cfg.DataSources, cfg.TableConfigs))

	require.Nil(t, cfg.Parse([]string{"--config", "config_sharding.toml"}))
	// we change the config from config.toml to config_sharding.toml
	// this action will raise error.
	require.Contains(t, cfg.Init().Error(), "failed to init Task: config changes breaking the checkpoint, please use another outputDir and start over again!")

	require.NoError(t, os.RemoveAll(cfg.Task.OutputDir))
	require.Nil(t, cfg.Parse([]string{"--config", "config_sharding.toml"}))
	// this time will be ok, because we remove the last outputDir.
	require.Nil(t, cfg.Init())
	require.Nil(t, cfg.Task.Init(cfg.DataSources, cfg.TableConfigs))

	require.True(t, cfg.CheckConfig())

	// we might not use the same config to run this test. e.g. MYSQL_PORT can be 4000
	require.Equal(t, cfg.String(), `{"log-level":"info","check-thread-count":4,"compare-checksum-only":false,"ignore-struct-check":false,"ignore-stats":false,"ignore-data-check":false,"use-checkpoint":true,"dm-addr":"","dm-task":"","data-sources":{"mysql1":{"host":"127.0.0.1","port":3306,"user":"root","password":"","sql-mode":"","snapshot":"","route-rules":["rule1","rule2"],"Router":{"Selector":{}},"Conn":null},"mysql2":{"host":"127.0.0.1","port":3306,"user":"root","password":"","sql-mode":"","snapshot":"","route-rules":["rule1","rule2"],"Router":{"Selector":{}},"Conn":null},"mysql3":{"host":"127.0.0.1","port":3306,"user":"root","password":"","sql-mode":"","snapshot":"","route-rules":["rule1","rule3"],"Router":{"Selector":{}},"Conn":null},"tidb":{"host":"127.0.0.1","port":4000,"user":"root","password":"","sql-mode":"","snapshot":"","route-rules":null,"Router":{"Selector":{}},"Conn":null}},"routes":{"rule1":{"schema-pattern":"test_*","table-pattern":"t_*","target-schema":"test","target-table":"t"},"rule2":{"schema-pattern":"test2_*","table-pattern":"t2_*","target-schema":"test2","target-table":"t2"},"rule3":{"schema-pattern":"test2_*","table-pattern":"t2_*","target-schema":"test","target-table":"t"}},"table-configs":{"config1":{"schema":"schema1","table":"table","IgnoreColumns":["",""],"Fields":"","Range":"age \u003e 10 AND age \u003c 20","TargetTableInfo":null,"Collation":"","chunk-size":0}},"task":{"source-instances":["mysql1","mysql2","mysql3"],"source-routes":null,"target-instance":"tidb","target-check-tables":["schema*.table*","!c.*","test2.t2"],"target-configs":["config1"],"output-dir":"./output","SourceInstances":[{"host":"127.0.0.1","port":3306,"user":"root","password":"","sql-mode":"","snapshot":"","route-rules":["rule1","rule2"],"Router":{"Selector":{}},"Conn":null},{"host":"127.0.0.1","port":3306,"user":"root","password":"","sql-mode":"","snapshot":"","route-rules":["rule1","rule2"],"Router":{"Selector":{}},"Conn":null},{"host":"127.0.0.1","port":3306,"user":"root","password":"","sql-mode":"","snapshot":"","route-rules":["rule1","rule3"],"Router":{"Selector":{}},"Conn":null}],"TargetInstance":{"host":"127.0.0.1","port":4000,"user":"root","password":"","sql-mode":"","snapshot":"","route-rules":null,"Router":{"Selector":{}},"Conn":null},"TargetTableConfigs":[{"schema":"schema1","table":"table","IgnoreColumns":["",""],"Fields":"","Range":"age \u003e 10 AND age \u003c 20","TargetTableInfo":null,"Collation":"","chunk-size":0}],"TargetCheckTables":[{},{},{}],"FixDir":"output/7a0babf855b73acd3d2e8bcb9a368819a19ceb7b0f0a63f28aadfaa48482de30/fix-on-tidb","CheckpointDir":"output/7a0babf855b73acd3d2e8bcb9a368819a19ceb7b0f0a63f28aadfaa48482de30/checkpoint","HashFile":""},"ConfigFile":"config_sharding.toml","PrintVersion":false}`)
	hash, err := cfg.Task.ComputeConfigHash()
	require.NoError(t, err)
	require.Equal(t, hash, "7a0babf855b73acd3d2e8bcb9a368819a19ceb7b0f0a63f28aadfaa48482de30")

	require.True(t, cfg.TableConfigs["config1"].Valid())

	require.NoError(t, os.RemoveAll(cfg.Task.OutputDir))

}

func TestError(t *testing.T) {
	tableConfig := &TableConfig{}
	require.False(t, tableConfig.Valid())
	tableConfig.Schema = "123"
	require.False(t, tableConfig.Valid())
	tableConfig.Table = "234"
	require.True(t, tableConfig.Valid())

	cfg := NewConfig()
	// Parse
	require.Contains(t, cfg.Parse([]string{"--config", "no_exist.toml"}).Error(), "no_exist.toml: no such file or directory")

	// CheckConfig
	cfg.CheckThreadCount = 0
	require.False(t, cfg.CheckConfig())
	cfg.CheckThreadCount = 1
	require.True(t, cfg.CheckConfig())

	// Init
	cfg.DataSources = make(map[string]*DataSource)
	cfg.DataSources["123"] = &DataSource{
		RouteRules: []string{"111"},
	}
	err := cfg.Init()
	require.Contains(t, err.Error(), "not found source routes for rule 111, please correct the config")
}
