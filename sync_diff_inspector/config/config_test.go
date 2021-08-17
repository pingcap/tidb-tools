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

package config

import (
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
	c.Assert(cfg.Task.Init(cfg.DataSources, cfg.Routes, cfg.TableConfigs), IsNil)

	c.Assert(cfg.Parse([]string{"-config", "config_sharding.toml"}), IsNil)
	c.Assert(cfg.Task.Init(cfg.DataSources, cfg.Routes, cfg.TableConfigs), IsNil)
}
