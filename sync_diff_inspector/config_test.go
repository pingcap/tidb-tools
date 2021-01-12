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
	"io/ioutil"
	"path/filepath"
	"strings"
	"testing"

	. "github.com/pingcap/check"
)

func TestClient(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testConfigSuite{})

type testConfigSuite struct{}

func (s *testConfigSuite) TestUseDMConfig(c *C) {
	cfg := NewConfig()
	cfg.DMAddr = "127.0.0.1:8261"
	isValid := cfg.checkConfig()
	c.Assert(isValid, IsFalse)

	cfg.DMAddr = "http://127.0.0.1:8261"
	isValid = cfg.checkConfig()
	c.Assert(isValid, IsFalse)

	cfg.DMTask = "test"
	isValid = cfg.checkConfig()
	c.Assert(isValid, IsTrue)

	cfg.TargetDBCfg = DBConfig{
		InstanceID: "target",
	}
	isValid = cfg.checkConfig()
	c.Assert(isValid, IsFalse)

	cfg.TargetDBCfg.InstanceID = ""
	isValid = cfg.checkConfig()
	c.Assert(isValid, IsTrue)

	cfg.SourceDBCfg = []DBConfig{
		{
			InstanceID: "source-1",
		},
	}
	isValid = cfg.checkConfig()
	c.Assert(isValid, IsFalse)

	cfg.SourceDBCfg = nil
	isValid = cfg.checkConfig()
	c.Assert(isValid, IsTrue)

	cfg.Tables = []*CheckTables{
		{}, {},
	}
	isValid = cfg.checkConfig()
	c.Assert(isValid, IsFalse)
}

func (s *testConfigSuite) TestUnknownFlagOrItem(c *C) {
	cfg := NewConfig()
	c.Assert(cfg.Parse([]string{"-L", "info"}), IsNil)

	unknownFlag := []string{"-LL", "info"}
	err := cfg.Parse(unknownFlag)
	c.Assert(err, ErrorMatches, ".*LL.*")

	c.Assert(cfg.Parse([]string{"-config", "config.toml"}), IsNil)

	dir := c.MkDir()
	path := filepath.Join(dir, "wrong.toml")
	content, err := ioutil.ReadFile("config.toml")
	c.Assert(err, IsNil)
	// table_rules is a typo
	wrongContentStr := strings.ReplaceAll(string(content), "#[[table-rules]]", "[[table_rules]]")
	c.Assert(ioutil.WriteFile(path, []byte(wrongContentStr), 0644), IsNil)
	err = cfg.Parse([]string{"-config", path})
	c.Assert(err, ErrorMatches, ".*table_rules.*")
}
