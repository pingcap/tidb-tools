// Copyright 2018 PingCAP, Inc.
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

package column

import (
	"testing"

	. "github.com/pingcap/check"
)

func TestClient(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testColumnMappingSuit{})

type testColumnMappingSuit struct{}

func (t *testColumnMappingSuit) TestHandle(c *C) {
	rules := []*Rule{
		{"test*", "abc*", "id", "copyid", Clone, nil, "xx"},
		{"test*", "xxx*", "", "id", AddPrefix, []string{"instance_id"}, "xx"},
	}

	inValidRule := &Rule{"test*", "abc*", "id", "id", "Error", nil, "xxx"}
	c.Assert(inValidRule.Valid(), NotNil)
	inValidRule.TargetColumn = ""
	inValidRule.Expression = AddPrefix
	c.Assert(inValidRule.Valid(), NotNil)

	// initial column mapping
	m, err := NewMapping(rules)
	c.Assert(err, IsNil)
	c.Assert(m.cache.infos, HasLen, 0)

	// test clone
	vals, err := m.HandleRowValue("test", "abc", []string{"id", "copyid", "name"}, []interface{}{1, "name"})
	c.Assert(err, IsNil)
	c.Assert(vals, DeepEquals, []interface{}{1, 1, "name"})

	// test cache
	vals, err = m.HandleRowValue("test", "abc", []string{"copyid", "name", "id"}, []interface{}{"name", 1})
	c.Assert(err, IsNil)
	c.Assert(vals, DeepEquals, []interface{}{"name", "name", 1})

	m.resetCache()
	vals, err = m.HandleRowValue("test", "abc", []string{"copyid", "name", "id"}, []interface{}{"name", 1})
	c.Assert(err, IsNil)
	c.Assert(vals, DeepEquals, []interface{}{1, "name", 1})

	// test add prefix
	vals, err = m.HandleRowValue("test", "xxx", []string{"id"}, []interface{}{1.1})
	c.Assert(err, IsNil)
	c.Assert(vals, DeepEquals, []interface{}{"instance_id:1.1"})
}
