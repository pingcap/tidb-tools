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

package filter

// This package exists only for backward-compatibility.
// New code should depend on the module "github.com/pingcap/tidb-tools/components/table-filter" instead.

import (
	base "github.com/pingcap/tidb-tools/components/table-filter"
)

type Filter = base.Filter
type Table = base.Table
type MySQLReplicationRules = base.MySQLReplicationRules

func Parse(args []string) (Filter, error) {
	return base.Parse(args)
}

func CaseInsensitive(f Filter) Filter {
	return base.CaseInsensitive(f)
}

func All() Filter {
	return base.All()
}

func NewSchemasFilter(schemas ...string) Filter {
	return base.NewSchemasFilter(schemas...)
}

func NewTablesFilter(tables ...Table) Filter {
	return base.NewTablesFilter(tables...)
}

func ParseMySQLReplicationRules(rules *MySQLReplicationRules) (Filter, error) {
	return base.ParseMySQLReplicationRules(rules)
}
