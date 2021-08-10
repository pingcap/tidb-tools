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

package common

import (
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb-tools/sync_diff_inspector/config"
)

// TableDiff saves config for diff table
type TableDiff struct {
	InstanceID string           `json:"instance-id"`
	Schema     string           `json:"schema"`
	Table      string           `json:"table"`
	Info       *model.TableInfo `json:"info"`

	// columns be ignored
	IgnoreColumns []string `json:"-"`

	// field should be the primary key, unique key or field with index
	Fields string `json:"fields"`

	// select range, for example: "age > 10 AND age < 20"
	Range string `json:"range"`

	// set false if want to comapre the data directly
	UseChecksum bool `json:"-"`

	// set true if just want compare data by checksum, will skip select data when checksum is not equal
	OnlyUseChecksum bool `json:"-"`

	// ignore check table's struct
	IgnoreStructCheck bool `json:"-"`

	// ignore check table's data
	IgnoreDataCheck bool `json:"-"`

	// tableMap record the map relationship of upstream tables and downstream table
	// target table instance => source table instances
	TableMaps map[config.TableInstance][]config.TableInstance `json:"table-map"`

	Collation string `json:"collation"`

	TableRowsQuery string

	TableOrderKeyCols []*model.ColumnInfo
}
