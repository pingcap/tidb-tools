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

package source

import (
	"context"
	"database/sql"
	"github.com/pingcap/tidb-tools/sync_diff_inspector/config"
	"github.com/pingcap/tidb-tools/sync_diff_inspector/source/common"
	"github.com/pingcap/tidb-tools/sync_diff_inspector/utils"
)

type MySQLSources struct {
	tableDiffs []*common.TableDiff

	sourceDBs map[string]*sql.DB
}

func NewMySQLSources(ctx context.Context, tableDiffs []*common.TableDiff, dbs []*config.DBConfig) (Source, error) {
	sourceDBs := make(map[string]*sql.DB)
	for _, db := range dbs {
		sourceDBs[db.InstanceID] = db.Conn
	}

	for _, table := range tableDiffs {
		table.TableRowsQuery, table.TableOrderKeyCols = utils.GetTableRowsQueryFormat(table.Schema, table.Table, table.Info, table.Collation)
	}

	mss := &MySQLSources{
		tableDiffs: tableDiffs,
		sourceDBs:  sourceDBs,
	}
	return mss, nil
}
