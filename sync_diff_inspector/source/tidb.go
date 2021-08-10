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
	router "github.com/pingcap/tidb-tools/pkg/table-router"
	"github.com/pingcap/tidb-tools/sync_diff_inspector/utils"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb-tools/sync_diff_inspector/source/common"
	"github.com/pingcap/tidb-tools/sync_diff_inspector/splitter"
	"go.uber.org/zap"
)

type TiDBSource struct {
	BasicSource
}

func (s *TiDBSource) GetTableAnalyzer() TableAnalyzer {
	return &TiDBTableAnalyzer{s.sourceTableMap}
}

type TiDBTableAnalyzer struct {
	sourceTableMap map[string]*common.SourceTable
}

func (a *TiDBTableAnalyzer) AnalyzeSplitter(ctx context.Context, table *common.TableDiff, startRange *splitter.RangeInfo) (splitter.ChunkIterator, error) {
	chunkSize := 0
	uniqueID := utils.UniqueID(table.Schema, table.Table)
	dbConn := a.sourceTableMap[uniqueID].DBConn
	originTable := a.sourceTableMap[uniqueID].OriginTable
	originSchema := a.sourceTableMap[uniqueID].OriginSchema
	table.Schema = originSchema
	table.Table = originTable
	// if we decide to use bucket to split chunks
	// we always use bucksIter even we load from checkpoint is not bucketNode
	// TODO check whether we can use bucket for this table to split chunks.
	bucketIter, err := splitter.NewBucketIteratorWithCheckpoint(ctx, table, dbConn, chunkSize, startRange)
	if err == nil {
		return bucketIter, nil
	}
	log.Warn("build bucketIter failed", zap.Error(err))
	// fall back to random splitter

	// use random splitter if we cannot use bucket splitter, then we can simply choose target table to generate chunks.
	randIter, err := splitter.NewRandomIteratorWithCheckpoint(ctx, table, dbConn, chunkSize, startRange)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return randIter, nil
}

func NewTiDBSource(ctx context.Context, tableDiffs []*common.TableDiff, tableRouter *router.Table, dbConn *sql.DB) (Source, error) {
	sourceMap, err := getSourceMap(ctx, tableDiffs, tableRouter, dbConn)
	if err != nil {
		return nil, errors.Trace(err)
	}
	ts := &TiDBSource{
		BasicSource{
			tableDiffs:     tableDiffs,
			sourceTableMap: sourceMap,
		},
	}
	return ts, nil
}
