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

package splitter

import (
	"context"
	"database/sql"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb-tools/pkg/dbutil"
	"github.com/pingcap/tidb-tools/sync_diff_inspector/chunk"
	"github.com/pingcap/tidb-tools/sync_diff_inspector/source/common"
	"github.com/pingcap/tidb-tools/sync_diff_inspector/utils"
	"go.uber.org/zap"
)

type BucketIterator struct {
	table        *common.TableDiff
	indexcolumns []*model.ColumnInfo
	buckets      []dbutil.Bucket

	dbConn *sql.DB
}

func NewBucketIterator(table *common.TableDiff, dbConn *sql.DB) (*BucketIterator, error) {
	buckets, err := dbutil.GetBucketsInfo(context.Background(), dbConn, table.Schema, table.Table, table.Info)
	if err != nil {
		return nil, errors.Trace(err)
	}
	bs := &BucketIterator{
		table:  table,
		dbConn: dbConn,
	}
	// TODO ignore columns
	indices := dbutil.FindAllIndex(table.Info)
	for _, index := range indices {
		if index == nil {
			continue
		}
		bucket, ok := buckets[index.Name.O]
		if !ok {
			return nil, errors.NotFoundf("index %s in buckets info", index.Name.O)
		}
		log.Debug("buckets for index", zap.String("index", index.Name.O), zap.Reflect("buckets", buckets))

		indexcolumns := utils.GetColumnsFromIndex(index, table.Info)

		if len(indexcolumns) == 0 {
			continue
		}
		bs.buckets = bucket
		bs.indexcolumns = indexcolumns
	}

	if bs.buckets == nil || bs.indexcolumns == nil {
		return nil, errors.NotFoundf("no index to split buckets")
	}

	return bs, nil
}

func (s *BucketIterator) Next() (*chunk.Range, error) {

	return nil, nil
}
