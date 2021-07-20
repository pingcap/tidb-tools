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
	"github.com/pingcap/tidb-tools/pkg/dbutil"
	"github.com/pingcap/tidb-tools/sync_diff_inspector/chunk"
	"github.com/pingcap/tidb-tools/sync_diff_inspector/source/common"
	"go.uber.org/zap"
)

type BucketSplitter struct {
	table     *common.TableDiff
	collation string
	buckets   map[string][]dbutil.Bucket

	dbConn *sql.DB
}

func NewBucketSplitter(table *common.TableDiff, collation string, dbConn *sql.DB) (*BucketSplitter, error) {
	buckets, err := dbutil.GetBucketsInfo(context.Background(), dbConn, table.Schema, table.Table, table.Info)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &BucketSplitter{
		table:     table,
		buckets:   buckets,
		collation: collation,
		dbConn:    dbConn,
	}, nil
}

func (s *BucketSplitter) Split() (chunk.Iterator, error) {
	indices := dbutil.FindAllIndex(s.table.Info)
	for _, index := range indices {
		if index == nil {
			continue
		}
		buckets, ok := s.buckets[index.Name.O]
		if !ok {
			return nil, errors.NotFoundf("index %s in buckets info", index.Name.O)
		}
		log.Debug("buckets for index", zap.String("index", index.Name.O), zap.Reflect("buckets", buckets))

		// TODO build next one chunk

	}

	return nil, nil
}
