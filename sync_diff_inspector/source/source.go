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
	"strconv"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb-tools/pkg/dbutil"
	"github.com/pingcap/tidb-tools/sync_diff_inspector/checkpoints"
	"github.com/pingcap/tidb-tools/sync_diff_inspector/chunk"
	"github.com/pingcap/tidb-tools/sync_diff_inspector/config"
	"github.com/pingcap/tidb-tools/sync_diff_inspector/source/common"
	"github.com/pingcap/tidb/types"
	"go.uber.org/zap"
)

type DMLType int32

const (
	Insert DMLType = iota + 1
	Delete
	Replace
)

type RowData struct {
	Data   map[string]*dbutil.ColumnData
	Source int
}

// RowDatas is a heap of MergeItems.
type RowDatas struct {
	Rows         []RowData
	OrderKeyCols []*model.ColumnInfo
}

func (r RowDatas) Len() int { return len(r.Rows) }
func (r RowDatas) Less(i, j int) bool {
	for _, col := range r.OrderKeyCols {
		col1, ok := r.Rows[i].Data[col.Name.O]
		if !ok {
			log.Fatal("data don't have column", zap.String("column", col.Name.O), zap.Reflect("data", r.Rows[i].Data))
		}
		col2, ok := r.Rows[j].Data[col.Name.O]
		if !ok {
			log.Fatal("data don't have column", zap.String("column", col.Name.O), zap.Reflect("data", r.Rows[j].Data))
		}

		if col1.IsNull {
			if col2.IsNull {
				continue
			}

			return true
		}
		if col2.IsNull {
			return false
		}

		strData1 := string(col1.Data)
		strData2 := string(col2.Data)

		if needQuotes(col.FieldType) {
			if strData1 == strData2 {
				continue
			}
			if strData1 > strData2 {
				return false
			}
			return true
		}

		num1, err1 := strconv.ParseFloat(strData1, 64)
		if err1 != nil {
			log.Fatal("convert string to float failed", zap.String("column", col.Name.O), zap.String("data", strData1), zap.Error(err1))
		}
		num2, err2 := strconv.ParseFloat(strData2, 64)
		if err2 != nil {
			log.Fatal("convert string to float failed", zap.String("column", col.Name.O), zap.String("data", strData2), zap.Error(err2))
		}

		if num1 == num2 {
			continue
		}
		if num1 > num2 {
			return false
		}
		return true

	}

	return false
}
func (r RowDatas) Swap(i, j int) { r.Rows[i], r.Rows[j] = r.Rows[j], r.Rows[i] }

// Push implements heap.Interface's Push function
func (r *RowDatas) Push(x interface{}) {
	r.Rows = append(r.Rows, x.(RowData))
}

// Pop implements heap.Interface's Pop function
func (r *RowDatas) Pop() interface{} {
	if len(r.Rows) == 0 {
		return nil
	}
	old := r.Rows
	n := len(old)
	x := old[n-1]
	r.Rows = old[0 : n-1]
	return x
}

func needQuotes(ft types.FieldType) bool {
	return !(dbutil.IsNumberType(ft.Tp) || dbutil.IsFloatType(ft.Tp))
}

type RowDataIterator interface {
	Next() (map[string]*dbutil.ColumnData, error)
	GenerateFixSQL(t DMLType) (string, error)
	Close()
}

type ChecksumInfo struct {
	Checksum int64
	Err      error
	Cost     time.Duration
}

type TableRange struct {
	ChunkRange *chunk.Range
	TableIndex int
}

type Source interface {
	GenerateChunksIterator(chunkSize int, node checkpoints.Node) (DBIterator, error)
	GetCrc32(context.Context, *TableRange, chan *ChecksumInfo)
	GetOrderKeyCols(int) []*model.ColumnInfo
	GetRowsIterator(context.Context, *TableRange) (RowDataIterator, error)
	GenerateReplaceDML(map[string]*dbutil.ColumnData, int) string
	GenerateDeleteDML(map[string]*dbutil.ColumnData, int) string
	Close()
}

func NewSources(ctx context.Context, cfg *config.Config) (downstream Source, upstream Source, err error) {
	// upstream

	return
}

func NewSource(ctx context.Context, tableDiffs []*common.TableDiff, cfg ...*config.DBConfig) (Source, error) {
	if len(cfg) < 1 {
		return nil, errors.New(" source config")
	}
	switch cfg[0].DBType {
	case "tidb":
		return NewTiDBSource(ctx, tableDiffs, cfg[0])
	case "mysql":
		// TODO implement NewMysqlSource and NewMysqlSources
		if len(cfg) == 1 {
			// return NewMySQLSource(tableDiffs, &cfg.SourceDBCfg[0]
			//)
		} else {
			// return NewMySQLSources(tableDiffs, &cfg.SourceDBCfg)
		}
	default:
	}
	return nil, nil
}

// DBIterator generate next chunk for the whole tables lazily.
type DBIterator interface {
	Next() (*TableRange, error)
	// Next seeks the next chunk, return nil if seeks to end.
	Close()
}
