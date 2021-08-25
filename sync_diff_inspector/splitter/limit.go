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
	"fmt"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb-tools/pkg/dbutil"
	"github.com/pingcap/tidb-tools/sync_diff_inspector/chunk"
	"github.com/pingcap/tidb-tools/sync_diff_inspector/progress"
	"github.com/pingcap/tidb-tools/sync_diff_inspector/source/common"
	"github.com/pingcap/tidb-tools/sync_diff_inspector/utils"
	"go.uber.org/zap"
)

type LimitIterator struct {
	table     *common.TableDiff
	tagChunk  *chunk.Range
	queryTmpl string

	indexID int64

	chunksCh chan *chunk.Range
	errCh    chan error
	cancel   context.CancelFunc
	dbConn   *sql.DB

	progressID string
}

func NewLimitIterator(ctx context.Context, progressID string, table *common.TableDiff, dbConn *sql.DB, chunkSize int) (*LimitIterator, error) {
	return NewLimitIteratorWithCheckpoint(ctx, progressID, table, dbConn, chunkSize, nil)
}

func NewLimitIteratorWithCheckpoint(ctx context.Context, progressID string, table *common.TableDiff, dbConn *sql.DB, chunkSize int, startRange *RangeInfo) (*LimitIterator, error) {
	indices := dbutil.FindAllIndex(table.Info)
	if err := utils.GetBetterIndex(ctx, dbConn, table.Schema, table.Table, table.Info, indices); err != nil {
		return nil, errors.Trace(err)
	}
	var indexColumns []*model.ColumnInfo
	tagChunk := chunk.NewChunkRange()
	chunksCh := make(chan *chunk.Range, DefaultChannelBuffer)
	errCh := make(chan error)
	var indexID int64
	for _, index := range indices {
		if index == nil {
			continue
		}
		if startRange != nil && startRange.IndexID != index.ID {
			continue
		}
		log.Debug("Limit select index", zap.String("index", index.Name.O))

		indexColumns = utils.GetColumnsFromIndex(index, table.Info)

		if len(indexColumns) < len(index.Columns) {
			// some column in index is ignored.
			log.Debug("indexColumns empty, try next index")
			indexColumns = nil
			continue
		}

		indexID = index.ID
		if startRange != nil {
			bounds := startRange.ChunkRange.Bounds
			if len(bounds) != len(indexColumns) {
				log.Warn("checkpoint node columns are not equal to selected index columns, skip checkpoint.")
				break
			}
			for _, bound := range bounds {
				if !bound.HasUpper {
					// TODO deal with this situation gracefully
					log.Warn("checkpoint node has none-Upper bound, skip checkpoint.")
					break
				}
				tagChunk.Update(bound.Column, bound.Upper, "", true, false)
			}
		}
		break
	}

	if indexColumns == nil {
		return nil, errors.NotFoundf("not found index")
	}

	// There are only 10k chunks at most
	if chunkSize <= 0 {
		cnt, err := dbutil.GetRowCount(ctx, dbConn, table.Schema, table.Table, "", nil)
		if err != nil {
			return nil, errors.Trace(err)
		}
		chunkSize = SplitThreshold
		if len(table.Info.Indices) != 0 {
			// use binary checksum
			chunkSize = SplitThreshold * 2
			chunkSize2 := int(cnt / 10000)
			if chunkSize2 >= chunkSize {
				chunkSize = chunkSize2
			}
		}
	}

	ctxx, cancel := context.WithCancel(ctx)
	queryTmpl := generateLimitQueryTemplate(indexColumns, table, chunkSize)

	limitIterator := &LimitIterator{
		table,
		tagChunk,
		queryTmpl,

		indexID,

		chunksCh,
		errCh,

		cancel,
		dbConn,

		progressID,
	}

	progress.StartTable(progressID, 0, false)
	go limitIterator.produceChunks(ctxx)

	return limitIterator, nil
}

func (lmt *LimitIterator) Close() {
	lmt.cancel()
}

func (lmt *LimitIterator) Next() (*chunk.Range, error) {
	select {
	case err := <-lmt.errCh:
		return nil, errors.Trace(err)
	case c, ok := <-lmt.chunksCh:
		if !ok && c == nil {
			return nil, nil
		}
		return c, nil
	}
}

func (lmt *LimitIterator) GetIndexID() int64 {
	return lmt.indexID
}

func (lmt *LimitIterator) produceChunks(ctx context.Context) {
	for {
		where, args := lmt.tagChunk.ToString(lmt.table.Collation)
		query := fmt.Sprintf(lmt.queryTmpl, where)
		dataMap, err := lmt.getLimitRow(ctx, query, args)
		if err != nil {
			select {
			case <-ctx.Done():
			case lmt.errCh <- errors.Trace(err):
			}
			return
		}

		chunkRange := lmt.tagChunk
		lmt.tagChunk = nil
		if dataMap == nil {
			// there is no row in result set
			chunk.InitChunk(chunkRange, chunk.Limit, 0, lmt.table.Collation, lmt.table.Range)
			progress.UpdateTotal(lmt.progressID, 1, true)
			select {
			case <-ctx.Done():
			case lmt.chunksCh <- chunkRange:
			}
			close(lmt.chunksCh)
			return
		}

		newTagChunk := chunk.NewChunkRange()
		for column, data := range dataMap {
			newTagChunk.Update(column, string(data.Data), "", !data.IsNull, false)
			chunkRange.Update(column, "", string(data.Data), false, !data.IsNull)
		}

		chunk.InitChunk(chunkRange, chunk.Limit, 0, lmt.table.Collation, lmt.table.Range)
		progress.UpdateTotal(lmt.progressID, 1, false)
		select {
		case <-ctx.Done():
			return
		case lmt.chunksCh <- chunkRange:
		}
		lmt.tagChunk = newTagChunk
	}
}

func (lmt *LimitIterator) getLimitRow(ctx context.Context, query string, args []string) (map[string]*dbutil.ColumnData, error) {
	rows, err := lmt.dbConn.QueryContext(ctx, query, utils.StringsToInterfaces(args)...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return nil, err
		}
		return nil, nil
	}
	dataMap, err := dbutil.ScanRow(rows)
	if err != nil {
		return nil, err
	}
	return dataMap, nil
}

func generateLimitQueryTemplate(indexColumns []*model.ColumnInfo, table *common.TableDiff, chunkSize int) string {
	fields := make([]string, 0, len(indexColumns))
	for _, columnInfo := range indexColumns {
		fields = append(fields, dbutil.ColumnName(columnInfo.Name.O))
	}
	columns := strings.Join(fields, ", ")

	return fmt.Sprintf("SELECT %s FROM %s WHERE %%s ORDER BY %s LIMIT %d,1", columns, dbutil.TableName(table.Schema, table.Table), columns, chunkSize)
}
