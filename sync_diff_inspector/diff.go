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

package main

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb-tools/pkg/dbutil"
	"github.com/pingcap/tidb-tools/sync_diff_inspector/checkpoints"
	"github.com/pingcap/tidb-tools/sync_diff_inspector/chunk"
	"github.com/pingcap/tidb-tools/sync_diff_inspector/config"
	"github.com/pingcap/tidb-tools/sync_diff_inspector/source"
	"github.com/pingcap/tidb-tools/sync_diff_inspector/utils"
	tidbconfig "github.com/pingcap/tidb/config"
	"go.uber.org/zap"
)

// Diff contains two sql DB, used for comparing.
type Diff struct {
	// we may have multiple sources in dm sharding sync.
	upstream   source.Source
	downstream source.Source

	chunkSize         int
	sample            int
	checkThreadCount  int
	useChecksum       bool
	useCheckpoint     bool
	onlyUseChecksum   bool
	ignoreDataCheck   bool
	ignoreStructCheck bool
	ignoreStats       bool
	fixSQLFile        *os.File
	wg                sync.WaitGroup

	chunkCh chan *source.TableRange
	sqlCh   chan string
	cp      *checkpoints.Checkpoint
}

// NewDiff returns a Diff instance.
func NewDiff(ctx context.Context, cfg *config.Config) (diff *Diff, err error) {
	diff = &Diff{
		chunkSize:         cfg.ChunkSize,
		sample:            cfg.Sample,
		checkThreadCount:  cfg.CheckThreadCount,
		useChecksum:       cfg.UseChecksum,
		useCheckpoint:     cfg.UseCheckpoint,
		onlyUseChecksum:   cfg.OnlyUseChecksum,
		ignoreDataCheck:   cfg.IgnoreDataCheck,
		ignoreStructCheck: cfg.IgnoreStructCheck,
		ignoreStats:       cfg.IgnoreStats,
		//TODO add fixSQLFile
		//fixSQLFile: ???,
		// TODO use a meaningfull chunk channel buffer
		chunkCh: make(chan *source.TableRange, 1024),
		sqlCh:   make(chan string, 1024),
		cp:      new(checkpoints.Checkpoint),
	}

	if err = diff.init(ctx, cfg); err != nil {
		diff.Close()
		return nil, errors.Trace(err)
	}

	return diff, nil
}

func (df *Diff) Close() {
	// close sql channel
	close(df.sqlCh)
	// close checkpoint channel
	df.cp.Close()

	if df.fixSQLFile != nil {
		df.fixSQLFile.Close()
	}
	if df.upstream != nil {
		df.upstream.Close()
	}
	if df.downstream != nil {
		df.downstream.Close()
	}
}

func (df *Diff) init(ctx context.Context, cfg *config.Config) (err error) {
	// TODO adjust config
	setTiDBCfg()

	df.downstream, df.upstream, err = source.NewSources(ctx, cfg)

	df.fixSQLFile, err = os.Create(cfg.FixSQLFile)
	if err != nil {
		return errors.Trace(err)
	}
	df.cp.Init()
	return nil
}

// Equal tests whether two database have same data and schema.
func (df *Diff) Equal(ctx context.Context) error {
	chunksIter, err := df.generateChunksIterator()
	if err != nil {
		return errors.Trace(err)
	}
	defer chunksIter.Close()

	go df.handleChunks(ctx)

	go df.handleCheckpoints(ctx)
	go df.writeSQLs(ctx)

	for {
		c, err := chunksIter.Next()
		if err != nil {
			return errors.Trace(err)
		}
		if c == nil {
			// finish read the tables
			// if the chunksIter is done, close the chunkCh
			close(df.chunkCh)
			break
		}

		select {
		case <-ctx.Done():
			log.Info("Stop generate chunks by user canceled")
		// Produce chunk
		case df.chunkCh <- c:
		}
	}

	// release source
	df.Close()
	df.wg.Wait()
	return nil
}

func (df *Diff) generateChunksIterator() (source.DBIterator, error) {
	// TODO choose upstream or downstream to generate chunks
	// if isTiDB(df.upstream) {
	//		return df.upstream.GenerateChunksIterator()
	// }
	// if isTiDB(df.downstream) {
	//		return df.downstream.GenerateChunksIterator()
	//}
	var node checkpoints.Node
	var err error
	if df.useCheckpoint {
		node, err = df.cp.LoadChunk()
		if err != nil {
			log.Warn("the checkpoint load process failed, diable checkpoint and start from begining")
			df.useCheckpoint = false
		} else {
			// this need not be synchronized, because at the moment, the is only one thread access the section
			log.Info("load checkpoint",
				zap.Int("id", node.GetID()),
				zap.String("table", dbutil.TableName(node.GetSchema(), node.GetTable())),
				zap.Reflect("type", node.GetType()),
				zap.String("state", node.GetChunkState()))
			df.cp.SetCurrentSavedID(node.GetID() + 1)
		}
	}
	// if node != nil, generateChunksIterator from checkpoint
	// else generateChunkIterator from beginning
	return df.downstream.GenerateChunksIterator(node, source.Downstream)
}

func (df *Diff) handleCheckpoints(ctx context.Context) {
	// a background goroutine which will insert the verified chunk,
	// and periodically save checkpoint
	df.wg.Add(1)
	defer df.wg.Done()
	for {
		select {
		case <-ctx.Done():
			log.Info("Stop do checkpoint")
			return
		case <-time.After(10 * time.Second):
			_, err := df.cp.SaveChunk(ctx)
			if err != nil {
				log.Warn("fail to save the chunk", zap.Error(err))
				// maybe we should panic, because SaveChunk method should not failed.
			}
		case node, ok := <-df.cp.NodeChan:
			if !ok {
				log.Info("checkpoint channel closed")
				return
			}
			df.cp.Insert(node)
		}
	}
}

func (df *Diff) handleChunks(ctx context.Context) {
	df.wg.Add(1)
	defer df.wg.Done()
	// TODO use a meaningfull count
	pool := utils.NewWorkerPool(4, "consumer")
	for {
		select {
		case <-ctx.Done():
			log.Info("Stop consumer chunks by user canceled")
			return
			// TODO: close worker gracefully
		case c, ok := <-df.chunkCh:
			if !ok {
				return
			}
			pool.Apply(func() {
				res, err := df.consume(ctx, c)
				if err != nil {
					// TODO: catch error
				}
				// TODO: handle res
				if res {

				}
			})
		}
	}
}

func (df *Diff) consume(ctx context.Context, tableChunk *source.TableRange) (bool, error) {
	isEqual, err := df.compareChecksum(ctx, tableChunk)
	if err != nil {
		// TODO retry or log this chunk's error to checkpoint.
		return false, err
	}

	var node checkpoints.Node
	var state string
	if !isEqual {
		state = checkpoints.FailedState
		isEqual, err = df.compareRows(ctx, tableChunk)
		if err != nil {
			return false, err
		}
	} else {
		// update chunk success state in summary
		state = checkpoints.SuccessState
	}
	var from source.Source
	if tableChunk.From == source.Upstream {
		from = df.upstream
	} else {
		from = df.downstream
	}
	table := from.GetTable(tableChunk.TableIndex)
	uppers := make([]string, 0, len(tableChunk.ChunkRange.Bounds))
	columnName := make([]string, 0, len(tableChunk.ChunkRange.Bounds))
	for _, bound := range tableChunk.ChunkRange.Bounds {
		uppers = append(uppers, bound.Upper)
		columnName = append(columnName, bound.Column)
	}
	inner := checkpoints.Inner{
		Type: tableChunk.ChunkRange.Type,
		ID:   tableChunk.ChunkRange.ID,
		Schema: table.Schema,
		Table: table.Table,
		UpperBound: uppers,
		ColumnName: columnName,
		ChunkState: state,
	}
	switch tableChunk.ChunkRange.Type {
	case chunk.Bucket:
		bucketNode := &checkpoints.BucketNode{
			Inner: inner,
			// TODO need BucketID
			BucketID: 0,
			IndexID:  tableChunk.ChunkRange.IndexID,
		}
		node = bucketNode
	case chunk.Random:
		randomNode := &checkpoints.RandomNode{
			Inner: inner,
			// TODO need random value
		}
		node = randomNode
	}
	df.cp.NodeChan <- node
	return isEqual, nil
}

func (df *Diff) compareChecksum(ctx context.Context, tableChunk *source.TableRange) (bool, error) {
	upstreamChecksumCh := make(chan *source.ChecksumInfo)
	go df.upstream.GetCrc32(ctx, tableChunk, upstreamChecksumCh)
	downstreamChecksumCh := make(chan *source.ChecksumInfo)
	go df.downstream.GetCrc32(ctx, tableChunk, downstreamChecksumCh)
	crc1Info := <-downstreamChecksumCh
	crc2Info := <-upstreamChecksumCh
	if crc1Info.Err != nil {
		return false, errors.Trace(crc1Info.Err)
	}
	if crc2Info.Err != nil {
		return false, errors.Trace(crc2Info.Err)
	}
	if crc1Info.Checksum != crc2Info.Checksum {
		return false, nil
	}

	return true, nil
}

func (df *Diff) compareRows(ctx context.Context, tableChunk *source.TableRange) (bool, error) {
	upstreamRowsIterator, err := df.upstream.GetRowsIterator(ctx, tableChunk)
	if err != nil {
		return false, errors.Trace(err)
	}
	defer upstreamRowsIterator.Close()
	downstreamRowsIterator, err := df.downstream.GetRowsIterator(ctx, tableChunk)
	if err != nil {
		return false, errors.Trace(err)
	}
	defer downstreamRowsIterator.Close()

	var lastUpstreamData, lastDownstreamData map[string]*dbutil.ColumnData
	equal := true

	for {
		if lastUpstreamData == nil {
			lastUpstreamData, err = upstreamRowsIterator.Next()
			if err != nil {
				return false, err
			}
		}

		if lastDownstreamData == nil {
			lastDownstreamData, err = downstreamRowsIterator.Next()
			if err != nil {
				return false, err
			}
		}

		if lastUpstreamData == nil {
			// don't have source data, so all the targetRows's data is redundant, should be deleted
			for lastDownstreamData != nil {
				sql := df.downstream.GenerateDeleteDML(lastDownstreamData, tableChunk.TableIndex)
				log.Info("[delete]", zap.String("sql", sql))

				select {
				case df.sqlCh <- sql:
				case <-ctx.Done():
					return false, nil
				}
				equal = false
				lastDownstreamData, err = downstreamRowsIterator.Next()
				if err != nil {
					return false, err
				}
			}
			break
		}

		if lastDownstreamData != nil {
			// target lack some data, should insert the last source datas
			for lastUpstreamData != nil {
				sql := df.downstream.GenerateReplaceDML(lastUpstreamData, tableChunk.TableIndex)
				log.Info("[insert]", zap.String("sql", sql))

				select {
				case df.sqlCh <- sql:
				case <-ctx.Done():
					return false, nil
				}
				equal = false

				lastUpstreamData, err = upstreamRowsIterator.Next()
				if err != nil {
					return false, err
				}
			}
			break
		}

		// TODO where is orderKeycols from?
		eq, cmp, err := utils.CompareData(lastUpstreamData, lastDownstreamData, df.downstream.GetOrderKeyCols(tableChunk.TableIndex))
		if err != nil {
			return false, errors.Trace(err)
		}
		if eq {
			lastDownstreamData = nil
			lastUpstreamData = nil
			continue
		}

		equal = false
		sql := ""

		switch cmp {
		case 1:
			// delete
			sql = df.downstream.GenerateDeleteDML(lastDownstreamData, tableChunk.TableIndex)
			log.Info("[delete]", zap.String("sql", sql))
			lastDownstreamData = nil
		case -1:
			// insert
			sql = df.downstream.GenerateReplaceDML(lastUpstreamData, tableChunk.TableIndex)
			log.Info("[insert]", zap.String("sql", sql))
			lastUpstreamData = nil
		case 0:
			// update
			sql = df.downstream.GenerateReplaceDML(lastUpstreamData, tableChunk.TableIndex)
			log.Info("[update]", zap.String("sql", sql))
			lastUpstreamData = nil
			lastDownstreamData = nil
		}

		select {
		case df.sqlCh <- sql:
		case <-ctx.Done():
			return false, nil
		}
	}

	if equal {
		// Log
	} else {
		// log
	}

	return equal, nil
}

// WriteSQLs write sqls to file
func (df *Diff) writeSQLs(ctx context.Context) {
	df.wg.Add(1)
	defer df.wg.Done()

	for {
		select {
		case <-ctx.Done():
			return
		case dml, ok := <-df.sqlCh:
			if !ok {
				log.Info("write sql channel closed")
				return
			}
			_, err := df.fixSQLFile.WriteString(fmt.Sprintf("%s\n", dml))
			if err != nil {
				log.Error("write sql failed", zap.String("sql", dml), zap.Error(err))
			}
		}
	}
}

func setTiDBCfg() {
	// to support long index key in TiDB
	tidbCfg := tidbconfig.GetGlobalConfig()
	// 3027 * 4 is the max value the MaxIndexLength can be set
	tidbCfg.MaxIndexLength = 3027 * 4
	tidbconfig.StoreGlobalConfig(tidbCfg)

	fmt.Println("set tidb cfg")
}
