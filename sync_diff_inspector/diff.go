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
	"github.com/pingcap/tidb-tools/sync_diff_inspector/config"
	"github.com/pingcap/tidb-tools/sync_diff_inspector/source"
	"github.com/pingcap/tidb-tools/sync_diff_inspector/splitter"
	"github.com/pingcap/tidb-tools/sync_diff_inspector/utils"
	tidbconfig "github.com/pingcap/tidb/config"
	"go.uber.org/zap"
)

const splitThreshold = 1000
const splitBound float64 = 3.

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
	fromUpstream      bool

	chunkCh chan *splitter.RangeInfo
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
		chunkCh: make(chan *splitter.RangeInfo, 1024),
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
	var startRange *splitter.RangeInfo
	if df.useCheckpoint {
		node, err := df.cp.LoadChunk()
		if err != nil {
			return nil, errors.Annotate(err, "the checkpoint load process failed")
		} else {
			// this need not be synchronized, because at the moment, the is only one thread access the section
			log.Info("load checkpoint",
				zap.Int("id", node.GetID()),
				zap.Reflect("chunk", node),
				zap.String("state", node.GetState()))
			df.cp.SetCurrentSavedID(node.GetID() + 1)
		}
		if node != nil {
			startRange = splitter.FromNode(node)
		}
	}
	df.fromUpstream = false
	return df.downstream.GenerateChunksIterator(startRange)
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

func (df *Diff) consume(ctx context.Context, rangeInfo *splitter.RangeInfo) (bool, error) {
	countCh := make(chan int64)
	upstreamChecksumCh := make(chan *source.ChecksumInfo)
	downstreamChecksumCh := make(chan *source.ChecksumInfo)
	var targetSource source.Source
	if df.fromUpstream {
		targetSource = df.upstream
		go df.upstream.GetCountAndCrc32(ctx, rangeInfo, countCh, upstreamChecksumCh)
		go df.downstream.GetCrc32(ctx, rangeInfo, downstreamChecksumCh)
	} else {
		targetSource = df.downstream
		go df.upstream.GetCrc32(ctx, rangeInfo, downstreamChecksumCh)
		go df.downstream.GetCountAndCrc32(ctx, rangeInfo, countCh, upstreamChecksumCh)
	}
	count := <-countCh
	log.Info("chunk size",
		zap.Int("chunk id", rangeInfo.ID),
		zap.String("schema", rangeInfo.Schema),
		zap.String("table", rangeInfo.Table))
	crc1Info := <-downstreamChecksumCh
	crc2Info := <-upstreamChecksumCh
	if crc1Info.Err != nil {
		return false, errors.Trace(crc1Info.Err)
	}
	if crc2Info.Err != nil {
		return false, errors.Trace(crc2Info.Err)
	}
	var tableRange *splitter.RangeInfo
	var err error
	if crc1Info.Checksum != crc2Info.Checksum {
		tableRange, err = df.BinGenerate(ctx, targetSource, rangeInfo, count)
		if err != nil {
			// TODO retry or log this chunk's error to checkpoint.
			return false, err
		}
	} else {
		tableRange = nil
	}

	var state string
	isEqual := true
	if tableRange != nil {
		state = checkpoints.FailedState
		// if the chunk's checksum differ, try to do binary check
		if df.chunkSize > splitThreshold {
			df.BinGenerate(ctx, targetSource, rangeInfo, count)
		}
		isEqual, err = df.compareRows(ctx, tableRange)
		if err != nil {
			return false, err
		}
	} else {
		// update chunk success state in summary
		state = checkpoints.SuccessState
	}

	node := rangeInfo.ToNode()
	node.State = state
	df.cp.Insert(node)
	return isEqual, nil
}

func (df *Diff) BinGenerate(ctx context.Context, targetSource source.Source, tableRange *splitter.RangeInfo, count int64) (*splitter.RangeInfo, error) {
	if count <= splitThreshold {
		return tableRange, nil
	}
	// TODO Find great index
	tableDiff := targetSource.GetTable(tableRange.TableIndex)
	indices := dbutil.FindAllIndex(tableDiff.Info)

	var (
		isEqual1, isEqual2 bool
		count1, count2     int64
	)
	tableRange1 := tableRange.Copy()
	tableRange2 := tableRange.Copy()
	// if no index, do not split
	if len(indices) == 0 {
		return tableRange, nil
	}
	// using the index
	index := indices[tableRange.IndexID]
	if index == nil {
		return nil, errors.NotFoundf("cannot found a index to split")
	}
	log.Debug("index for BinGerate", zap.String("index", index.Name.O))
	indexColumns := utils.GetColumnsFromIndex(index, tableDiff.Info)
	if len(indexColumns) == 0 {
		log.Warn("no index to split")
	}
	chunkLimits, args := tableRange.ChunkRange.ToString(tableDiff.Collation)
	limitRange := fmt.Sprintf("(%s) AND %s", chunkLimits, tableDiff.Range)
	midValues, err := dbutil.GetApproximateMidBySize(ctx, targetSource.GetDB(), tableRange.Schema, tableRange.Table, tableDiff.Info, limitRange, utils.StringsToInterfaces(args), count)
	if err != nil {
		return nil, errors.Trace(err)
	}
	for i, value := range midValues {
		tableRange1.ChunkRange.Update(indexColumns[i].Name.O, "", value, false, true)
		tableRange2.ChunkRange.Update(indexColumns[i].Name.O, value, "", true, false)
	}
	isEqual1, count1, err = df.compareChecksumAndGetCount(ctx, tableRange1)
	if err != nil {
		return nil, errors.Trace(err)
	}
	isEqual2, count2, err = df.compareChecksumAndGetCount(ctx, tableRange2)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if count1+count2 != count {
		log.Error("the count is not correct",
			zap.Int64("count1", count1),
			zap.Int64("count2", count2),
			zap.Int64("count", count))
		panic("count is not correct")
	}
	log.Info("chunk split successfully",
		zap.Int("chunk id", tableRange.ID),
		zap.Int64("count1", count1),
		zap.Int64("count2", count2))

	var cnt int64
	if df.fromUpstream {
		cnt = count1
	} else {
		cnt = count2
	}
	if !isEqual1 && !isEqual2 {
		return tableRange, nil
	} else if !isEqual1 {
		c, err := df.BinGenerate(ctx, targetSource, tableRange2, cnt)
		if err != nil {
			return nil, errors.Trace(err)
		}
		return c, nil
	} else if !isEqual2 {
		c, err := df.BinGenerate(ctx, targetSource, tableRange1, cnt)
		if err != nil {
			return nil, errors.Trace(err)
		}
		return c, nil
	} else {
		panic("error")
	}
}

func (df *Diff) compareChecksumAndGetCount(ctx context.Context, tableRange *splitter.RangeInfo) (bool, int64, error) {
	countCh := make(chan int64)
	upstreamChecksumCh := make(chan *source.ChecksumInfo)
	downstreamChecksumCh := make(chan *source.ChecksumInfo)
	if df.fromUpstream {
		go df.upstream.GetCountAndCrc32(ctx, tableRange, countCh, upstreamChecksumCh)
		go df.downstream.GetCrc32(ctx, tableRange, downstreamChecksumCh)
	} else {
		go df.upstream.GetCrc32(ctx, tableRange, downstreamChecksumCh)
		go df.downstream.GetCountAndCrc32(ctx, tableRange, countCh, upstreamChecksumCh)
	}
	count := <-countCh
	crc1Info := <-downstreamChecksumCh
	crc2Info := <-upstreamChecksumCh
	if crc1Info.Err != nil {
		return false, -1, errors.Trace(crc1Info.Err)
	}
	if crc2Info.Err != nil {
		return false, -1, errors.Trace(crc2Info.Err)
	}
	return true, count, nil
}

func (df *Diff) compareChecksum(ctx context.Context, tableRange *splitter.RangeInfo) (bool, error) {
	upstreamChecksumCh := make(chan *source.ChecksumInfo)
	go df.upstream.GetCrc32(ctx, tableRange, upstreamChecksumCh)
	downstreamChecksumCh := make(chan *source.ChecksumInfo)
	go df.downstream.GetCrc32(ctx, tableRange, downstreamChecksumCh)
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

func (df *Diff) compareRows(ctx context.Context, rangeInfo *splitter.RangeInfo) (bool, error) {
	upstreamRowsIterator, err := df.upstream.GetRowsIterator(ctx, rangeInfo)
	if err != nil {
		return false, errors.Trace(err)
	}
	defer upstreamRowsIterator.Close()
	downstreamRowsIterator, err := df.downstream.GetRowsIterator(ctx, rangeInfo)
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
				sql := df.downstream.GenerateDeleteDML(lastDownstreamData, rangeInfo.GetTableIndex())
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
				sql := df.downstream.GenerateReplaceDML(lastUpstreamData, rangeInfo.GetTableIndex())
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
		eq, cmp, err := utils.CompareData(lastUpstreamData, lastDownstreamData, df.downstream.GetOrderKeyCols(rangeInfo.GetTableIndex()))
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
			sql = df.downstream.GenerateDeleteDML(lastDownstreamData, rangeInfo.GetTableIndex())
			log.Info("[delete]", zap.String("sql", sql))
			lastDownstreamData = nil
		case -1:
			// insert
			sql = df.downstream.GenerateReplaceDML(lastUpstreamData, rangeInfo.GetTableIndex())
			log.Info("[insert]", zap.String("sql", sql))
			lastUpstreamData = nil
		case 0:
			// update
			sql = df.downstream.GenerateReplaceDML(lastUpstreamData, rangeInfo.GetTableIndex())
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
