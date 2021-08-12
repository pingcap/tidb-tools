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
	"crypto/md5"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb-tools/pkg/dbutil"
	"github.com/pingcap/tidb-tools/sync_diff_inspector/checkpoints"
	"github.com/pingcap/tidb-tools/sync_diff_inspector/config"
	"github.com/pingcap/tidb-tools/sync_diff_inspector/source"
	"github.com/pingcap/tidb-tools/sync_diff_inspector/splitter"
	"github.com/pingcap/tidb-tools/sync_diff_inspector/utils"
	tidbconfig "github.com/pingcap/tidb/config"
	"github.com/siddontang/go/ioutil2"
	"go.uber.org/zap"
)

const (
	// checkpointFile represents the checkpoints' file name which used for save and loads chunks
	checkpointFile = "sync_diff_checkpoints.pb"
)

// DML SQL struct for each chunk
type ChunkDML struct {
	node *checkpoints.Node
	sqls []string
}

// Diff contains two sql DB, used for comparing.
type Diff struct {
	// we may have multiple sources in dm sharding sync.
	upstream   source.Source
	downstream source.Source

	// workSource is one of upstream/downstream by some policy in #pickSource.
	workSource source.Source

	chunkSize         int
	sample            int
	checkThreadCount  int
	useChecksum       bool
	useCheckpoint     bool
	onlyUseChecksum   bool
	ignoreDataCheck   bool
	ignoreStructCheck bool
	ignoreStats       bool
	fixSQLFilePath    string
	wg                sync.WaitGroup

	sqlCh chan *ChunkDML
	cp    *checkpoints.Checkpoint

	configHash string
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
		sqlCh:             make(chan *ChunkDML, splitter.DefaultChannelBuffer),
		cp:                new(checkpoints.Checkpoint),
	}

	if err = diff.init(ctx, cfg); err != nil {
		diff.Close()
		return nil, errors.Trace(err)
	}

	return diff, nil
}

func (df *Diff) Close() {
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
	if err != nil {
		return errors.Trace(err)
	}

	df.workSource = df.pickSource(ctx)
	df.fixSQLFilePath = cfg.FixSQLFile

	df.cp.Init()
	df.configHash, err = df.ComputeConfigHash()
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

// Equal tests whether two database have same data and schema.
func (df *Diff) Equal(ctx context.Context) error {
	chunksIter, err := df.generateChunksIterator(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	defer chunksIter.Close()
	// TODO use a meaningfull count
	pool := utils.NewWorkerPool(64, "consumer")
	stopCh := make(chan struct{})

	go df.handleCheckpoints(ctx, stopCh)
	go df.writeSQLs(ctx)

	defer func() {
		pool.WaitFinished()
		// close the sql channel
		close(df.sqlCh)
		stopCh <- struct{}{}
		df.wg.Wait()
	}()

	for {
		c, err := chunksIter.Next(ctx)
		if err != nil {
			return errors.Trace(err)
		}
		if c == nil {
			// finish read the tables
			break
		}
		log.Debug("generate chunk", zap.Int("chunk id", c.ID))

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

	return nil
}

// pickSource pick one proper source to do some work. e.g. generate chunks
func (df *Diff) pickSource(ctx context.Context) source.Source {
	if ok, _ := dbutil.IsTiDB(ctx, df.upstream.GetDB()); ok {
		log.Info("The upstream is TiDB. pick it as work source")
		return df.upstream
	}
	if ok, _ := dbutil.IsTiDB(ctx, df.downstream.GetDB()); ok {
		log.Info("The downstream is TiDB. pick it as work source")
		return df.downstream
	}

	// if the both sides are not TiDB, choose any one of them would be ok
	log.Info("pick the downstream as work source")
	return df.downstream
}

func (df *Diff) ComputeConfigHash() (string, error) {
	checkConfigs, err := df.GetCheckConfig()
	if err != nil {
		return "", errors.Trace(err)
	}
	bytes := make([]byte, 0)
	for _, config := range checkConfigs {
		configBytes, err := json.Marshal(config)
		if err != nil {
			return "", errors.Trace(err)
		}
		bytes = append(bytes, configBytes...)
	}
	return fmt.Sprintf("%x", md5.Sum(bytes)), nil
}

func (df *Diff) GetCheckConfig() ([]*checkpoints.CheckConfig, error) {
	tableDiffs := df.workSource.GetTables()
	checkConfigs := make([]*checkpoints.CheckConfig, len(tableDiffs))
	for i, tableDiff := range tableDiffs {
		checkConfigs[i] = &checkpoints.CheckConfig{
			Table:  utils.UniqueID(tableDiff.Schema, tableDiff.Table),
			Fields: tableDiff.Fields,
			Range:  tableDiff.Range,
			// TODO get snapshot
			Snapshot:  "",
			Collation: tableDiff.Collation,
		}
	}
	return checkConfigs, nil
}

func (df *Diff) generateChunksIterator(ctx context.Context) (source.RangeIterator, error) {
	var startRange *splitter.RangeInfo
	if df.useCheckpoint {
		path := filepath.Join(df.configHash, checkpointFile)
		if ioutil2.FileExists(path) {
			node, err := df.cp.LoadChunk(path)
			if err != nil {
				return nil, errors.Annotate(err, "the checkpoint load process failed")
			} else {
				// this need not be synchronized, because at the moment, the is only one thread access the section
				log.Info("load checkpoint",
					zap.String("config hash", df.configHash),
					zap.Int("id", node.GetID()),
					zap.Reflect("chunk", node),
					zap.String("state", node.GetState()))
				df.cp.SetCurrentSavedID(node.GetID() + 1)
			}
			if node != nil {
				startRange = splitter.FromNode(node)
			}
		} else {
			df.useCheckpoint = false
		}
	}

	return df.workSource.GetRangeIterator(ctx, startRange, df.workSource.GetTableAnalyzer())
}

func (df *Diff) handleCheckpoints(ctx context.Context, stopCh chan struct{}) {
	// a background goroutine which will insert the verified chunk,
	// and periodically save checkpoint
	df.wg.Add(1)
	log.Debug("start handleCheckpoint goroutine")
	defer func() {
		log.Debug("close handleCheckpoint goroutine")
		df.wg.Done()
	}()
	flush := func() {
		if !ioutil2.FileExists(df.configHash) {
			err := os.Mkdir(df.configHash, checkpoints.LocalFilePerm)
			if err != nil {
				log.Error("fail to create checkpoint directory", zap.String("config hash", df.configHash))
			}
		}
		_, err := df.cp.SaveChunk(ctx, filepath.Join(df.configHash, checkpointFile))
		if err != nil {
			log.Warn("fail to save the chunk", zap.Error(err))
			// maybe we should panic, because SaveChunk method should not failed.
		}
	}
	defer flush()
	for {
		select {
		case <-ctx.Done():
			log.Info("Stop do checkpoint")
			return
		case <-stopCh:
			log.Info("Stop do checkpoint")
			return
		case <-time.After(10 * time.Second):
			flush()
		}
	}
}

func (df *Diff) consume(ctx context.Context, rangeInfo *splitter.RangeInfo) (bool, error) {
	isEqual, count, err := df.compareChecksumAndGetCount(ctx, rangeInfo)
	if err != nil {
		log.Warn("compute checksum error", zap.Int("chunk id", rangeInfo.ID))
		return false, errors.Trace(err)
	}
	log.Info("count size",
		zap.Int("chunk id", rangeInfo.ID),
		zap.Int64("chunk size", count))
	var state string
	dml := &ChunkDML{}
	if !isEqual {
		log.Debug("checksum failed", zap.Int("chunk id", rangeInfo.ID), zap.Int64("chunk size", count), zap.String("table", df.workSource.GetTable(rangeInfo.TableIndex).Table))
		state = checkpoints.FailedState
		// if the chunk's checksum differ, try to do binary check
		if count > splitter.SplitThreshold {
			log.Debug("count greater than threshold, start do bingenerate", zap.Int("chunk id", rangeInfo.ID), zap.Int64("chunk size", count))
			rangeInfo, err = df.BinGenerate(ctx, df.workSource, rangeInfo, count)
			log.Debug("bingenerate finished", zap.Reflect("chunk", rangeInfo.ChunkRange), zap.Int("chunk id", rangeInfo.ID))
			if err != nil {
				return false, errors.Trace(err)
			}
		}
		dml.sqls = make([]string, 0, 4)
		isEqual, err = df.compareRows(ctx, rangeInfo, dml)
		if err != nil {
			return false, errors.Trace(err)
		}
	} else {
		// update chunk success state in summary
		log.Debug("checksum success", zap.Int("chunk id", rangeInfo.ID), zap.Int64("chunk size", count), zap.String("table", df.workSource.GetTable(rangeInfo.TableIndex).Table))
		state = checkpoints.SuccessState
	}

	dml.node = rangeInfo.ToNode()
	dml.node.State = state
	df.sqlCh <- dml
	return isEqual, nil
}

func (df *Diff) BinGenerate(ctx context.Context, targetSource source.Source, tableRange *splitter.RangeInfo, count int64) (*splitter.RangeInfo, error) {
	if count <= splitter.SplitThreshold {
		return tableRange, nil
	}
	// TODO Find great index
	tableDiff := targetSource.GetTables()[tableRange.GetTableIndex()]
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
	var index *model.IndexInfo
	// using the index
	for _, i := range indices {
		if tableRange.IndexID == i.ID {
			index = i
			break
		}
	}
	if index == nil {
		log.Error("cannot found a index to split and disable the BinGenerate",
			zap.String("table", dbutil.TableName(tableDiff.Schema, tableDiff.Table)))
		return nil, nil
	}
	log.Debug("index for BinGenerate", zap.String("index", index.Name.O))
	indexColumns := utils.GetColumnsFromIndex(index, tableDiff.Info)
	if len(indexColumns) == 0 {
		log.Warn("no index to split")
	}
	chunkLimits, args := tableRange.ChunkRange.ToString(tableDiff.Collation)
	limitRange := fmt.Sprintf("(%s) AND %s", chunkLimits, tableDiff.Range)
	midValues, err := utils.GetApproximateMidBySize(ctx, targetSource.GetDB(), tableDiff.Schema, tableDiff.Table, tableDiff.Info, limitRange, utils.StringsToInterfaces(args), count)
	log.Debug("mid values", zap.Reflect("mid values", midValues), zap.Reflect("indices", indexColumns), zap.Reflect("bounds", tableRange.ChunkRange.Bounds))
	if err != nil {
		return nil, errors.Trace(err)
	}
	log.Debug("table ranges", zap.Reflect("original range", tableRange))
	for i := range indexColumns {
		log.Debug("update tableRange", zap.String("field", indexColumns[i].Name.O), zap.String("value", midValues[indexColumns[i].Name.O]))
		tableRange1.Update(indexColumns[i].Name.O, "", midValues[indexColumns[i].Name.O], false, true, tableDiff.Collation, tableDiff.Range)
		tableRange2.Update(indexColumns[i].Name.O, midValues[indexColumns[i].Name.O], "", true, false, tableDiff.Collation, tableDiff.Range)
	}
	log.Debug("table ranges", zap.Reflect("tableRange 1", tableRange1), zap.Reflect("tableRange 2", tableRange2))
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

	if !isEqual1 && !isEqual2 {
		return tableRange, nil
	} else if !isEqual1 {
		c, err := df.BinGenerate(ctx, targetSource, tableRange1, count1)
		if err != nil {
			return nil, errors.Trace(err)
		}
		return c, nil
	} else if !isEqual2 {
		c, err := df.BinGenerate(ctx, targetSource, tableRange2, count2)
		if err != nil {
			return nil, errors.Trace(err)
		}
		return c, nil
	} else {
		log.Fatal("the isEqual1 and isEqual2 cannot be both true")
		return nil, nil
	}
}

func (df *Diff) compareChecksumAndGetCount(ctx context.Context, tableRange *splitter.RangeInfo) (bool, int64, error) {
	checkSumCh := make(chan *source.ChecksumInfo, 2)
	go df.upstream.GetCountAndCrc32(ctx, tableRange, checkSumCh)
	go df.downstream.GetCountAndCrc32(ctx, tableRange, checkSumCh)

	crc1Info := <-checkSumCh
	crc2Info := <-checkSumCh
	close(checkSumCh)

	if crc1Info.Err != nil {
		log.Warn("checksum fail")
		return false, -1, errors.Trace(crc1Info.Err)
	}
	if crc2Info.Err != nil {
		log.Warn("checksum fail")
		return false, -1, errors.Trace(crc2Info.Err)

	}
	// TODO two counts are not necessary equal
	if crc1Info.Count == crc2Info.Count && crc1Info.Checksum == crc2Info.Checksum {
		return true, crc1Info.Count, nil
	}
	return false, crc1Info.Count, nil
}

func (df *Diff) compareRows(ctx context.Context, rangeInfo *splitter.RangeInfo, dml *ChunkDML) (bool, error) {
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
				sql := df.downstream.GenerateFixSQL(source.Delete, lastDownstreamData, rangeInfo.GetTableIndex())
				log.Info("[delete]", zap.String("sql", sql))

				dml.sqls = append(dml.sqls, sql)
				equal = false
				lastDownstreamData, err = downstreamRowsIterator.Next()
				if err != nil {
					return false, err
				}
			}
			break
		}

		if lastDownstreamData == nil {
			// target lack some data, should insert the last source datas
			for lastUpstreamData != nil {
				sql := df.downstream.GenerateFixSQL(source.Replace, lastUpstreamData, rangeInfo.GetTableIndex())
				log.Info("[insert]", zap.String("sql", sql))

				dml.sqls = append(dml.sqls, sql)
				equal = false

				lastUpstreamData, err = upstreamRowsIterator.Next()
				if err != nil {
					return false, err
				}
			}
			break
		}
		_, orderKeyCols := dbutil.SelectUniqueOrderKey(df.workSource.GetTables()[rangeInfo.GetTableIndex()].Info)
		eq, cmp, err := utils.CompareData(lastUpstreamData, lastDownstreamData, orderKeyCols)
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
			sql = df.downstream.GenerateFixSQL(source.Delete, lastDownstreamData, rangeInfo.GetTableIndex())
			log.Info("[delete]", zap.String("sql", sql))
			lastDownstreamData = nil
		case -1:
			// insert
			sql = df.downstream.GenerateFixSQL(source.Replace, lastUpstreamData, rangeInfo.GetTableIndex())
			log.Info("[insert]", zap.String("sql", sql))
			lastUpstreamData = nil
		case 0:
			// update
			sql = df.downstream.GenerateFixSQL(source.Replace, lastUpstreamData, rangeInfo.GetTableIndex())
			log.Info("[update]", zap.String("sql", sql))
			lastUpstreamData = nil
			lastDownstreamData = nil
		}

		dml.sqls = append(dml.sqls, sql)
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
	log.Info("start writeSQLs goroutine")
	defer func() {
		log.Info("close writeSQLs goroutine")
		df.wg.Done()
	}()
	for {
		select {
		case <-ctx.Done():
			return
		case dml, ok := <-df.sqlCh:
			if !ok {
				log.Info("write sql channel closed")
				return
			}
			if len(dml.sqls) > 0 {
				fixSQLFile, err := os.Create(fmt.Sprintf("%s_%d", df.fixSQLFilePath, dml.node.GetID()))
				defer fixSQLFile.Close()
				if err != nil {
					log.Error("write sql failed: cannot create file", zap.Strings("sql", dml.sqls), zap.Error(err))
					continue
				}
				for _, sql := range dml.sqls {
					_, err = fixSQLFile.WriteString(fmt.Sprintf("%s\n", sql))
					if err != nil {
						log.Error("write sql failed", zap.String("sql", sql), zap.Error(err))
					}
				}
			}
			df.cp.Insert(dml.node)
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
