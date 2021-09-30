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

package main

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb-tools/pkg/dbutil"
	"github.com/pingcap/tidb-tools/sync_diff_inspector/checkpoints"
	"github.com/pingcap/tidb-tools/sync_diff_inspector/chunk"
	"github.com/pingcap/tidb-tools/sync_diff_inspector/config"
	"github.com/pingcap/tidb-tools/sync_diff_inspector/progress"
	"github.com/pingcap/tidb-tools/sync_diff_inspector/report"
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

// ChunkDML SQL struct for each chunk
type ChunkDML struct {
	node      *checkpoints.Node
	sqls      []string
	rowAdd    int
	rowDelete int
}

// Diff contains two sql DB, used for comparing.
type Diff struct {
	// we may have multiple sources in dm sharding sync.
	upstream   source.Source
	downstream source.Source

	// workSource is one of upstream/downstream by some policy in #pickSource.
	workSource source.Source

	sample            int
	checkThreadCount  int
	useChecksum       bool
	useCheckpoint     bool
	ignoreDataCheck   bool
	ignoreStructCheck bool
	ignoreStats       bool
	sqlWg             sync.WaitGroup
	checkpointWg      sync.WaitGroup

	FixSQLDir     string
	CheckpointDir string

	sqlCh      chan *ChunkDML
	cp         *checkpoints.Checkpoint
	startRange *splitter.RangeInfo
	report     *report.Report
}

// NewDiff returns a Diff instance.
func NewDiff(ctx context.Context, cfg *config.Config) (diff *Diff, err error) {
	diff = &Diff{
		sample:            cfg.Sample,
		checkThreadCount:  cfg.CheckThreadCount,
		useChecksum:       cfg.UseChecksum,
		useCheckpoint:     cfg.UseCheckpoint,
		ignoreDataCheck:   cfg.IgnoreDataCheck,
		ignoreStructCheck: cfg.IgnoreStructCheck,
		ignoreStats:       cfg.IgnoreStats,
		sqlCh:             make(chan *ChunkDML, splitter.DefaultChannelBuffer),
		cp:                new(checkpoints.Checkpoint),
		report:            report.NewReport(),
	}
	if err = diff.init(ctx, cfg); err != nil {
		diff.Close()
		return nil, errors.Trace(err)
	}

	return diff, nil
}

func (df *Diff) PrintSummary(ctx context.Context, taskCfg *config.TaskConfig) bool {
	// Stop updating progress bar so that summary won't be flushed.
	progress.Close()
	df.report.CalculateTotalSize(ctx, df.downstream.GetDB())
	err := df.report.CommitSummary(taskCfg)
	if err != nil {
		log.Fatal("failed to commit report", zap.Error(err))
	}
	df.report.Print("sync_diff.log", os.Stdout)
	return df.report.Result == report.Pass
}

func (df *Diff) Close() {
	if df.upstream != nil {
		df.upstream.Close()
	}
	if df.downstream != nil {
		df.downstream.Close()
	}

	failpoint.Inject("wait-for-checkpoint", func() {
		log.Info("failpoint wait-for-checkpoint injected, skip delete checkpoint file.")
		failpoint.Return()
	})

	if err := os.Remove(filepath.Join(df.CheckpointDir, checkpointFile)); err != nil && !os.IsNotExist(err) {
		log.Fatal("fail to remove the checkpoint file", zap.String("error", err.Error()))
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
	df.FixSQLDir = cfg.Task.FixDir
	df.CheckpointDir = cfg.Task.CheckpointDir

	sourceConfigs, targetConfig, err := getConfigsForReport(cfg)
	if err != nil {
		return errors.Trace(err)
	}
	df.report.Init(df.downstream.GetTables(), sourceConfigs, targetConfig)
	if err := df.initCheckpoint(); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (df *Diff) initCheckpoint() error {
	df.cp.Init()

	finishTableNums := 0
	if df.useCheckpoint {
		path := filepath.Join(df.CheckpointDir, checkpointFile)
		if ioutil2.FileExists(path) {
			node, reportInfo, err := df.cp.LoadChunk(path)
			if err != nil {
				return errors.Annotate(err, "the checkpoint load process failed")
			} else {
				// this need not be synchronized, because at the moment, the is only one thread access the section
				log.Info("load checkpoint",
					zap.Any("chunk index", node.GetID()),
					zap.Reflect("chunk", node),
					zap.String("state", node.GetState()))
				df.cp.SetCurrentSavedID(node)
			}

			if node != nil {
				// remove the sql file that ID bigger than node.
				// cause we will generate these sql again.
				err = df.removeSQLFiles(node.GetID())
				if err != nil {
					return errors.Trace(err)
				}
				df.startRange = splitter.FromNode(node)
				df.report.LoadReport(reportInfo)
				finishTableNums = df.startRange.GetTableIndex()
			}
		} else {
			log.Info("not found checkpoint file, start from beginning")
			id := &chunk.ChunkID{TableIndex: -1, BucketIndexLeft: -1, BucketIndexRight: -1, ChunkIndex: -1, ChunkCnt: 0}
			err := df.removeSQLFiles(id)
			if err != nil {
				return errors.Trace(err)
			}
		}
	}
	progress.Init(len(df.workSource.GetTables()), finishTableNums)
	return nil
}

func encodeReportConfig(config *report.ReportConfig) ([]byte, error) {
	buf := new(bytes.Buffer)
	if err := toml.NewEncoder(buf).Encode(config); err != nil {
		return nil, errors.Trace(err)
	}
	return buf.Bytes(), nil
}

func getConfigsForReport(cfg *config.Config) ([][]byte, []byte, error) {
	sourceConfigs := make([]*report.ReportConfig, len(cfg.Task.SourceInstances))
	for i := 0; i < len(cfg.Task.SourceInstances); i++ {
		instance := cfg.Task.SourceInstances[i]

		sourceConfigs[i] = &report.ReportConfig{
			Host:     instance.Host,
			Port:     instance.Port,
			User:     instance.User,
			Snapshot: instance.Snapshot,
			SqlMode:  instance.SqlMode,
		}
	}
	instance := cfg.Task.TargetInstance
	targetConfig := &report.ReportConfig{
		Host:     instance.Host,
		Port:     instance.Port,
		User:     instance.User,
		Snapshot: instance.Snapshot,
		SqlMode:  instance.SqlMode,
	}
	sourceBytes := make([][]byte, len(sourceConfigs))
	var err error
	for i := range sourceBytes {
		sourceBytes[i], err = encodeReportConfig(sourceConfigs[i])
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
	}
	targetBytes, err := encodeReportConfig(targetConfig)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	return sourceBytes, targetBytes, nil
}

// Equal tests whether two database have same data and schema.
func (df *Diff) Equal(ctx context.Context) error {
	chunksIter, err := df.generateChunksIterator(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	defer chunksIter.Close()
	pool := utils.NewWorkerPool(uint(df.checkThreadCount), "consumer")
	stopCh := make(chan struct{})

	df.checkpointWg.Add(1)
	go df.handleCheckpoints(ctx, stopCh)
	df.sqlWg.Add(1)
	go df.writeSQLs(ctx)

	defer func() {
		pool.WaitFinished()
		log.Debug("all comsume tasks finished")
		// close the sql channel
		close(df.sqlCh)
		df.sqlWg.Wait()
		stopCh <- struct{}{}
		df.checkpointWg.Wait()
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
		log.Info("chunk index", zap.Any("chunk index", c.ChunkRange.Index), zap.Any("chunk bound", c.ChunkRange.Bounds))
		pool.Apply(func() {
			isEqual := df.consume(ctx, c)
			if !isEqual {
				progress.FailTable(c.ProgressID)
			}
			progress.Inc(c.ProgressID)
		})
	}

	return nil
}

func (df *Diff) StructEqual(ctx context.Context) error {
	tables := df.downstream.GetTables()
	for tableIndex := range tables {
		structEq, err := df.compareStruct(ctx, tableIndex)
		if err != nil {
			return errors.Trace(err)
		}
		progress.RegisterTable(dbutil.TableName(tables[tableIndex].Schema, tables[tableIndex].Table), !structEq, false)
		df.report.SetTableStructCheckResult(tables[tableIndex].Schema, tables[tableIndex].Table, structEq)
	}
	return nil
}

func (df *Diff) compareStruct(ctx context.Context, tableIndex int) (structEq bool, err error) {
	sourceTableInfos, err := df.upstream.GetSourceStructInfo(ctx, tableIndex)
	if err != nil {
		return false, errors.Trace(err)
	}
	structEq = true
	for _, tableInfo := range sourceTableInfos {
		eq, _ := dbutil.EqualTableInfo(tableInfo, df.downstream.GetTables()[tableIndex].Info)
		structEq = structEq && eq
	}
	return structEq, nil
}

func (df *Diff) startGCKeeperForTiDB(ctx context.Context, db *sql.DB) {
	pdCli, _ := utils.GetPDClientForGC(ctx, db)
	if pdCli != nil {
		// Get latest snapshot
		snap, err := utils.GetSnapshot(ctx, db)
		if err != nil {
			log.Info("failed to get snapshot, user should guarantee the GC stopped during diff progress.")
			return
		}
		if len(snap) == 1 {
			err = utils.StartGCSavepointUpdateService(ctx, pdCli, db, snap[0])
			if err != nil {
				log.Info("failed to keep snapshot, user should guarantee the GC stopped during diff progress.")
			} else {
				log.Info("start update service to keep GC stopped automatically")
			}
		}
	}
}

// pickSource pick one proper source to do some work. e.g. generate chunks
func (df *Diff) pickSource(ctx context.Context) source.Source {
	workSource := df.downstream
	if ok, _ := dbutil.IsTiDB(ctx, df.upstream.GetDB()); ok {
		log.Info("The upstream is TiDB. pick it as work source candidate")
		df.startGCKeeperForTiDB(ctx, df.upstream.GetDB())
		workSource = df.upstream
	}
	if ok, _ := dbutil.IsTiDB(ctx, df.downstream.GetDB()); ok {
		log.Info("The downstream is TiDB. pick it as work source first")
		df.startGCKeeperForTiDB(ctx, df.downstream.GetDB())
		workSource = df.downstream
	}
	return workSource
}

func (df *Diff) generateChunksIterator(ctx context.Context) (source.RangeIterator, error) {
	return df.workSource.GetRangeIterator(ctx, df.startRange, df.workSource.GetTableAnalyzer())
}

func (df *Diff) handleCheckpoints(ctx context.Context, stopCh chan struct{}) {
	// a background goroutine which will insert the verified chunk,
	// and periodically save checkpoint
	log.Info("start handleCheckpoint goroutine")
	defer func() {
		log.Info("close handleCheckpoint goroutine")
		df.checkpointWg.Done()
	}()
	flush := func() {
		chunk := df.cp.GetChunkSnapshot()
		if chunk != nil {
			tableDiff := df.downstream.GetTables()[chunk.GetTableIndex()]
			schema, table := tableDiff.Schema, tableDiff.Table
			r, err := df.report.GetSnapshot(chunk.GetID(), schema, table)
			if err != nil {
				log.Warn("fail to save the report", zap.Error(err))
			}
			_, err = df.cp.SaveChunk(ctx, filepath.Join(df.CheckpointDir, checkpointFile), chunk, r)
			if err != nil {
				log.Warn("fail to save the chunk", zap.Error(err))
				// maybe we should panic, because SaveChunk method should not failed.
			}
		}
	}
	defer flush()
	for {
		select {
		case <-ctx.Done():
			log.Info("Stop do checkpoint by context done")
			return
		case <-stopCh:
			log.Info("Stop do checkpoint")
			return
		case <-time.After(10 * time.Second):
			flush()
		}
	}
}

func (df *Diff) consume(ctx context.Context, rangeInfo *splitter.RangeInfo) bool {
	tableDiff := df.downstream.GetTables()[rangeInfo.GetTableIndex()]
	schema, table := tableDiff.Schema, tableDiff.Table
	isEqual, count, err := df.compareChecksumAndGetCount(ctx, rangeInfo)
	if err != nil {
		df.report.SetTableMeetError(schema, table, err)
	}
	var state string
	dml := &ChunkDML{}
	if !isEqual {
		log.Debug("checksum failed", zap.Any("chunk id", rangeInfo.ChunkRange.Index), zap.Int64("chunk size", count), zap.String("table", df.workSource.GetTables()[rangeInfo.GetTableIndex()].Table))
		state = checkpoints.FailedState
		// if the chunk's checksum differ, try to do binary check
		info := rangeInfo
		if count > splitter.SplitThreshold {
			log.Debug("count greater than threshold, start do bingenerate", zap.Any("chunk id", rangeInfo.ChunkRange.Index), zap.Int64("chunk size", count))
			info, err = df.BinGenerate(ctx, df.workSource, rangeInfo, count)
			log.Debug("bin generate", zap.Reflect("info", info))
			log.Debug("bin generate finished", zap.Reflect("chunk", info.ChunkRange), zap.Any("chunk id", info.ChunkRange.Index))
			if err != nil {
				df.report.SetTableMeetError(schema, table, err)
			}
		}
		_, err := df.compareRows(ctx, info, dml)
		if err != nil {
			df.report.SetTableMeetError(schema, table, err)
		}
	} else {
		// update chunk success state in summary
		state = checkpoints.SuccessState
	}
	dml.node = rangeInfo.ToNode()
	dml.node.State = state
	id := rangeInfo.ChunkRange.Index
	df.report.SetTableDataCheckResult(schema, table, isEqual, dml.rowAdd, dml.rowDelete, id)
	df.sqlCh <- dml
	return isEqual
}

func (df *Diff) BinGenerate(ctx context.Context, targetSource source.Source, tableRange *splitter.RangeInfo, count int64) (*splitter.RangeInfo, error) {
	if count <= splitter.SplitThreshold {
		return tableRange, nil
	}
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
		log.Warn("cannot found a index to split and disable the BinGenerate",
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
	midValues, err := utils.GetApproximateMidBySize(ctx, targetSource.GetDB(), tableDiff.Schema, tableDiff.Table, tableDiff.Info, limitRange, args, count)
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
		log.Fatal("the count is not correct",
			zap.Int64("count1", count1),
			zap.Int64("count2", count2),
			zap.Int64("count", count))
	}
	log.Info("chunk split successfully",
		zap.Any("chunk id", tableRange.ChunkRange.Index),
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
	var wg sync.WaitGroup
	var upstreamInfo, downstreamInfo *source.ChecksumInfo
	wg.Add(1)
	go func() {
		defer wg.Done()
		upstreamInfo = df.upstream.GetCountAndCrc32(ctx, tableRange)
	}()
	downstreamInfo = df.downstream.GetCountAndCrc32(ctx, tableRange)
	wg.Wait()

	if upstreamInfo.Err != nil {
		log.Warn("failed to compare upstream checksum")
		return false, -1, errors.Trace(upstreamInfo.Err)
	}
	if downstreamInfo.Err != nil {
		log.Warn("failed to compare downstream checksum")
		return false, -1, errors.Trace(downstreamInfo.Err)

	}
	// TODO two counts are not necessary equal
	if upstreamInfo.Count == downstreamInfo.Count && upstreamInfo.Checksum == downstreamInfo.Checksum {
		return true, upstreamInfo.Count, nil
	}
	return false, upstreamInfo.Count, nil
}

func (df *Diff) compareRows(ctx context.Context, rangeInfo *splitter.RangeInfo, dml *ChunkDML) (bool, error) {
	rowsAdd, rowsDelete := 0, 0
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

	tableInfo := df.workSource.GetTables()[rangeInfo.GetTableIndex()].Info
	_, orderKeyCols := dbutil.SelectUniqueOrderKey(tableInfo)
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
				sql := df.downstream.GenerateFixSQL(source.Delete, lastUpstreamData, lastDownstreamData, rangeInfo.GetTableIndex())
				rowsDelete++
				log.Debug("[delete]", zap.String("sql", sql))

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
				sql := df.downstream.GenerateFixSQL(source.Insert, lastUpstreamData, lastDownstreamData, rangeInfo.GetTableIndex())
				rowsAdd++
				log.Debug("[insert]", zap.String("sql", sql))

				dml.sqls = append(dml.sqls, sql)
				equal = false

				lastUpstreamData, err = upstreamRowsIterator.Next()
				if err != nil {
					return false, err
				}
			}
			break
		}

		eq, cmp, err := utils.CompareData(lastUpstreamData, lastDownstreamData, orderKeyCols, tableInfo.Columns)
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
			sql = df.downstream.GenerateFixSQL(source.Delete, lastUpstreamData, lastDownstreamData, rangeInfo.GetTableIndex())
			rowsDelete++
			log.Debug("[delete]", zap.String("sql", sql))
			lastDownstreamData = nil
		case -1:
			// insert
			sql = df.downstream.GenerateFixSQL(source.Insert, lastUpstreamData, lastDownstreamData, rangeInfo.GetTableIndex())
			rowsAdd++
			log.Debug("[insert]", zap.String("sql", sql))
			lastUpstreamData = nil
		case 0:
			// update
			sql = df.downstream.GenerateFixSQL(source.Replace, lastUpstreamData, lastDownstreamData, rangeInfo.GetTableIndex())
			rowsAdd++
			rowsDelete++
			log.Debug("[update]", zap.String("sql", sql))
			lastUpstreamData = nil
			lastDownstreamData = nil
		}

		dml.sqls = append(dml.sqls, sql)
	}
	dml.rowAdd = rowsAdd
	dml.rowDelete = rowsDelete
	return equal, nil
}

// WriteSQLs write sqls to file
func (df *Diff) writeSQLs(ctx context.Context) {
	log.Info("start writeSQLs goroutine")
	defer func() {
		log.Info("close writeSQLs goroutine")
		df.sqlWg.Done()
	}()
	for {
		select {
		case <-ctx.Done():
			return
		case dml, ok := <-df.sqlCh:
			if !ok && dml == nil {
				log.Info("write sql channel closed")
				return
			}
			if len(dml.sqls) > 0 {
				fileName := fmt.Sprintf("%s.sql", utils.GetSQLFileName(dml.node.GetID()))
				fixSQLPath := filepath.Join(df.FixSQLDir, fileName)
				if ok := ioutil2.FileExists(fixSQLPath); ok {
					// unreachable
					log.Fatal("write sql failed: repeat sql happen", zap.Strings("sql", dml.sqls))
				}
				fixSQLFile, err := os.Create(fixSQLPath)
				if err != nil {
					log.Fatal("write sql failed: cannot create file", zap.Strings("sql", dml.sqls), zap.Error(err))
					continue
				}
				// write chunk meta
				chunkRange := dml.node.ChunkRange
				tableDiff := df.workSource.GetTables()[dml.node.GetTableIndex()]
				fixSQLFile.WriteString(fmt.Sprintf("-- table: %s.%s\n-- %s\n", tableDiff.Schema, tableDiff.Table, chunkRange.ToMeta()))
				for _, sql := range dml.sqls {
					_, err = fixSQLFile.WriteString(fmt.Sprintf("%s\n", sql))
					if err != nil {
						log.Fatal("write sql failed", zap.String("sql", sql), zap.Error(err))
					}
				}
				fixSQLFile.Close()
			}
			log.Debug("insert node", zap.Any("chunk index", dml.node.GetID()))
			df.cp.Insert(dml.node)
		}
	}
}

func (df *Diff) removeSQLFiles(checkPointId *chunk.ChunkID) error {
	ts := time.Now().Format("2006-01-02T15:04:05Z07:00")
	dirName := fmt.Sprintf(".trash-%s", ts)
	folderPath := filepath.Join(df.FixSQLDir, dirName)

	if _, err := os.Stat(folderPath); os.IsNotExist(err) {
		err = os.MkdirAll(folderPath, os.ModePerm)
		if err != nil {
			return errors.Trace(err)
		}
	}

	err := filepath.Walk(df.FixSQLDir, func(path string, f fs.FileInfo, err error) error {
		if os.IsNotExist(err) {
			// if path not exists, we should return nil to continue.
			return nil
		}
		if err != nil {
			return errors.Trace(err)
		}

		if f == nil || f.IsDir() {
			return nil
		}

		name := f.Name()
		// in mac osx, the path parameter is absolute path; in linux, the path is relative path to execution base dir,
		// so use Rel to convert to relative path to l.base
		relPath, _ := filepath.Rel(df.FixSQLDir, path)
		oldPath := filepath.Join(df.FixSQLDir, relPath)
		newPath := filepath.Join(folderPath, relPath)
		if strings.Contains(oldPath, ".trash") {
			return nil
		}

		if strings.HasSuffix(name, ".sql") {
			fileIDStr := strings.TrimRight(name, ".sql")
			tableIndex, bucketIndexLeft, bucketIndexRight, chunkIndex, err := utils.GetChunkIDFromSQLFileName(fileIDStr)
			if err != nil {
				return errors.Trace(err)
			}
			fileID := &chunk.ChunkID{
				TableIndex: tableIndex, BucketIndexLeft: bucketIndexLeft, BucketIndexRight: bucketIndexRight, ChunkIndex: chunkIndex, ChunkCnt: 0,
			}
			if err != nil {
				return errors.Trace(err)
			}
			if fileID.Compare(checkPointId) > 0 {
				// move to trash
				err = os.Rename(oldPath, newPath)
				if err != nil {
					return errors.Trace(err)
				}
			}
		}
		return nil
	})
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

func setTiDBCfg() {
	// to support long index key in TiDB
	tidbCfg := tidbconfig.GetGlobalConfig()
	// 3027 * 4 is the max value the MaxIndexLength can be set
	tidbCfg.MaxIndexLength = 3027 * 4
	tidbconfig.StoreGlobalConfig(tidbCfg)

	log.Info("set tidb cfg")
}
