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
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb-tools/sync_diff_inspector/chunk"
	"github.com/pingcap/tidb-tools/sync_diff_inspector/config"
	"github.com/pingcap/tidb-tools/sync_diff_inspector/source"
	"github.com/pingcap/tidb-tools/sync_diff_inspector/source/common"
	tidbconfig "github.com/pingcap/tidb/config"
	"os"
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
	tables            map[string]map[string]*config.TableConfig
	fixSQLFile        *os.File

	chunkCh chan *chunk.Range
}

// NewDiff returns a Diff instance.
func NewDiff(cfg *config.Config) (diff *Diff, err error) {
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
		tables:            make(map[string]map[string]*config.TableConfig),

		// TODO use a meaningfull chunk channel buffer
		chunkCh: make(chan *chunk.Range, 1024),
	}

	if err = diff.init(cfg); err != nil {
		return nil, errors.Trace(err)
	}

	return diff, nil
}

func (df *Diff) init(cfg *config.Config) (err error) {
	// TODO adjust config
	setTiDBCfg()

	tableDiffs := make([]*common.TableDiff, 0, len(cfg.Tables))
	for _, table := range cfg.Tables {
		tableDiffs = append(tableDiffs, &common.TableDiff{
			// TODO build table diffs
			Schema: table.Schema,
		})
	}

	df.downstream, err = source.NewSource(tableDiffs, cfg.TargetDBCfg)
	if err != nil {
		return errors.Trace(err)
	}
	df.upstream, err = source.NewSource(tableDiffs, cfg.SourceDBCfg...)
	if err != nil {
		return errors.Trace(err)
	}

	return nil
}

// Equal tests whether two database have same data and schema.
func (df *Diff) Equal(ctx context.Context) error {
	chunksIter, err := df.generateChunks()
	if err != nil {
		return errors.Trace(err)
	}

	go df.handleChunks(ctx)

	for {
		c, err := chunksIter.Next()
		if err != nil {
			return errors.Trace(err)
		}
		if c == nil {
			// finish read chunks
			break
		}

		select {
		case <-ctx.Done():
			log.Info("Stop generate chunks by user canceled")
		// Produce chunk
		case df.chunkCh <- c:
		}
	}

	return nil
}

func (df *Diff) consume(chunk *chunk.Range) {
	crc1, err := df.upstream.GetCrc32(chunk)
	if err != nil {
		// retry or log this chunk's error to checkpoint.
	}
	crc2, err := df.downstream.GetCrc32(chunk)
	if err != nil {
		// retry or log this chunk's error to checkpoint.
	}
	if crc1 != crc2 {
		// 1. compare rows
		// 2. generate fix sql
	} else {
		// update chunk success state in summary
	}
}

func (df *Diff) handleChunks(ctx context.Context) {
	// TODO use a meaningfull count
	pool := NewWorkerPool(4, "consumer")
	for {
		select {
		case <-ctx.Done():
			log.Info("Stop consumer chunks by user canceled")
		case c := <-df.chunkCh:
			pool.Apply(func() {
				df.consume(c)
			})
		}
	}
}

func (df *Diff) generateChunks() (chunk.Iterator, error) {
	// TODO choose upstream or downstream to generate chunks
	// if isTiDB(df.upstream) {
	//		return df.upstream.GenerateChunks()
	// }
	// if isTiDB(df.downstream) {
	//		return df.downstream.GenerateChunks()
	//}
	return df.downstream.GenerateChunks()
}

func setTiDBCfg() {
	// to support long index key in TiDB
	tidbCfg := tidbconfig.GetGlobalConfig()
	// 3027 * 4 is the max value the MaxIndexLength can be set
	tidbCfg.MaxIndexLength = 3027 * 4
	tidbconfig.StoreGlobalConfig(tidbCfg)

	fmt.Println("set tidb cfg")
}
