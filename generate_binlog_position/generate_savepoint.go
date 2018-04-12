// Copyright 2016 PingCAP, Inc.
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
	"fmt"
	"os"
	"path"
	"strings"
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/pd/pd-client"
	"github.com/pingcap/tidb-tools/generate_binlog_position/pkg"
	"golang.org/x/net/context"
)

const physicalShiftBits = 18
const slowDist = 30 * time.Millisecond

// GenSavepointInfo generates drainer meta from pd
func GenSavepointInfo(cfg *Config) error {
	if err := os.MkdirAll(cfg.DataDir, 0700); err != nil {
		return errors.Trace(err)
	}

	// get newest ts from pd
	commitTS, err := getTSO(cfg)
	if err != nil {
		log.Errorf("get tso failed: %s", err)
		return errors.Trace(err)
	}

	// generate meta infomation
	meta := NewLocalMeta(path.Join(cfg.DataDir, "savePoint"))
	err = meta.Save(commitTS, cfg.TimeZone)
	return errors.Trace(err)
}

func getTSO(cfg *Config) (int64, error) {
	now := time.Now()

	urlv, err := pkg.NewURLsValue(cfg.EtcdURLs)
	if err != nil {
		return 0, errors.Trace(err)
	}

	pdCli, err := pd.NewClient(urlv.StringSlice(), pd.SecurityOption{
		CAPath:   cfg.SSLCA,
		CertPath: cfg.SSLCert,
		KeyPath:  cfg.SSLKey,
	})
	physical, logical, err := pdCli.GetTS(context.Background())
	if err != nil {
		return 0, errors.Trace(err)
	}
	dist := time.Since(now)
	if dist > slowDist {
		log.Warnf("get timestamp too slow: %s", dist)
	}

	return int64(composeTS(physical, logical)), nil
}

func composeTS(physical, logical int64) uint64 {
	return uint64((physical << physicalShiftBits) + logical)
}

func parseBinlogName(str string) (index uint64, err error) {
	if !strings.HasPrefix(str, "binlog-") {
		return 0, errors.Errorf("invalid binlog name %s", str)
	}

	_, err = fmt.Sscanf(str, "binlog-%016d", &index)
	return
}
