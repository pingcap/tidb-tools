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
	"bytes"
	"os"
	"path"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/juju/errors"
	"github.com/ngaut/log"
	pd "github.com/pingcap/pd/pd-client"
	"github.com/pingcap/tidb-tools/pkg/utils"
	"github.com/siddontang/go/ioutil2"
	"golang.org/x/net/context"
)

const physicalShiftBits = 18
const slowDist = 30 * time.Millisecond

// generateMeta generates Meta from pd
func generateMetaInfo(cfg *Config) error {
	if err := os.MkdirAll(cfg.DataDir, 0700); err != nil {
		return errors.Trace(err)
	}

	// get newest ts from pd
	commitTS, err := GetTSO(cfg)
	if err != nil {
		log.Errorf("get tso failed: %s", err)
		return errors.Trace(err)
	}

	// generate meta file
	metaFileName := path.Join(cfg.DataDir, "savepoint")
	err = saveMeta(metaFileName, commitTS, cfg.TimeZone)
	return errors.Trace(err)
}

// GetTSO gets ts from pd
func GetTSO(cfg *Config) (int64, error) {
	now := time.Now()

	ectdEndpoints, err := utils.ParseHostPortAddr(cfg.EtcdURLs)
	if err != nil {
		return 0, errors.Trace(err)
	}

	pdCli, err := pd.NewClient(ectdEndpoints, pd.SecurityOption{
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

// Meta contains commit TS that can be used to specifies the location of the synchronized data
// TODO: improve meta later, like adding offset of kafka topic that corresponds to each pump node
type Meta struct {
	CommitTS int64 `toml:"commitTS" json:"commitTS"`
}

// saveMeta saves current tso in meta file.
func saveMeta(metaFileName string, ts int64, timeZone string) error {
	meta := &Meta{CommitTS: ts}

	var buf bytes.Buffer
	e := toml.NewEncoder(&buf)
	err := e.Encode(meta)
	if err != nil {
		return errors.Annotatef(err, "save meta %s into %s", meta, metaFileName)
	}

	if timeZone != "" {
		t := utils.TSOToRoughTime(ts)
		location, err1 := time.LoadLocation(timeZone)
		if err1 != nil {
			log.Warningf("fail to load location %s, error %v", timeZone, err1)
		} else {
			buf.WriteString(t.UTC().String())
			buf.WriteByte('\n')
			buf.WriteString(t.In(location).String())
		}
	}

	err = ioutil2.WriteFileAtomic(metaFileName, buf.Bytes(), 0644)
	if err != nil {
		return errors.Annotatef(err, "save meta %s into %s", meta, metaFileName)
	}

	log.Infof("meta: %+v", meta)
	return nil
}
