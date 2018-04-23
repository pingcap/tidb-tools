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

package util

import (
	"fmt"

	"github.com/juju/errors"
	"github.com/pingcap/tidb"
	"github.com/pingcap/tidb-tools/generate_binlog_position/pkg"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/store/tikv"
)

// LoadHistoryDDLJobs load history ddl jobs
func LoadHistoryDDLJobs(etcdUrls string, initCommitTS int64) ([]*model.Job, error) {
	var version kv.Version
	var err error

	urlv, err := pkg.NewURLsValue(etcdUrls)
	if err != nil {
		return nil, errors.Trace(err)
	}

	tidb.RegisterStore("tikv", tikv.Driver{})
	tiPath := fmt.Sprintf("tikv://%s?disableGC=true", urlv.HostString())
	tiStore, err := tidb.NewStore(tiPath)
	if err != nil {
		return nil, errors.Trace(err)
	}

	if initCommitTS == 0 {
		version, err = tiStore.CurrentVersion()
		if err != nil {
			return nil, errors.Trace(err)
		}
	} else {
		version = kv.NewVersion(uint64(initCommitTS))
	}

	snapshot, err := tiStore.GetSnapshot(version)
	snapMeta := meta.NewSnapshotMeta(snapshot)
	jobs, err := snapMeta.GetAllHistoryDDLJobs()
	if err != nil {
		return nil, errors.Trace(err)
	}

	return jobs, nil
}
