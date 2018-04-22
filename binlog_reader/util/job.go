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
