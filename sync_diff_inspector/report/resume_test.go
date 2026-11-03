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

package report

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/pingcap/tidb-tools/sync_diff_inspector/chunk"
	"github.com/pingcap/tidb-tools/sync_diff_inspector/config"
	"github.com/pingcap/tidb-tools/sync_diff_inspector/source/common"
	"github.com/stretchr/testify/require"
)

func TestResumeRetainsMismatchBeforeCheckpoint(t *testing.T) {
	task := &config.TaskConfig{OutputDir: t.TempDir()}
	tables := []*common.TableDiff{{Schema: "test", Table: "a"}, {Schema: "test", Table: "b"}}
	original := NewReport(task)
	original.Init(tables, nil, nil)
	original.SetTableStructCheckResult("test", "a", true, false, common.AllTableExistFlag)
	original.SetTableDataCheckResult("test", "a", false, 1, 1, 2, 2, &chunk.ChunkID{TableIndex: 0})
	checkpoint := &chunk.ChunkID{TableIndex: 1}
	snapshot, err := original.GetSnapshot(checkpoint, "test", "b")
	require.NoError(t, err)
	data, err := json.Marshal(snapshot)
	require.NoError(t, err)
	var saved Report
	require.NoError(t, json.Unmarshal(data, &saved))
	resumed := NewReport(task)
	resumed.Init(tables, nil, nil)
	resumed.LoadReport(&saved)
	require.Equal(t, Fail, resumed.Result)
	require.False(t, resumed.TableResults["test"]["a"].DataEqual)
	require.NoError(t, resumed.CommitSummary())
	require.EqualValues(t, 1, resumed.FailedNum)
	var summary bytes.Buffer
	require.NoError(t, resumed.Print(&summary))
	require.NotContains(t, summary.String(), "all are equal")
}

func TestLoadLegacyReportDerivesVerdict(t *testing.T) {
	for _, field := range []string{"data-equal", "struct-equal"} {
		t.Run(field, func(t *testing.T) {
			var saved Report
			data := `{"table-results":{"test":{"t":{"schema":"test","table":"t","struct-equal":true,"data-equal":true}}}}`
			require.NoError(t, json.Unmarshal([]byte(data), &saved))
			if field == "data-equal" {
				saved.TableResults["test"]["t"].DataEqual = false
			} else {
				saved.TableResults["test"]["t"].StructEqual = false
			}
			resumed := NewReport(&config.TaskConfig{OutputDir: t.TempDir()})
			resumed.LoadReport(&saved)
			require.Equal(t, Fail, resumed.Result)
			require.NoError(t, resumed.CommitSummary())
			require.Equal(t, Fail, resumed.Result)
		})
	}
}

func TestSnapshotRetainsSkippedTable(t *testing.T) {
	original := NewReport(&config.TaskConfig{OutputDir: t.TempDir()})
	original.Init([]*common.TableDiff{{Schema: "test", Table: "a"}, {Schema: "test", Table: "b"}}, nil, nil)
	original.SetTableStructCheckResult("test", "a", false, true, common.DownstreamTableLackFlag)
	snapshot, err := original.GetSnapshot(&chunk.ChunkID{TableIndex: 1}, "test", "b")
	require.NoError(t, err)
	require.Contains(t, snapshot.TableResults["test"], "a")
	require.Equal(t, common.DownstreamTableLackFlag, snapshot.TableResults["test"]["a"].TableLack)
	require.True(t, snapshot.TableResults["test"]["a"].DataSkip)
}

func TestRejectCheckpointMissingCompletedTable(t *testing.T) {
	tables := []*common.TableDiff{{Schema: "test", Table: "a"}, {Schema: "test", Table: "b"}}
	saved := NewReport(nil)
	saved.Init(tables[1:], nil, nil)
	require.ErrorContains(t, saved.ValidateCheckpoint(tables, 1), "missing completed table results")
	saved.Init(tables, nil, nil)
	require.NoError(t, saved.ValidateCheckpoint(tables, 1))
	require.ErrorContains(t, saved.ValidateCheckpoint(tables, 2), "invalid checkpoint")
}
