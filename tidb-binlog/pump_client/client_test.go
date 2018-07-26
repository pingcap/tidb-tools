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

package client

import (
	"fmt"
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb-tools/tidb-binlog/node"
	pb "github.com/pingcap/tipb/go-binlog"
)

func TestClient(t *testing.T) {
	TestingT(t)
}

type testCase struct {
	binlogs     []*pb.Binlog
	choosePumps []*PumpStatus
	setAvliable []bool
	setNodeID   []string
}

var _ = Suite(&testClientSuite{})

type testClientSuite struct{}

func (*testClientSuite) TestPumpsClient(c *C) {
	pumpsClient := &PumpsClient{
		Pumps:              make(map[string]*PumpStatus),
		AvaliablePumps:     make(map[string]*PumpStatus),
		NeedCheckPumps:     make(map[string]*PumpStatus),
		Selector:           NewHashSelector(),
		RetryTime:          DefaultRetryTime,
		BinlogWriteTimeout: DefaultBinlogWriteTimeout,
	}

	pumps := []*PumpStatus{{}, {}, {}}
	for i, pump := range pumps {
		pump.NodeID = fmt.Sprintf("pump%d", i)
		pump.State = node.Offline
		// set pump client to avoid create grpc client.
		pump.Client = pb.NewPumpClient(nil)
	}

	for _, pump := range pumps {
		pumpsClient.addPump(pump)
	}
	pumpsClient.Selector.SetPumps(pumpsClient.AvaliablePumps)

	tCase := &testCase{}

	tCase.binlogs = []*pb.Binlog{
		{
			Tp:      pb.BinlogType_Prewrite,
			StartTs: 1,
		}, {
			Tp:       pb.BinlogType_Commit,
			StartTs:  1,
			CommitTs: 2,
		}, {
			Tp:      pb.BinlogType_Prewrite,
			StartTs: 3,
		}, {
			Tp:       pb.BinlogType_Commit,
			StartTs:  3,
			CommitTs: 4,
		}, {
			Tp:       pb.BinlogType_Commit,
			StartTs:  6,
			CommitTs: 7,
		},
	}

	tCase.setNodeID = []string{"pump0", "", "pump0", "pump1", "pump2"}
	tCase.setAvliable = []bool{true, false, false, true, true}
	tCase.choosePumps = []*PumpStatus{pumpsClient.Pumps["pump0"], pumpsClient.Pumps["pump0"], nil, pumpsClient.Pumps["pump1"], pumpsClient.Pumps["pump2"]}

	for i, nodeID := range tCase.setNodeID {
		if nodeID != "" {
			pumpsClient.setPumpAvaliable(pumpsClient.Pumps[nodeID], tCase.setAvliable[i])
		}
		pump := pumpsClient.Selector.Select(tCase.binlogs[i])
		c.Assert(pump, Equals, tCase.choosePumps[i])
	}
}
