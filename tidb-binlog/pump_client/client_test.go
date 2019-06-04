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
	"context"
	"fmt"
	"math"
	"net"
	"os"
	"sync/atomic"
	"testing"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb-tools/tidb-binlog/node"
	binlog "github.com/pingcap/tipb/go-binlog"
	pb "github.com/pingcap/tipb/go-binlog"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"google.golang.org/grpc"
)

var (
	testMaxRecvMsgSize = 1024
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

func (t *testClientSuite) TestSelector(c *C) {
	strategys := []string{Hash, Range}
	for _, strategy := range strategys {
		t.testSelector(c, strategy)
	}
}

func (*testClientSuite) testSelector(c *C, strategy string) {
	pumpsClient := &PumpsClient{
		Pumps:              NewPumpInfos(),
		Selector:           NewSelector(strategy),
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
		pumpsClient.addPump(pump, false)
	}
	pumpsClient.Selector.SetPumps(copyPumps(pumpsClient.Pumps.AvaliablePumps))

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
			Tp:      pb.BinlogType_Prewrite,
			StartTs: 5,
		}, {
			Tp:       pb.BinlogType_Commit,
			StartTs:  5,
			CommitTs: 6,
		},
	}

	tCase.setNodeID = []string{"pump0", "", "pump0", "pump1", "", "pump2"}
	tCase.setAvliable = []bool{true, false, false, true, false, true}
	tCase.choosePumps = []*PumpStatus{pumpsClient.Pumps.Pumps["pump0"], pumpsClient.Pumps.Pumps["pump0"], nil,
		nil, pumpsClient.Pumps.Pumps["pump1"], pumpsClient.Pumps.Pumps["pump1"]}

	for i, nodeID := range tCase.setNodeID {
		if nodeID != "" {
			pumpsClient.setPumpAvaliable(pumpsClient.Pumps.Pumps[nodeID], tCase.setAvliable[i])
		}
		pump := pumpsClient.Selector.Select(tCase.binlogs[i], 0)
		pumpsClient.Selector.Feedback(tCase.binlogs[i].StartTs, tCase.binlogs[i].Tp, pump)
		c.Assert(pump, Equals, tCase.choosePumps[i])
	}

	for j := 0; j < 10; j++ {
		prewriteBinlog := &pb.Binlog{
			Tp:      pb.BinlogType_Prewrite,
			StartTs: int64(j),
		}
		commitBinlog := &pb.Binlog{
			Tp:      pb.BinlogType_Commit,
			StartTs: int64(j),
		}

		pump1 := pumpsClient.Selector.Select(prewriteBinlog, 0)
		if j%2 == 0 {
			pump1 = pumpsClient.Selector.Select(prewriteBinlog, 1)
		}
		pumpsClient.Selector.Feedback(prewriteBinlog.StartTs, prewriteBinlog.Tp, pump1)

		pumpsClient.setPumpAvaliable(pump1, false)
		pump2 := pumpsClient.Selector.Select(commitBinlog, 0)
		pumpsClient.Selector.Feedback(commitBinlog.StartTs, commitBinlog.Tp, pump2)
		// prewrite binlog and commit binlog with same start ts should choose same pump
		c.Assert(pump1.NodeID, Equals, pump2.NodeID)
		pumpsClient.setPumpAvaliable(pump1, true)

		// after change strategy, prewrite binlog and commit binlog will choose same pump
		pump1 = pumpsClient.Selector.Select(prewriteBinlog, 0)
		pumpsClient.Selector.Feedback(prewriteBinlog.StartTs, prewriteBinlog.Tp, pump1)
		if strategy == Range {
			err := pumpsClient.SetSelectStrategy(Hash)
			c.Assert(err, IsNil)
		} else {
			err := pumpsClient.SetSelectStrategy(Range)
			c.Assert(err, IsNil)
		}
		pump2 = pumpsClient.Selector.Select(commitBinlog, 0)
		c.Assert(pump1.NodeID, Equals, pump2.NodeID)

		// set back
		err := pumpsClient.SetSelectStrategy(strategy)
		c.Assert(err, IsNil)
	}
}

func (t *testClientSuite) TestNormalWriteBinlog(c *C) {

}

type pumpInstance struct {
	nodeID          string
	Status          string
	addr            string
	serverMode      string
	writeSuccessPer int64
}

func mustCreateServers(c *C, pumpServerConfig []pumpInstance) (servers []*mockPumpServer) {
	for _, cfg := range pumpServerConfig {
		pumpServer, err := createMockPumpServer(cfg.addr, cfg.serverMode, cfg.writeSuccessPer)
		c.Assert(err, IsNil)
		servers = append(servers, pumpServer)
	}

	return
}

func (t *testClientSuite) TestWriteBinlog(c *C) {
	log.SetLevel(zapcore.DebugLevel)
	pumpServerConfig := []pumpInstance{
		{
			localPump,
			node.Online,
			"/tmp/mock-pump.sock",
			"unix",
			1,
		}, {
			"Node-1",
			node.Online,
			"127.0.0.1:15049",
			"tcp",
			1,
		}, {
			"Node-2",
			node.Online,
			"127.0.0.1:15050",
			"tcp",
			math.MinInt64, // never return success
		},
	}

	// make test faster
	RetryInterval = 100 * time.Millisecond
	CommitBinlogTimeout = time.Second

	servers := mustCreateServers(c, pumpServerConfig)
	c.Log("create all server success")

	pumpClient := mockPumpsClient(pumpServerConfig)

	// test binlog size bigger than grpc's MaxRecvMsgSize
	blog := &pb.Binlog{
		Tp:            pb.BinlogType_Prewrite,
		PrewriteValue: make([]byte, testMaxRecvMsgSize+1),
	}
	err := pumpClient.WriteBinlog(blog)
	c.Assert(err, NotNil)

	for i := 0; i < 50; i++ {
		// test binlog size small than grpc's MaxRecvMsgSize
		blog = &pb.Binlog{
			Tp:            pb.BinlogType_Prewrite,
			PrewriteValue: make([]byte, 1),
		}
		err = pumpClient.WriteBinlog(blog)
		c.Assert(err, IsNil)
	}

	// after write some binlog, the never return success pump will be move to unavailable list in pump client.
	c.Assert(len(pumpClient.Pumps.UnAvaliablePumps), Equals, 1)

	// test write commit binlog, will not return error although write binlog failed.
	preWriteBinlog := &pb.Binlog{
		Tp:            pb.BinlogType_Prewrite,
		StartTs:       123,
		PrewriteValue: make([]byte, 1),
	}
	commitBinlog := &pb.Binlog{
		Tp:            pb.BinlogType_Commit,
		StartTs:       123,
		CommitTs:      123,
		PrewriteValue: make([]byte, 1),
	}

	err = pumpClient.WriteBinlog(preWriteBinlog)
	c.Assert(err, IsNil)

	// test when pump is down
	for _, s := range servers {
		s.Close()
	}

	// write commit binlog failed will not return error
	err = pumpClient.WriteBinlog(commitBinlog)
	c.Assert(err, IsNil)

	err = pumpClient.WriteBinlog(blog)
	c.Assert(err, NotNil)

	// recover after recreate servers
	servers = mustCreateServers(c, pumpServerConfig)
	err = pumpClient.WriteBinlog(blog)
	c.Assert(err, IsNil)
}

type mockPumpServer struct {
	mode   string
	addr   string
	server *grpc.Server

	writeCount int64
	// return success for every writeSuccessPer write
	// 1 means always success
	writeSuccessPer int64
}

// WriteBinlog implements PumpServer interface.
func (p *mockPumpServer) WriteBinlog(ctx context.Context, req *binlog.WriteBinlogReq) (*binlog.WriteBinlogResp, error) {
	p.writeCount++
	if p.writeCount%atomic.LoadInt64(&p.writeSuccessPer) == 0 {
		return &binlog.WriteBinlogResp{}, nil
	} else {
		return &binlog.WriteBinlogResp{}, errors.New("fake error")
	}
}

// PullBinlogs implements PumpServer interface.
func (p *mockPumpServer) PullBinlogs(req *binlog.PullBinlogReq, srv binlog.Pump_PullBinlogsServer) error {
	return nil
}

func (p *mockPumpServer) Close() {
	p.server.Stop()
	if p.mode == "unix" {
		os.Remove(p.addr)
	}
}

func createMockPumpServer(addr string, mode string, writeSuccessPer int64) (*mockPumpServer, error) {
	if mode == "unix" {
		os.Remove(addr)
	}

	l, err := net.Listen(mode, addr)
	if err != nil {
		return nil, err
	}
	serv := grpc.NewServer(grpc.MaxRecvMsgSize(testMaxRecvMsgSize))
	pump := &mockPumpServer{
		mode:            mode,
		addr:            addr,
		server:          serv,
		writeCount:      0,
		writeSuccessPer: writeSuccessPer,
	}
	pb.RegisterPumpServer(serv, pump)
	go func() {
		err := serv.Serve(l)
		if err != nil {
			log.Error("serve err", zap.Error(err))
		}
	}()

	return pump, nil
}

// mockPumpsClient creates a PumpsClient, used for test.
func mockPumpsClient(instances []pumpInstance) *PumpsClient {
	pumpInfos := NewPumpInfos()
	var pumps []*PumpStatus

	for _, ins := range instances {
		nodeID := ins.nodeID
		status := &node.Status{
			NodeID: nodeID,
			State:  ins.Status,
			Addr:   ins.addr,
		}
		pump := NewPumpStatus(status, nil)

		pumpInfos.Pumps[nodeID] = pump
		pumpInfos.AvaliablePumps[nodeID] = pump

		pumps = append(pumps, pump)
	}

	pCli := &PumpsClient{
		ClusterID:          1,
		Pumps:              pumpInfos,
		Selector:           NewSelector(Range),
		BinlogWriteTimeout: time.Second,
	}

	pCli.Selector.SetPumps(pumps)

	return pCli
}
