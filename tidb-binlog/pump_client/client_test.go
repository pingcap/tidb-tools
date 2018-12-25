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
	"net"
	"os"
	"testing"
	"time"

	"github.com/juju/errors"
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb-tools/tidb-binlog/node"
	binlog "github.com/pingcap/tipb/go-binlog"
	pb "github.com/pingcap/tipb/go-binlog"
	"google.golang.org/grpc"
)

var (
	testMaxRecvMsgSize = 1024
	testRetryTime      = 5
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
	algorithms := []string{Hash, Range}
	for _, algorithm := range algorithms {
		t.testSelector(c, algorithm)
	}
}

func (*testClientSuite) testSelector(c *C, algorithm string) {
	pumpsClient := &PumpsClient{
		Pumps:              NewPumpInfos(),
		Selector:           NewSelector(algorithm),
		RetryTime:          DefaultAllRetryTime,
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
		pumpsClient.Pumps.Pumps["pump1"], pumpsClient.Pumps.Pumps["pump1"], pumpsClient.Pumps.Pumps["pump1"]}

	for i, nodeID := range tCase.setNodeID {
		if nodeID != "" {
			pumpsClient.setPumpAvaliable(pumpsClient.Pumps.Pumps[nodeID], tCase.setAvliable[i])
		}
		pump := pumpsClient.Selector.Select(tCase.binlogs[i])
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

		pump1 := pumpsClient.Selector.Select(prewriteBinlog)
		if j%2 == 0 {
			pump1 = pumpsClient.Selector.Next(prewriteBinlog, 0)
		}

		pumpsClient.setPumpAvaliable(pump1, false)
		pump2 := pumpsClient.Selector.Select(commitBinlog)
		c.Assert(pump2.IsAvaliable, Equals, false)
		// prewrite binlog and commit binlog with same start ts should choose same pump
		c.Assert(pump1.NodeID, Equals, pump2.NodeID)

		pumpsClient.setPumpAvaliable(pump1, true)
		c.Assert(pump2.IsAvaliable, Equals, true)
	}
}

func (t *testClientSuite) TestWriteBinlog(c *C) {
	pumpServerConfig := []struct {
		addr       string
		serverMode string
	}{
		{
			"/tmp/mock-pump.sock",
			"unix",
		}, {
			"127.0.0.1:15049",
			"tcp",
		},
	}

	for _, cfg := range pumpServerConfig {
		pumpServer, err := createMockPumpServer(cfg.addr, cfg.serverMode)
		c.Assert(err, IsNil)

		opt := grpc.WithDialer(func(addr string, timeout time.Duration) (net.Conn, error) {
			return net.DialTimeout(cfg.serverMode, addr, timeout)
		})
		clientCon, err := grpc.Dial(cfg.addr, opt, grpc.WithInsecure())
		c.Assert(err, IsNil)
		c.Assert(clientCon, NotNil)
		pumpClient := mockPumpsClient(pb.NewPumpClient(clientCon))

		// test binlog size bigger than grpc's MaxRecvMsgSize
		blog := &pb.Binlog{
			Tp:            pb.BinlogType_Prewrite,
			PrewriteValue: make([]byte, testMaxRecvMsgSize+1),
		}
		err = pumpClient.WriteBinlog(blog)
		c.Assert(err, NotNil)

		for i := 0; i < 10; i++ {
			// test binlog size small than grpc's MaxRecvMsgSize
			blog = &pb.Binlog{
				Tp:            pb.BinlogType_Prewrite,
				PrewriteValue: make([]byte, 1),
			}
			err = pumpClient.WriteBinlog(blog)
			c.Assert(err, IsNil)
		}

		// after write some binlog, the pump without grpc client will move to unavaliable list in pump client.
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
		pumpServer.Close()

		CommitBinlogMaxRetryTime = time.Second
		// write commit binlog failed will not return error
		err = pumpClient.WriteBinlog(commitBinlog)
		c.Assert(err, IsNil)

		err = pumpClient.WriteBinlog(blog)
		c.Assert(err, NotNil)
	}
}

type mockPumpServer struct {
	mode   string
	addr   string
	server *grpc.Server

	retryTime int
}

// WriteBinlog implements PumpServer interface.
func (p *mockPumpServer) WriteBinlog(ctx context.Context, req *binlog.WriteBinlogReq) (*binlog.WriteBinlogResp, error) {
	p.retryTime++
	if p.retryTime < testRetryTime {
		return &binlog.WriteBinlogResp{}, errors.New("fake error")
	}

	// only the last retry will return succuess
	p.retryTime = 0
	return &binlog.WriteBinlogResp{}, nil
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

func createMockPumpServer(addr string, mode string) (*mockPumpServer, error) {
	if mode == "unix" {
		os.Remove(addr)
	}

	l, err := net.Listen(mode, addr)
	if err != nil {
		return nil, err
	}
	serv := grpc.NewServer(grpc.MaxRecvMsgSize(testMaxRecvMsgSize))
	pump := &mockPumpServer{
		mode:   mode,
		addr:   addr,
		server: serv,
	}
	pb.RegisterPumpServer(serv, pump)
	go serv.Serve(l)

	return pump, nil
}

// mockPumpsClient creates a PumpsClient, used for test.
func mockPumpsClient(client pb.PumpClient) *PumpsClient {
	// add a avaliable pump
	nodeID1 := "pump-1"
	pump1 := &PumpStatus{
		Status: node.Status{
			NodeID: nodeID1,
			State:  node.Online,
		},
		IsAvaliable: true,
		Client:      client,
	}

	// add a pump without grpc client
	nodeID2 := "pump-2"
	pump2 := &PumpStatus{
		Status: node.Status{
			NodeID: nodeID2,
			State:  node.Online,
		},
		IsAvaliable: true,
	}

	pumpInfos := NewPumpInfos()
	pumpInfos.Pumps[nodeID1] = pump1
	pumpInfos.AvaliablePumps[nodeID1] = pump1
	pumpInfos.Pumps[nodeID2] = pump2
	pumpInfos.AvaliablePumps[nodeID2] = pump2

	pCli := &PumpsClient{
		ClusterID: 1,
		Pumps:     pumpInfos,
		Selector:  NewSelector(Range),
		// have two pump, so use 2 * testRetryTime
		RetryTime:          2 * testRetryTime,
		BinlogWriteTimeout: 15 * time.Second,
	}
	pCli.Selector.SetPumps([]*PumpStatus{pump1, pump2})

	return pCli
}
