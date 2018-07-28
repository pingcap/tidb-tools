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
	"net"
	"time"

	"github.com/ngaut/log"
	"github.com/pingcap/tidb-tools/tidb-binlog/node"
	pb "github.com/pingcap/tipb/go-binlog"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

// PumpStatus saves pump's status
type PumpStatus struct {
	node.Status

	// the pump is avaliable or not.
	IsAvaliable bool

	// the client of this pump
	Client pb.PumpClient
}

// NewPumpStatus returns a new PumpStatus according to node's status.
func NewPumpStatus(status *node.Status) *PumpStatus {
	pumpStatus := &PumpStatus{}
	pumpStatus.Status = *status
	pumpStatus.IsAvaliable = (status.State == node.Online)

	if !pumpStatus.IsAvaliable {
		return pumpStatus
	}

	err := pumpStatus.createGrpcClient()
	if err != nil {
		log.Errorf("[pumps client] create grpc client for %s failed, error %v", status.NodeID, err)
		pumpStatus.IsAvaliable = false
	}

	return pumpStatus
}

// createGrpcClient create grpc client for online pump.
func (p *PumpStatus) createGrpcClient() error {
	if p.Client != nil {
		return nil
	}

	dialerOpt := grpc.WithDialer(func(addr string, timeout time.Duration) (net.Conn, error) {
		return net.DialTimeout("tcp", addr, timeout)
	})
	log.Debugf("[pumps client] create gcpc client at %s", p.Host)
	clientConn, err := grpc.Dial(p.Host, dialerOpt, grpc.WithInsecure())
	if err != nil {
		return err
	}
	p.Client = pb.NewPumpClient(clientConn)

	return nil
}

func (p *PumpStatus) writeBinlog(req *pb.WriteBinlogReq, timeout time.Duration) (*pb.WriteBinlogResp, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	resp, err := p.Client.WriteBinlog(ctx, req)
	cancel()

	return resp, err
}

func (p *PumpStatus) statusChanged(newStatus *node.Status) bool {
	if p.State != newStatus.State || p.Score != newStatus.Score {
		return true
	}
	return false
}
