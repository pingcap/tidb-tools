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
	"crypto/tls"
	"net"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb-tools/tidb-binlog/node"
	pb "github.com/pingcap/tipb/go-binlog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

var (
	// localPump is used to write local pump through unix socket connection.
	localPump = "localPump"
)

// PumpStatus saves pump's status.
type PumpStatus struct {
	/*
		Pump has these state:
		Online:
			only when pump's state is online that pumps client can write binlog to.
		Pausing:
			this pump is pausing, and can't provide write binlog service. And this state will turn into Paused when pump is quit.
		Paused:
			this pump is paused, and can't provide write binlog service.
		Closing:
			this pump is closing, and can't provide write binlog service. And this state will turn into Offline when pump is quit.
		Offline:
			this pump is offline, and can't provide write binlog service forever.
	*/
	sync.RWMutex

	node.Status

	// the pump is avaliable or not
	IsAvaliable bool

	security *tls.Config

	grpcConn *grpc.ClientConn

	// the client of this pump
	Client pb.PumpClient
}

// NewPumpStatus returns a new PumpStatus according to node's status.
func NewPumpStatus(status *node.Status, security *tls.Config) *PumpStatus {
	pumpStatus := &PumpStatus{}
	pumpStatus.Status = *status
	pumpStatus.IsAvaliable = (status.State == node.Online)
	pumpStatus.security = security

	if status.State != node.Online {
		return pumpStatus
	}

	err := pumpStatus.createGrpcClient()
	if err != nil {
		Logger.Errorf("[pumps client] create grpc client for %s failed, error %v", status.NodeID, err)
		pumpStatus.IsAvaliable = false
	}

	return pumpStatus
}

// createGrpcClient create grpc client for online pump.
func (p *PumpStatus) createGrpcClient() error {
	// release the old connection, and create a new one
	if p.grpcConn != nil {
		p.grpcConn.Close()
	}

	var dialerOpt grpc.DialOption
	if p.NodeID == localPump {
		dialerOpt = grpc.WithDialer(func(addr string, timeout time.Duration) (net.Conn, error) {
			return net.DialTimeout("unix", addr, timeout)
		})
	} else {
		dialerOpt = grpc.WithDialer(func(addr string, timeout time.Duration) (net.Conn, error) {
			return net.DialTimeout("tcp", addr, timeout)
		})
	}
	Logger.Debugf("[pumps client] create gcpc client at %s", p.Addr)
	var clientConn *grpc.ClientConn
	var err error
	if p.security != nil {
		clientConn, err = grpc.Dial(p.Addr, dialerOpt, grpc.WithTransportCredentials(credentials.NewTLS(p.security)))
	} else {
		clientConn, err = grpc.Dial(p.Addr, dialerOpt, grpc.WithInsecure())
	}
	if err != nil {
		return err
	}

	p.grpcConn = clientConn
	p.Client = pb.NewPumpClient(clientConn)

	return nil
}

// ResetGrpcClient closes the pump's grpc connection.
func (p *PumpStatus) ResetGrpcClient() {
	p.Lock()
	defer p.UnLock()

	if p.grpcConn != nil {
		p.grpcConn.Close()
		p.Client = nil
	}
}

// WriteBinlog write binlog by grpc client.
func (p *PumpStatus) WriteBinlog(req *pb.WriteBinlogReq, timeout time.Duration) (*pb.WriteBinlogResp, error) {
	p.RLock()
	client := p.Client
	p.RUnlock()

	if client == nil {
		p.Lock()
		err := p.createGrpcClient()
		if err != nil {
			p.Unlock()
			return nil, errors.Errorf("create grpc connection for pump %s failed, error %v", p.NodeID, err)
		}
		client = p.Client
		p.Unlock()
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	resp, err := client.WriteBinlog(ctx, req)
	cancel()

	return resp, err
}
