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
	"math"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb-tools/tidb-binlog/node"
	pb "github.com/pingcap/tipb/go-binlog"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

var (
	// localPump is used to write local pump through unix socket connection.
	localPump = "localPump"

	// if pump failed more than defaultMaxFailedSecond seconds, this pump can treated as unavaliable.
	defaultMaxFailedSecond int64 = 300
)

type realPumpState struct {
	// the failed start unix timestamp of pump
	failedTS int64
}

func (p *realPumpState) isAvaliable() bool {
	return atomic.LoadInt64(&p.failedTS) != math.MaxInt64
}

func (p *realPumpState) markAvailable() {
	atomic.StoreInt64(&p.failedTS, 0)
}

func (p *realPumpState) markUnAvailable() {
	atomic.StoreInt64(&p.failedTS, math.MaxInt64)
}

func (p *realPumpState) updateFailedTS(ts int64) {
	var (
		originTS = atomic.LoadInt64(&p.failedTS)
		targetTS int64
	)
	if originTS == 0 {
		targetTS = ts
	} else if ts-originTS > defaultMaxFailedSecond {
		targetTS = math.MaxInt64
	}

	if targetTS > 0 {
		atomic.CompareAndSwapInt64(&p.failedTS, originTS, targetTS)
	}
}

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

	// the pump is avaliable or not, obsolete now
	IsAvaliable bool

	security *tls.Config

	grpcConn *grpc.ClientConn

	reCreateClient bool

	// the client of this pump
	Client pb.PumpClient

	state *realPumpState
}

// NewPumpStatus returns a new PumpStatus according to node's status.
func NewPumpStatus(status *node.Status, security *tls.Config) *PumpStatus {
	pumpStatus := PumpStatus{
		Status:         *status,
		security:       security,
		reCreateClient: true,
		state:          &realPumpState{},
	}
	return &pumpStatus
}

// MarkReCreateClient marks that we need to re-create grpc connection
func (p *PumpStatus) MarkReCreateClient() {
	p.Lock()
	p.reCreateClient = true
	p.Unlock()
}

// Close close pumps
func (p *PumpStatus) Close() {
	if p.grpcConn != nil {
		p.grpcConn.Close()
		p.grpcConn = nil
	}
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
			log.Debug("dial tcp", zap.String("addr", addr))
			return net.DialTimeout("tcp", addr, timeout)
		})
	}
	log.Debug("[pumps client] create grpc client", zap.String("address", p.Addr))
	var clientConn *grpc.ClientConn
	var err error
	if p.security != nil {
		clientConn, err = grpc.Dial(p.Addr, dialerOpt, grpc.WithTransportCredentials(credentials.NewTLS(p.security)))
	} else {
		clientConn, err = grpc.Dial(p.Addr, dialerOpt, grpc.WithInsecure())
	}
	if err != nil {
		p.state.updateFailedTS(int64(time.Now().Second()))
		return errors.Trace(err)
	}

	p.grpcConn = clientConn
	p.Client = pb.NewPumpClient(clientConn)

	return nil
}

// Reset resets the pump's real state.
func (p *PumpStatus) Reset() {
	p.state.markAvailable()
}

// WriteBinlog write binlog by grpc client.
func (p *PumpStatus) WriteBinlog(req *pb.WriteBinlogReq, timeout time.Duration) (*pb.WriteBinlogResp, error) {
	p.RLock()
	if p.reCreateClient || p.grpcConn == nil {
		p.RUnlock()
		p.Lock()
		if p.reCreateClient || p.grpcConn == nil {
			p.reCreateClient = false
			err := p.createGrpcClient()
			if err != nil {
				p.Unlock()
				log.Info("[pumps client] fail to create grpc client", zap.String("NodeID", p.NodeID))
				return nil, errors.Trace(err)
			}
		}

		p.Unlock()
		p.RLock()
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	resp, err := p.Client.WriteBinlog(ctx, req)
	p.RUnlock()

	cancel()

	if err == nil {
		p.state.markAvailable()
	} else {
		p.state.updateFailedTS(int64(time.Now().Second()))
	}

	return resp, err
}

// IsUsable returns true if pump is usable.
func (p *PumpStatus) IsUsable() bool {
	if !p.ShouldBeUsable() {
		return false
	}

	return p.state.isAvaliable()
}

// ShouldBeUsable returns true if pump should be usable
func (p *PumpStatus) ShouldBeUsable() bool {
	return p.Status.State == node.Online
}
