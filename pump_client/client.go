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
	"path"
	"time"

	//"github.com/pingcap/pd/pd-client"
	"github.com/juju/errors"
	//"github.com/ngaut/log"
	"github.com/pingcap/tidb-tools/pkg/etcd"
	pb "github.com/pingcap/tipb/go-binlog"
	"google.golang.org/grpc"
)

const (
	// DefaultEtcdTimeout is the default timeout config for etcd.
	DefaultEtcdTimeout = 5 * time.Second
)

// PumpsClient is the client of pumps.
type PumpsClient struct {
	Ctx context.Context

	// the client of pd.
	//PdClient  pd.Client

	// the client of etcd.
	EtcdCli *etcd.Client

	// Pumps saves the whole pumps' status.
	Pumps []*PumpStatus

	// AvliablePumps saves the whole avaliable pumps' status.
	AvaliablePumps []*PumpStatus

	// NeedCheckPumps saves the pumps need to be checked.
	NeedCheckPumps []*PumpStatus

	// Selector will select a suitable pump.
	Selector PumpSelector
}

// NewPumpsClient returns a PumpsClient.
func NewPumpsClient(ctx context.Context, endpoints []string, security *tls.Config, algorithm string) (*PumpsClient, error) {
	var selector PumpSelector
	switch algorithm {
	case Hash:
		selector = NewHashSelector()
	case Score:
		selector = NewScoreSelector()
	default:
		selector = NewHashSelector()
	}

	cli, err := etcd.NewClientFromCfg(endpoints, DefaultEtcdTimeout, RootPath, security)
	if err != nil {
		return nil, errors.Trace(err)
	}

	newPumpsClient := &PumpsClient{
		Ctx:      ctx,
		EtcdCli:  cli,
		Selector: selector,
	}

	err = newPumpsClient.GetPumpStatus(ctx)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return newPumpsClient, nil
}

// GetPumpStatus retruns all the pumps status in the etcd.
func (c *PumpsClient) GetPumpStatus(pctx context.Context) error {
	ctx, cancel := context.WithTimeout(pctx, DefaultEtcdTimeout)
	defer cancel()

	resp, err := c.EtcdCli.List(ctx, path.Join(RootPath))
	if err != nil {
		return errors.Trace(err)
	}
	nodesStatus, err := nodesStatusFromEtcdNode(resp)
	if err != nil {
		return errors.Trace(err)
	}

	for _, status := range nodesStatus {
		// TODO: use real info
		pumpStatus := &PumpStatus{
			NodeID:      status.NodeID,
			State:       Online,
			Score:       1,
			Label:       "",
			IsAvaliable: true,
			UpdateTime:  time.Now(),
		}
		c.Pumps = append(c.Pumps, pumpStatus)

		if pumpStatus.State == Online {
			dialerOpt := grpc.WithDialer(func(addr string, timeout time.Duration) (net.Conn, error) {
				return net.DialTimeout("tcp", addr, timeout)
			})
			clientConn, err := grpc.Dial(status.Host, dialerOpt, grpc.WithInsecure())
			if err != nil {
				return errors.Errorf("create grpc client for %s failed, error %v", status.NodeID, err)
			}
			pumpStatus.Client = pb.NewPumpClient(clientConn)
			c.AvaliablePumps = append(c.AvaliablePumps, pumpStatus)
		} else {
			c.NeedCheckPumps = append(c.NeedCheckPumps, pumpStatus)
		}
	}

	return nil
}
