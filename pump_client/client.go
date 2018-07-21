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
	"strings"
	"sync"
	"time"

	//"github.com/pingcap/pd/pd-client"
	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb-tools/pkg/etcd"
	"github.com/pingcap/tidb-tools/pkg/utils"
	pb "github.com/pingcap/tipb/go-binlog"
	"google.golang.org/grpc"
)

const (
	// DefaultEtcdTimeout is the default timeout config for etcd.
	DefaultEtcdTimeout = 5 * time.Second

	// DefaultRetryTime is the default time of retry.
	DefaultRetryTime = 20

	// DefaultBinlogWriteTimeout is the default max time binlog can use to write to pump.
	DefaultBinlogWriteTimeout = 15 * time.Second
)

// PumpsClient is the client of pumps.
type PumpsClient struct {
	sync.RWMutex

	Ctx context.Context

	Cancel context.CancelFunc

	// the cluster id of this tidb cluster.
	ClusterID uint64

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

	// the max retry time if write binlog failed.
	RetryTime int

	// BinlogWriteTimeout is the max time binlog can use to write to pump.
	BinlogWriteTimeout time.Duration
}

// NewPumpsClient returns a PumpsClient.
func NewPumpsClient(etcdURLs string, security *tls.Config, algorithm string) (*PumpsClient, error) {
	var selector PumpSelector
	switch algorithm {
	case Hash:
		selector = NewHashSelector()
	case Score:
		selector = NewScoreSelector()
	default:
		selector = NewHashSelector()
	}

	ectdEndpoints, err := utils.ParseHostPortAddr(etcdURLs)
	if err != nil {
		return nil, errors.Trace(err)
	}

	cli, err := etcd.NewClientFromCfg(ectdEndpoints, DefaultEtcdTimeout, RootPath, security)
	if err != nil {
		return nil, errors.Trace(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	newPumpsClient := &PumpsClient{
		Ctx:                ctx,
		Cancel:             cancel,
		EtcdCli:            cli,
		Selector:           selector,
		RetryTime:          DefaultRetryTime,
		BinlogWriteTimeout: DefaultBinlogWriteTimeout,
	}

	err = newPumpsClient.GetPumpStatus(ctx)
	if err != nil {
		return nil, errors.Trace(err)
	}
	newPumpsClient.Selector.SetPumps(newPumpsClient.AvaliablePumps)

	go newPumpsClient.WatchStatus(ctx)
	go newPumpsClient.Heartbeat(ctx)

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
		log.Infof("get pump %v from etcd", status)
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
			log.Infof("create gcpc client at %s", status.Host)
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

// WriteBinlog writes binlog to a situable pump.
func (c *PumpsClient) WriteBinlog(clusterID uint64, binlog *pb.Binlog) error {
	c.RLock()
	pump := c.Selector.Select(binlog)
	c.RUnlock()
	log.Infof("write binlog choose pump %v", pump)

	commitData, err := binlog.Marshal()
	if err != nil {
		return errors.Trace(err)
	}
	req := &pb.WriteBinlogReq{ClusterID: clusterID, Payload: commitData}

	// Retry many times because we may raise CRITICAL error here.
	for i := 0; i < c.RetryTime; i++ {
		if pump == nil {
			return errors.New("no pump can use")
		}

		var resp *pb.WriteBinlogResp
		ctx, cancel := context.WithTimeout(context.Background(), c.BinlogWriteTimeout)
		resp, err = pump.Client.WriteBinlog(ctx, req)
		cancel()
		if err == nil && resp.Errmsg != "" {
			err = errors.New(resp.Errmsg)
		}
		if err == nil {
			return nil
		}
		if strings.Contains(err.Error(), "received message larger than max") {
			// This kind of error is not critical and not retryable, return directly.
			return errors.Errorf("binlog data is too large (%s)", err.Error())
		}

		log.Errorf("write binlog error %v", err)

		// every pump can retry 5 times, if retry 5 times and still failed, set this pump unavaliable, and choose a new pump.
		if (i+1)%5 == 0 {
			c.SetPumpAvaliable(pump, false)
			pump = c.Selector.Next(pump, binlog, i/5+1)
		}
		time.Sleep(time.Second)
	}

	return nil
}

// SetPumpAvaliable set pump's isAvaliable, and modify NeedCheckPumps or AvaliablePumps.
func (c *PumpsClient) SetPumpAvaliable(pump *PumpStatus, avaliable bool) {
	c.Lock()
	defer c.Unlock()

	pump.IsAvaliable = avaliable
	if avaliable {
		for i, p := range c.NeedCheckPumps {
			if p.NodeID == pump.NodeID {
				c.NeedCheckPumps = append(c.NeedCheckPumps[:i], c.NeedCheckPumps[i+1:]...)
				break
			}
		}
	} else {
		for j, p := range c.AvaliablePumps {
			if p.NodeID == pump.NodeID {
				c.AvaliablePumps = append(c.AvaliablePumps[:j], c.AvaliablePumps[j+1:]...)
				break
			}
		}
	}
}

// WatchStatus watchs pump's status in etcd.
func (c *PumpsClient) WatchStatus(ctx context.Context) {
	// TODO
}

// Heartbeat send heartbeat request to NeedCheckPumps,
// if pump can return response, remove it from NeedCheckPumps.
func (c *PumpsClient) Heartbeat(ctx context.Context) {
	// TODO
}
