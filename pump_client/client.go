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
	"encoding/json"
	"crypto/tls"
	"path"
	"strings"
	"sync"
	"time"

	//"github.com/pingcap/pd/pd-client"
	"github.com/coreos/etcd/mvcc/mvccpb"
	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb-tools/pkg/etcd"
	"github.com/pingcap/tidb-tools/pkg/utils"
	"github.com/pingcap/tidb-tools/tidb_binlog/node"
	pb "github.com/pingcap/tipb/go-binlog"
	"golang.org/x/net/context"
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

	ctx context.Context

	cancel context.CancelFunc

	wg sync.WaitGroup

	// the cluster id of this tidb cluster.
	ClusterID uint64

	// the client of etcd.
	EtcdCli *etcd.Client

	// Pumps saves the whole pumps' status.
	Pumps []*PumpStatus

	// PumpMap saves the map of pump's node and pump status.
	PumpMap map[string]*PumpStatus

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
		log.Warnf("unknow algorithm %s, use hash as default", algorithm)
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
		ctx:                ctx,
		cancel:             cancel,
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

	newPumpsClient.wg.Add(2)
	go newPumpsClient.WatchStatus()
	go newPumpsClient.Heartbeat()

	return newPumpsClient, nil
}

// GetPumpStatus retruns all the pumps status in the etcd.
func (c *PumpsClient) GetPumpStatus(pctx context.Context) error {
	c.Lock()
	defer c.Unlock()

	ctx, cancel := context.WithTimeout(pctx, DefaultEtcdTimeout)
	defer cancel()

	resp, err := c.EtcdCli.List(ctx, path.Join(RootPath))
	if err != nil {
		return errors.Trace(err)
	}
	nodesStatus, err := node.NodesStatusFromEtcdNode(resp)
	if err != nil {
		return errors.Trace(err)
	}

	for _, status := range nodesStatus {
		log.Infof("get pump %v from etcd", status)
		pumpStatus := NewPumpStatus(status)
		c.Pumps = append(c.Pumps, pumpStatus)
		c.PumpMap[status.NodeID] = pumpStatus
		if pumpStatus.State == node.Online {
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
			c.Lock()
			c.setPumpAvaliable(pump, false)
			c.Unlock()
			log.Infof("avaliable pumps: %v", c.AvaliablePumps)
			pump = c.Selector.Next(pump, binlog, i/5+1)
			log.Infof("write binlog choose pump %v", pump)
		}
		time.Sleep(time.Second)
	}

	return errors.New("write binlog failed")
}

// setPumpAvaliable set pump's isAvaliable, and modify NeedCheckPumps or AvaliablePumps.
func (c *PumpsClient) setPumpAvaliable(pump *PumpStatus, avaliable bool) {
	pump.IsAvaliable = avaliable
	if avaliable {
		for i, p := range c.NeedCheckPumps {
			if p.NodeID == pump.NodeID {
				c.NeedCheckPumps = append(c.NeedCheckPumps[:i], c.NeedCheckPumps[i+1:]...)
				break
			}
		}
		c.AvaliablePumps = append(c.AvaliablePumps, pump)
	} else {
		for j, p := range c.AvaliablePumps {
			if p.NodeID == pump.NodeID {
				c.AvaliablePumps = append(c.AvaliablePumps[:j], c.AvaliablePumps[j+1:]...)
				break
			}
		}
		c.NeedCheckPumps = append(c.NeedCheckPumps, pump)
	}

	c.Selector.SetPumps(c.AvaliablePumps)
}

// addPump add a new pump.
func (c *PumpsClient) addPump(pump *PumpStatus) {
	if pump.State == node.Online {
		pump.IsAvaliable = true
		c.AvaliablePumps = append(c.AvaliablePumps, pump)
	} else {
		pump.IsAvaliable = false
		c.NeedCheckPumps = append(c.NeedCheckPumps, pump)
	}

	// TODO: create grpc client.
	c.Pumps = append(c.Pumps, pump)
}

// removePump removes a pump.
func (c *PumpsClient) removePump(pump *PumpStatus) {
	// TODO
}

// WatchStatus watchs pump's status in etcd.
func (c *PumpsClient) WatchStatus() {
	defer c.wg.Done()
	rch := c.EtcdCli.Watch(c.ctx, RootPath)
	for {
		select {
		case <-c.ctx.Done():
			log.Info("pumps client watch status finished")
			return
		case wresp := <-rch:
			for _, ev := range wresp.Events {
				log.Infof("%s %q : %q\n", ev.Type, ev.Kv.Key, ev.Kv.Value)
				status := &node.Status
				err := json.Unmarshal(ev.Kv.Value, &status)
				if err != nil {
					log.Errorf("unmarshal status %q failed", ev.Kv.Value)
					continue
				}
				// strings.Contains(ev.Kv.Key, "alive")
				
				switch ev.Type {
				case mvccpb.PUT:
					if statusChanged(c.PumpMap[status.NodeID], status) {
						c.Lock()
						updateStatus(c.PumpMap[status.NodeID], status)
						if statue.State != node.Online {
							c.setPumpAvaliable(c.PumpMap[status.NodeID], false)
						}
						c.Unlock()
					}
					// judge pump's status is changed or not.
				case mvccpb.DELETE:
					if strings.Contains(ev.Kv.Key, "object") {
						// object node is deleted, means this node is offline.
						// c.RemovePump()
					} else {
						// set this pump's state to unknow, and this pump is not avaliable.
						c.Lock()
						if pumpStatus, ok := c.PumpMap[status.NodeID]; ok {
							pumpStatus.State = node.unknow
							c.setPumpAvaliable(pumpStatus, false)
						}
						c.Unlock()
					}
				}
			}
		}
	}
}

// Heartbeat send heartbeat request to NeedCheckPumps,
// if pump can return response, remove it from NeedCheckPumps.
func (c *PumpsClient) Heartbeat() {
	defer c.wg.Done()
	for {
		select {
		case <-c.ctx.Done():
			log.Infof("pump client heartbeat finished")
			return
		}

		// TODO: send heartbeat.
		// if heartbeat success, update c.NeedCheckPumps.

		// write fake binlog as heartbeat?
	}
}

// Close closes the PumpsClient.
func (c *PumpsClient) Close() {
	log.Infof("pumps client is closing")
	c.cancel()
	c.wg.Wait()
	log.Infof("pumps client is closed")
}
