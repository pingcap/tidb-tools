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
	"crypto/tls"
	"encoding/json"
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

	// DefaultHeartbeatWaitTime is the default wait time for send heartbeat to unavaliable pumps.
	DefaultHeartbeatWaitTime = 30 * time.Second

	aliveStr  = "alive"
	objectStr = "object"

	fakeClusterID uint64 = 110119120
)

// PumpsClient is the client of pumps.
type PumpsClient struct {
	sync.RWMutex

	ctx context.Context

	cancel context.CancelFunc

	wg sync.WaitGroup

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

	err = newPumpsClient.getPumpStatus(ctx)
	if err != nil {
		return nil, errors.Trace(err)
	}
	newPumpsClient.Selector.SetPumps(newPumpsClient.AvaliablePumps)

	newPumpsClient.wg.Add(2)
	go newPumpsClient.watchStatus()
	go newPumpsClient.heartbeat()

	return newPumpsClient, nil
}

// getPumpStatus retruns all the pumps status in the etcd.
func (c *PumpsClient) getPumpStatus(pctx context.Context) error {
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
		c.addPump(status)
	}

	return nil
}

// WriteBinlog writes binlog to a situable pump.
func (c *PumpsClient) WriteBinlog(clusterID uint64, binlog *pb.Binlog) error {
	pump := c.Selector.Select(binlog)
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

		resp, err := c.writeBinlog(req, pump)
		if err == nil && resp.Errmsg != "" {
			err = errors.New(resp.Errmsg)
		}
		if err == nil {
			return nil
		}

		log.Errorf("write binlog error %v", err)
		if isCriticalError(err) {
			return err
		}

		// every pump can retry 5 times, if retry 5 times and still failed, set this pump unavaliable, and choose a new pump.
		if (i+1)%5 == 0 {
			c.Lock()
			c.setPumpAvaliable(pump, false)
			c.Unlock()
			pump = c.Selector.Next(pump, binlog, i/5+1)
			log.Infof("avaliable pumps: %v, write binlog choose pump %v", c.AvaliablePumps, pump)
		}
		time.Sleep(time.Second)
	}

	return errors.New("write binlog failed")
}

func (c *PumpsClient) writeBinlog(req *pb.WriteBinlogReq, pump *PumpStatus) (*pb.WriteBinlogResp, error) {
	ctx, cancel := context.WithTimeout(context.Background(), c.BinlogWriteTimeout)
	resp, err := pump.Client.WriteBinlog(ctx, req)
	cancel()

	return resp, err
}

// setPumpAvaliable set pump's isAvaliable, and modify NeedCheckPumps or AvaliablePumps.
func (c *PumpsClient) setPumpAvaliable(pump *PumpStatus, avaliable bool) {
	pump.IsAvaliable = avaliable
	if avaliable {
		err := pump.createGrpcClient()
		if err != nil {
			log.Errorf("create grpc client fot pump %s failed, error: %v", pump.NodeID, err)
			pump.IsAvaliable = false
			return
		}

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
func (c *PumpsClient) addPump(status *node.Status) {
	pump := NewPumpStatus(status)
	if status.State == node.Online {
		c.AvaliablePumps = append(c.AvaliablePumps, pump)
	} else {
		c.NeedCheckPumps = append(c.NeedCheckPumps, pump)
	}

	c.PumpMap[pump.NodeID] = pump
	c.Pumps = append(c.Pumps, pump)
}

// removePump removes a pump.
func (c *PumpsClient) removePump(nodeID string) {
	if _, ok := c.PumpMap[nodeID]; ok {
		delete(c.PumpMap, nodeID)
	}

	for i, p := range c.NeedCheckPumps {
		if p.NodeID == nodeID {
			c.NeedCheckPumps = append(c.NeedCheckPumps[:i], c.NeedCheckPumps[i+1:]...)
			break
		}
	}

	for j, p := range c.AvaliablePumps {
		if p.NodeID == nodeID {
			c.AvaliablePumps = append(c.AvaliablePumps[:j], c.AvaliablePumps[j+1:]...)
			break
		}
	}

	for k, p := range c.Pumps {
		if p.NodeID == nodeID {
			c.Pumps = append(c.Pumps[:k], c.Pumps[k+1:]...)
			break
		}
	}
}

// exist returns true if pumps client has pump matched this nodeID.
func (c *PumpsClient) exist(nodeID string) bool {
	_, ok := c.PumpMap[nodeID]
	return ok
}

// watchStatus watchs pump's status in etcd.
func (c *PumpsClient) watchStatus() {
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
				status := &node.Status{}
				err := json.Unmarshal(ev.Kv.Value, &status)
				if err != nil {
					log.Errorf("unmarshal status %q failed", ev.Kv.Value)
					continue
				}

				switch ev.Type {
				case mvccpb.PUT:
					// only need handle PUT event for `alive` node.
					if strings.Contains(string(ev.Kv.Key), aliveStr) {
						if !c.exist(status.NodeID) {
							c.addPump(status)
						}

						// judge pump's status is changed or not.
						if c.PumpMap[status.NodeID].statusChanged(status) {
							c.Lock()
							c.PumpMap[status.NodeID].updateStatus(status)
							if status.State != node.Online {
								c.setPumpAvaliable(c.PumpMap[status.NodeID], false)
							}
							c.Unlock()
						}
					}
				case mvccpb.DELETE:
					nodeID, nodeTp := node.AnalyzeKey(string(ev.Kv.Key))
					if nodeTp == objectStr {
						// object node is deleted, means this node is unregister, and can remove this pump.
						c.Lock()
						c.removePump(nodeID)
						c.Unlock()
					} else if nodeTp == aliveStr {
						// this pump is not alive, and we don't know the pump's state.
						// set this pump's state to unknow, and this pump is not avaliable.
						c.Lock()
						if pumpStatus, ok := c.PumpMap[nodeID]; ok {
							pumpStatus.State = node.Unknow
							c.setPumpAvaliable(pumpStatus, false)
						}
						c.Unlock()
					} else {
						log.Warnf("get unknow key %s from etcd", string(ev.Kv.Key))
					}
				}
			}
		}
	}
}

// heartbeat send heartbeat request to NeedCheckPumps,
// if pump can return response, remove it from NeedCheckPumps.
func (c *PumpsClient) heartbeat() {
	defer c.wg.Done()
	for {
		select {
		case <-c.ctx.Done():
			log.Infof("pump client heartbeat finished")
			return
		default:
			// send fake binlog to pump with wrong clusterID,
			// if this pump can return `mismatch` error, means this pump is avaliable.
			checkPassPumps := make([]*PumpStatus, 0, 1)
			req := &pb.WriteBinlogReq{ClusterID: fakeClusterID, Payload: nil}
			c.RLock()
			for _, pump := range c.NeedCheckPumps {
				if pump.Status.State != node.Online && pump.Status.State != node.Unknow {
					continue
				}

				_, err := c.writeBinlog(req, pump)
				if strings.Contains(err.Error(), "cluster ID are mismatch") {
					checkPassPumps = append(checkPassPumps, pump)
				}
			}
			c.RUnlock()

			c.Lock()
			for _, pump := range checkPassPumps {
				c.setPumpAvaliable(pump, true)
			}
			c.Unlock()

			time.Sleep(DefaultHeartbeatWaitTime)
		}
	}
}

// Close closes the PumpsClient.
func (c *PumpsClient) Close() {
	log.Infof("pumps client is closing")
	c.cancel()
	c.wg.Wait()
	log.Infof("pumps client is closed")
}

func isCriticalError(err error) bool {
	// TODO: add some critical error.
	return false
}
