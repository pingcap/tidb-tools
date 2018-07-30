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
	"sync"
	"time"

	"github.com/coreos/etcd/mvcc/mvccpb"
	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb-tools/pkg/etcd"
	"github.com/pingcap/tidb-tools/pkg/utils"
	"github.com/pingcap/tidb-tools/tidb-binlog/node"
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

	// CheckInterval is the default interval for check unavaliable pumps.
	CheckInterval = 30 * time.Second

	// RetryInterval is the default interval of retrying to write binlog.
	RetryInterval = 100 * time.Millisecond
)

var (
	// ErrNoAvaliablePump means no avaliable pump to write binlog.
	ErrNoAvaliablePump = errors.New("no avaliable pump to write binlog")

	// ErrWriteBinlog means write binlog failed, and reach the max retry time.
	ErrWriteBinlog = errors.New("write binlog failed")
)

// PumpInfos saves pumps' infomations in pumps client.
type PumpInfos struct {
	sync.RWMutex
	// Pumps saves the map of pump's nodeID and pump status.
	Pumps map[string]*PumpStatus

	// AvliablePumps saves the whole avaliable pumps' status.
	AvaliablePumps map[string]*PumpStatus

	// NeedCheckPumps saves the pumps need to be checked.
	NeedCheckPumps map[string]*PumpStatus
}

// PumpsClient is the client of pumps.
type PumpsClient struct {
	ctx context.Context

	cancel context.CancelFunc

	wg sync.WaitGroup

	// ClusterID is the cluster ID of this tidb cluster.
	ClusterID uint64

	// the registry of etcd.
	EtcdRegistry *node.EtcdRegistry

	// Pumps saves the pumps' information.
	Pumps *PumpInfos

	// Selector will select a suitable pump.
	Selector PumpSelector

	// the max retry time if write binlog failed.
	RetryTime int

	// BinlogWriteTimeout is the max time binlog can use to write to pump.
	BinlogWriteTimeout time.Duration
}

// NewPumpsClient returns a PumpsClient.
func NewPumpsClient(etcdURLs string, clusterID uint64, security *tls.Config, algorithm string) (*PumpsClient, error) {
	selector := NewSelector(algorithm)

	ectdEndpoints, err := utils.ParseHostPortAddr(etcdURLs)
	if err != nil {
		return nil, errors.Trace(err)
	}

	rootPath := path.Join(node.DefaultRootPath, node.NodePrefix[node.PumpNode])
	cli, err := etcd.NewClientFromCfg(ectdEndpoints, DefaultEtcdTimeout, rootPath, security)
	if err != nil {
		return nil, errors.Trace(err)
	}

	pumpInfos := &PumpInfos{
		Pumps:          make(map[string]*PumpStatus),
		AvaliablePumps: make(map[string]*PumpStatus),
		NeedCheckPumps: make(map[string]*PumpStatus),
	}

	ctx, cancel := context.WithCancel(context.Background())
	newPumpsClient := &PumpsClient{
		ctx:                ctx,
		cancel:             cancel,
		ClusterID:          clusterID,
		EtcdRegistry:       node.NewEtcdRegistry(cli, DefaultEtcdTimeout),
		Pumps:              pumpInfos,
		Selector:           selector,
		RetryTime:          DefaultRetryTime,
		BinlogWriteTimeout: DefaultBinlogWriteTimeout,
	}

	err = newPumpsClient.getPumpStatus(ctx)
	if err != nil {
		return nil, errors.Trace(err)
	}
	newPumpsClient.Selector.SetPumps(copyPumps(newPumpsClient.Pumps.AvaliablePumps))

	newPumpsClient.wg.Add(2)
	go newPumpsClient.watchStatus()
	go newPumpsClient.detect()

	return newPumpsClient, nil
}

// getPumpStatus retruns all the pumps status in the etcd.
func (c *PumpsClient) getPumpStatus(pctx context.Context) error {
	nodesStatus, err := c.EtcdRegistry.Nodes(pctx, node.DefaultRootPath)
	if err != nil {
		return errors.Trace(err)
	}

	for _, status := range nodesStatus {
		log.Debugf("[pumps client] get pump %v from etcd", status)
		c.Pumps.Lock()
		c.addPump(NewPumpStatus(status), false)
		c.Pumps.Unlock()
	}

	return nil
}

// WriteBinlog writes binlog to a situable pump.
func (c *PumpsClient) WriteBinlog(binlog *pb.Binlog) error {
	pump := c.Selector.Select(binlog)
	if pump == nil {
		return ErrNoAvaliablePump
	}
	log.Debugf("[pumps client] write binlog choose pump %s", pump.NodeID)

	commitData, err := binlog.Marshal()
	if err != nil {
		return errors.Trace(err)
	}
	req := &pb.WriteBinlogReq{ClusterID: c.ClusterID, Payload: commitData}

	// Retry many times because we may raise CRITICAL error here.
	for i := 0; i < c.RetryTime; i++ {
		if pump == nil {
			return ErrNoAvaliablePump
		}

		resp, err := pump.writeBinlog(req, c.BinlogWriteTimeout)
		if err == nil && resp.Errmsg != "" {
			err = errors.New(resp.Errmsg)
		}
		if err == nil {
			return nil
		}

		log.Errorf("[pumps client] write binlog error %v", err)
		if isCriticalError(err) {
			return err
		}

		// every pump can retry 5 times, if retry 5 times and still failed, set this pump unavaliable, and choose a new pump.
		if (i+1)%5 == 0 {
			c.Pumps.Lock()
			c.setPumpAvaliable(pump, false)
			c.Pumps.Unlock()
			pump = c.Selector.Next(pump, binlog, i/5+1)
			log.Debugf("[pumps client] avaliable pumps: %v, write binlog choose pump %v", c.Pumps.AvaliablePumps, pump)
		}
		time.Sleep(RetryInterval)
	}

	return ErrWriteBinlog
}

// setPumpAvaliable set pump's isAvaliable, and modify NeedCheckPumps or AvaliablePumps.
func (c *PumpsClient) setPumpAvaliable(pump *PumpStatus, avaliable bool) {
	pump.IsAvaliable = avaliable
	if avaliable {
		err := pump.createGrpcClient()
		if err != nil {
			log.Errorf("[pumps client] create grpc client for pump %s failed, error: %v", pump.NodeID, err)
			pump.IsAvaliable = false
			return
		}

		delete(c.Pumps.NeedCheckPumps, pump.NodeID)
		c.Pumps.AvaliablePumps[pump.NodeID] = pump

	} else {
		delete(c.Pumps.AvaliablePumps, pump.NodeID)
		c.Pumps.NeedCheckPumps[pump.NodeID] = pump
	}

	c.Selector.SetPumps(copyPumps(c.Pumps.AvaliablePumps))
}

// addPump add a new pump.
func (c *PumpsClient) addPump(pump *PumpStatus, updateSelector bool) {
	if pump.State == node.Online {
		c.Pumps.AvaliablePumps[pump.NodeID] = pump
	} else {
		c.Pumps.NeedCheckPumps[pump.NodeID] = pump
	}
	c.Pumps.Pumps[pump.NodeID] = pump

	if updateSelector {
		c.Selector.SetPumps(copyPumps(c.Pumps.AvaliablePumps))
	}
}

// updatePump update pump's status.
func (c *PumpsClient) updatePump(status *node.Status) {
	if _, ok := c.Pumps.Pumps[status.NodeID]; ok {
		c.Pumps.Pumps[status.NodeID].Status = *status
	}
}

// removePump removes a pump.
func (c *PumpsClient) removePump(nodeID string) {
	delete(c.Pumps.Pumps, nodeID)
	delete(c.Pumps.NeedCheckPumps, nodeID)
	delete(c.Pumps.AvaliablePumps, nodeID)

	c.Selector.SetPumps(copyPumps(c.Pumps.AvaliablePumps))
}

// exist returns true if pumps client has pump matched this nodeID.
func (c *PumpsClient) exist(nodeID string) bool {
	_, ok := c.Pumps.Pumps[nodeID]
	return ok
}

// watchStatus watchs pump's status in etcd.
func (c *PumpsClient) watchStatus() {
	defer c.wg.Done()
	rootPath := path.Join(node.DefaultRootPath, node.NodePrefix[node.PumpNode])
	rch := c.EtcdRegistry.WatchNode(c.ctx, rootPath)
	for {
		select {
		case <-c.ctx.Done():
			log.Info("[pumps client] watch status finished")
			return
		case wresp := <-rch:
			for _, ev := range wresp.Events {
				log.Debugf("[pumps client] watch etcd event type:%s, key: %q, value: %q", ev.Type, ev.Kv.Key, ev.Kv.Value)
				status := &node.Status{}
				err := json.Unmarshal(ev.Kv.Value, &status)
				if err != nil {
					log.Errorf("[pumps client] unmarshal pump status %q failed", ev.Kv.Value)
					continue
				}

				switch ev.Type {
				case mvccpb.PUT:
					if !c.exist(status.NodeID) {
						c.Pumps.Lock()
						c.addPump(NewPumpStatus(status), true)
						c.Pumps.Unlock()
						continue
					}

					c.Pumps.Lock()
					c.updatePump(status)
					c.Pumps.Unlock()

				case mvccpb.DELETE:
					// now will not delete pump node in fact, just for compatibility。
					nodeID := node.AnalyzeNodeID(string(ev.Kv.Key))
					c.Pumps.Lock()
					c.removePump(nodeID)
					c.Pumps.Unlock()
				}
			}
		}
	}
}

// detect send detect binlog to NeedCheckPumps,
// if pump can return response, remove it from NeedCheckPumps.
func (c *PumpsClient) detect() {
	defer c.wg.Done()
	for {
		select {
		case <-c.ctx.Done():
			log.Infof("[pumps client] heartbeat finished")
			return
		default:
			// send fake binlog to pump, if this pump can return response without error
			// means this pump is avaliable.
			needCheckPumps := make([]*PumpStatus, 0, len(c.Pumps.NeedCheckPumps))
			checkPassPumps := make([]*PumpStatus, 0, 1)
			req := &pb.WriteBinlogReq{ClusterID: c.ClusterID, Payload: nil}
			c.Pumps.RLock()
			for _, pump := range c.Pumps.NeedCheckPumps {
				needCheckPumps = append(needCheckPumps, pump)
			}
			c.Pumps.RUnlock()

			for _, pump := range needCheckPumps {
				if pump.Status.State != node.Online && !pump.Status.IsAlive {
					continue
				}

				err := pump.createGrpcClient()
				if err != nil {
					log.Errorf("[pumps client] create grpc client for pump %s failed, error %v", pump.NodeID, errors.Trace(err))
					continue
				}
				if pump.Client == nil {
					continue
				}

				_, err = pump.writeBinlog(req, c.BinlogWriteTimeout)
				if err == nil {
					checkPassPumps = append(checkPassPumps, pump)
				} else {
					log.Errorf("[pumps client] write detect binlog to pump %s error %v", pump.NodeID, err)
				}
			}

			for _, pump := range checkPassPumps {
				c.Pumps.Lock()
				c.setPumpAvaliable(pump, true)
				c.Pumps.Unlock()
			}

			time.Sleep(CheckInterval)
		}
	}
}

// Close closes the PumpsClient.
func (c *PumpsClient) Close() {
	log.Infof("[pumps client] is closing")
	c.cancel()
	c.wg.Wait()
	log.Infof("[pumps client] is closed")
}

func isCriticalError(err error) bool {
	// TODO: add some critical error.
	return false
}

func copyPumps(pumps map[string]*PumpStatus) []*PumpStatus {
	ps := make([]*PumpStatus, 0, len(pumps))
	for _, pump := range pumps {
		ps = append(ps, pump)
	}

	return ps
}
