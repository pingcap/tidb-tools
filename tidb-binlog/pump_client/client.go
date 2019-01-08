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
	"encoding/json"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/coreos/etcd/mvcc/mvccpb"
	"github.com/pingcap/errors"
	pd "github.com/pingcap/pd/client"
	"github.com/pingcap/tidb-tools/pkg/etcd"
	"github.com/pingcap/tidb-tools/pkg/utils"
	"github.com/pingcap/tidb-tools/tidb-binlog/node"
	pb "github.com/pingcap/tipb/go-binlog"
	log "github.com/sirupsen/logrus"
)

const (
	// DefaultEtcdTimeout is the default timeout config for etcd.
	DefaultEtcdTimeout = 5 * time.Second

	// DefaultRetryTime is the default retry time for each pump.
	DefaultRetryTime = 10

	// DefaultBinlogWriteTimeout is the default max time binlog can use to write to pump.
	DefaultBinlogWriteTimeout = 15 * time.Second

	// CheckInterval is the default interval for check unavaliable pumps.
	CheckInterval = 30 * time.Second
)

var (
	// Logger is ...
	Logger = log.New()

	// ErrNoAvaliablePump means no avaliable pump to write binlog.
	ErrNoAvaliablePump = errors.New("no avaliable pump to write binlog")

	// CommitBinlogTimeout is the max retry duration time for write commit/rollback binlog.
	CommitBinlogTimeout = 10 * time.Minute

	// RetryInterval is the interval of retrying to write binlog.
	RetryInterval = 100 * time.Millisecond
)

// PumpInfos saves pumps' infomations in pumps client.
type PumpInfos struct {
	sync.RWMutex
	// Pumps saves the map of pump's nodeID and pump status.
	Pumps map[string]*PumpStatus

	// AvliablePumps saves the whole avaliable pumps' status.
	AvaliablePumps map[string]*PumpStatus

	// UnAvaliablePumps saves the unAvaliable pumps.
	// And only pump with Online state in this map need check is it avaliable.
	UnAvaliablePumps map[string]*PumpStatus

	// ErrNums saves the error's num for every pump.
	ErrNums map[string]int64
}

// NewPumpInfos returns a PumpInfos.
func NewPumpInfos() *PumpInfos {
	return &PumpInfos{
		Pumps:            make(map[string]*PumpStatus),
		AvaliablePumps:   make(map[string]*PumpStatus),
		UnAvaliablePumps: make(map[string]*PumpStatus),
		ErrNums:          make(map[string]int64),
	}
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

	// Security is the security config
	Security *tls.Config

	// binlog socket file path, for compatible with kafka version pump.
	binlogSocket string
}

// NewPumpsClient returns a PumpsClient.
// TODO: get strategy from etcd, and can update strategy in real-time. Use Range as default now.
func NewPumpsClient(etcdURLs string, timeout time.Duration, securityOpt pd.SecurityOption) (*PumpsClient, error) {
	ectdEndpoints, err := utils.ParseHostPortAddr(etcdURLs)
	if err != nil {
		return nil, errors.Trace(err)
	}

	// get clusterid
	pdCli, err := pd.NewClient(ectdEndpoints, securityOpt)
	if err != nil {
		return nil, errors.Trace(err)
	}
	clusterID := pdCli.GetClusterID(context.Background())
	pdCli.Close()

	security, err := utils.ToTLSConfig(securityOpt.CAPath, securityOpt.CertPath, securityOpt.KeyPath)
	if err != nil {
		return nil, errors.Trace(err)
	}

	cli, err := etcd.NewClientFromCfg(ectdEndpoints, DefaultEtcdTimeout, node.DefaultRootPath, security)
	if err != nil {
		return nil, errors.Trace(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	newPumpsClient := &PumpsClient{
		ctx:                ctx,
		cancel:             cancel,
		ClusterID:          clusterID,
		EtcdRegistry:       node.NewEtcdRegistry(cli, DefaultEtcdTimeout),
		Pumps:              NewPumpInfos(),
		Selector:           NewSelector(Range),
		BinlogWriteTimeout: timeout,
		Security:           security,
	}

	revision, err := newPumpsClient.getPumpStatus(ctx)
	if err != nil {
		return nil, errors.Trace(err)
	}

	if len(newPumpsClient.Pumps.Pumps) == 0 {
		return nil, errors.New("no pump found in pd")
	}
	newPumpsClient.Selector.SetPumps(copyPumps(newPumpsClient.Pumps.AvaliablePumps))

	newPumpsClient.wg.Add(2)
	go newPumpsClient.watchStatus(revision)
	go newPumpsClient.detect()

	return newPumpsClient, nil
}

// NewLocalPumpsClient returns a PumpsClient, this PumpsClient will write binlog by socket file. For compatible with kafka version pump.
func NewLocalPumpsClient(etcdURLs, binlogSocket string, timeout time.Duration, securityOpt pd.SecurityOption) (*PumpsClient, error) {
	ectdEndpoints, err := utils.ParseHostPortAddr(etcdURLs)
	if err != nil {
		return nil, errors.Trace(err)
	}

	// get clusterid
	pdCli, err := pd.NewClient(ectdEndpoints, securityOpt)
	if err != nil {
		return nil, errors.Trace(err)
	}
	clusterID := pdCli.GetClusterID(context.Background())
	pdCli.Close()

	security, err := utils.ToTLSConfig(securityOpt.CAPath, securityOpt.CertPath, securityOpt.KeyPath)
	if err != nil {
		return nil, errors.Trace(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	newPumpsClient := &PumpsClient{
		ctx:                ctx,
		cancel:             cancel,
		ClusterID:          clusterID,
		Pumps:              NewPumpInfos(),
		Selector:           NewSelector(LocalUnix),
		BinlogWriteTimeout: timeout,
		Security:           security,
		binlogSocket:       binlogSocket,
	}
	newPumpsClient.getLocalPumpStatus(ctx)

	return newPumpsClient, nil
}

// getLocalPumpStatus gets the local pump. For compatible with kafka version tidb-binlog.
func (c *PumpsClient) getLocalPumpStatus(pctx context.Context) {
	nodeStatus := &node.Status{
		NodeID:  localPump,
		Addr:    c.binlogSocket,
		IsAlive: true,
		State:   node.Online,
	}
	c.addPump(NewPumpStatus(nodeStatus, c.Security), true)
}

// getPumpStatus gets all the pumps status in the etcd.
func (c *PumpsClient) getPumpStatus(pctx context.Context) (revision int64, err error) {
	nodesStatus, revision, err := c.EtcdRegistry.Nodes(pctx, node.NodePrefix[node.PumpNode])
	if err != nil {
		return -1, errors.Trace(err)
	}

	for _, status := range nodesStatus {
		Logger.Debugf("[pumps client] get pump %v from etcd", status)
		c.addPump(NewPumpStatus(status, c.Security), false)
	}

	return revision, nil
}

// WriteBinlog writes binlog to a situable pump.
func (c *PumpsClient) WriteBinlog(binlog *pb.Binlog) error {
	errNums := make(map[*PumpStatus]int64)
	var successPump *PumpStatus
	defer func() {
		if len(errNums) == 0 {
			return
		}

		for pump, errNum := range errNums {
			c.addErrNum(pump.NodeID, errNum)
		}
		if successPump != nil {
			c.clearErrNum(successPump.NodeID)
		}
		c.checkErr()
	}()

	commitData, err := binlog.Marshal()
	if err != nil {
		return errors.Trace(err)
	}
	req := &pb.WriteBinlogReq{ClusterID: c.ClusterID, Payload: commitData}

	retryTime := 0
	var pump *PumpStatus
	var resp *pb.WriteBinlogResp
	startTime := time.Now()
	for {
		if pump == nil || binlog.Tp == pb.BinlogType_Prewrite {
			pump = c.Selector.Select(binlog, retryTime)
		}
		if pump == nil {
			err = ErrNoAvaliablePump
			break
		}

		resp, err = pump.WriteBinlog(req, c.BinlogWriteTimeout)
		if err == nil && resp.Errmsg != "" {
			err = errors.New(resp.Errmsg)
		}
		if err == nil {
			successPump = pump
			return nil
		}
		errNums[pump]++
		Logger.Warnf("[pumps client] write binlog to pump %s (type: %s, start ts: %d, commit ts: %d, length: %d) error %v", pump.NodeID, binlog.Tp, binlog.StartTs, binlog.CommitTs, len(commitData), err)

		if binlog.Tp != pb.BinlogType_Prewrite {
			// only use one pump to write commit/rollback binlog, util write success or blocked for ten minutes. And will not return error to tidb.
			if time.Since(startTime) > CommitBinlogTimeout {
				break
			}
		} else {
			if !isRetryableError(err) {
				// this kind of error is not retryable, return directly.
				return err
			}

			if time.Since(startTime) > c.BinlogWriteTimeout {
				break
			}

			retryTime++
		}

		if binlog.Tp != pb.BinlogType_Prewrite {
			time.Sleep(RetryInterval * 10)
		} else {
			time.Sleep(RetryInterval)
		}
	}

	pump, err1 := c.backoffWriteBinlog(req, binlog.Tp)
	if err1 == nil {
		successPump = pump
		return nil
	}

	return errors.Errorf("write binlog failed, the last error %v", err)
}

func (c *PumpsClient) backoffWriteBinlog(req *pb.WriteBinlogReq, binlogType pb.BinlogType) (pump *PumpStatus, err error) {
	if binlogType != pb.BinlogType_Prewrite {
		// never return error for commit/rollback binlog.
		return nil, nil
	}

	unAvaliablePumps := make([]*PumpStatus, 0, 3)
	c.Pumps.RLock()
	for _, pump := range c.Pumps.UnAvaliablePumps {
		unAvaliablePumps = append(unAvaliablePumps, pump)
	}
	c.Pumps.RUnlock()

	var resp *pb.WriteBinlogResp
	// send binlog to unavaliable pumps to retry again.
	for _, pump := range unAvaliablePumps {
		resp, err = pump.WriteBinlog(req, c.BinlogWriteTimeout)
		if err == nil {
			if resp.Errmsg != "" {
				err = errors.New(resp.Errmsg)
			} else {
				// if this pump can write binlog success, set this pump to avaliable.
				Logger.Debugf("[pumps client] write binlog to unavaliable pump %s success, set this pump to avaliable", pump.NodeID)
				c.setPumpAvaliable(pump, true)
				return pump, nil
			}
		}
	}

	return nil, err
}

func (c *PumpsClient) addErrNum(nodeID string, num int64) {
	c.Pumps.Lock()
	c.Pumps.ErrNums[nodeID] += num
	c.Pumps.Unlock()
}

func (c *PumpsClient) clearErrNum(nodeID string) {
	c.Pumps.RLock()
	if _, ok := c.Pumps.ErrNums[nodeID]; !ok {
		c.Pumps.RUnlock()
		return
	}
	c.Pumps.RUnlock()

	c.Pumps.Lock()
	delete(c.Pumps.ErrNums, nodeID)
	c.Pumps.Unlock()
}

// checkErr checks the error num for each pump, if this num is greater than DefaultRetryTime, set this pump to unavaliable.
func (c *PumpsClient) checkErr() {
	unAvaliablePumps := make([]*PumpStatus, 0, 3)

	c.Pumps.RLock()
	for nodeID, num := range c.Pumps.ErrNums {
		if num >= DefaultRetryTime {
			if pump, ok := c.Pumps.Pumps[nodeID]; ok {
				unAvaliablePumps = append(unAvaliablePumps, pump)
			}
		}
	}
	c.Pumps.RUnlock()

	for _, pump := range unAvaliablePumps {
		c.setPumpAvaliable(pump, false)
	}

}

// setPumpAvaliable set pump's isAvaliable, and modify UnAvaliablePumps or AvaliablePumps.
func (c *PumpsClient) setPumpAvaliable(pump *PumpStatus, avaliable bool) {
	c.Pumps.Lock()
	defer c.Pumps.Unlock()

	pump.IsAvaliable = avaliable
	delete(c.Pumps.ErrNums, pump.NodeID)
	pump.ResetGrpcClient()

	if pump.IsAvaliable {
		delete(c.Pumps.UnAvaliablePumps, pump.NodeID)
		if _, ok := c.Pumps.Pumps[pump.NodeID]; ok {
			c.Pumps.AvaliablePumps[pump.NodeID] = pump
		}

	} else {
		delete(c.Pumps.AvaliablePumps, pump.NodeID)
		if _, ok := c.Pumps.Pumps[pump.NodeID]; ok {
			c.Pumps.UnAvaliablePumps[pump.NodeID] = pump
		}
	}

	c.Selector.SetPumps(copyPumps(c.Pumps.AvaliablePumps))
}

// addPump add a new pump.
func (c *PumpsClient) addPump(pump *PumpStatus, updateSelector bool) {
	c.Pumps.Lock()

	if pump.State == node.Online {
		c.Pumps.AvaliablePumps[pump.NodeID] = pump
	} else {
		c.Pumps.UnAvaliablePumps[pump.NodeID] = pump
	}
	c.Pumps.Pumps[pump.NodeID] = pump

	if updateSelector {
		c.Selector.SetPumps(copyPumps(c.Pumps.AvaliablePumps))
	}

	c.Pumps.Unlock()
}

// updatePump update pump's status, and return whether pump's IsAvaliable should be changed.
func (c *PumpsClient) updatePump(status *node.Status) (pump *PumpStatus, avaliableChanged, avaliable bool) {
	var ok bool
	c.Pumps.Lock()
	if pump, ok = c.Pumps.Pumps[status.NodeID]; ok {
		if pump.Status.State != status.State {
			if status.State == node.Online {
				avaliableChanged = true
				avaliable = true
			} else if pump.Status.State == node.Online {
				avaliableChanged = true
				avaliable = false
			}
		}
		pump.Status = *status
	}
	c.Pumps.Unlock()

	return
}

// removePump removes a pump, used when pump is offline.
func (c *PumpsClient) removePump(nodeID string) {
	c.Pumps.Lock()
	if pump, ok := c.Pumps.Pumps[nodeID]; ok {
		pump.ResetGrpcClient()
	}
	delete(c.Pumps.Pumps, nodeID)
	delete(c.Pumps.UnAvaliablePumps, nodeID)
	delete(c.Pumps.AvaliablePumps, nodeID)
	c.Selector.SetPumps(copyPumps(c.Pumps.AvaliablePumps))
	c.Pumps.Unlock()
}

// exist returns true if pumps client has pump matched this nodeID.
func (c *PumpsClient) exist(nodeID string) bool {
	c.Pumps.RLock()
	_, ok := c.Pumps.Pumps[nodeID]
	c.Pumps.RUnlock()
	return ok
}

// watchStatus watchs pump's status in etcd.
func (c *PumpsClient) watchStatus(revision int64) {
	defer c.wg.Done()
	rootPath := path.Join(node.DefaultRootPath, node.NodePrefix[node.PumpNode])
	rch := c.EtcdRegistry.WatchNode(c.ctx, rootPath, revision)
	for {
		select {
		case <-c.ctx.Done():
			Logger.Info("[pumps client] watch status finished")
			return
		case wresp := <-rch:
			err := wresp.Err()
			if err != nil {
				// meet error, some event may missed, get all the pump's information from etcd again.
				Logger.Warnf("[pumps client] watch status meet error %v", err)
				revision, err = c.getPumpStatus(c.ctx)
				if err == nil {
					c.Pumps.Lock()
					c.Selector.SetPumps(copyPumps(c.Pumps.AvaliablePumps))
					c.Pumps.Unlock()
					rch = c.EtcdRegistry.WatchNode(c.ctx, rootPath, revision)
				} else {
					Logger.Warnf("[pumps client] get pumps from pd failed, error: %v", errors.Trace(err))
				}

				continue
			}

			for _, ev := range wresp.Events {
				status := &node.Status{}
				err := json.Unmarshal(ev.Kv.Value, &status)
				if err != nil {
					Logger.Errorf("[pumps client] unmarshal pump status %q failed", ev.Kv.Value)
					continue
				}

				switch ev.Type {
				case mvccpb.PUT:
					if !c.exist(status.NodeID) {
						Logger.Infof("[pumps client] find a new pump %s", status.NodeID)
						c.addPump(NewPumpStatus(status, c.Security), true)
						continue
					}

					pump, avaliableChanged, avaliable := c.updatePump(status)
					if avaliableChanged {
						Logger.Infof("[pumps client] pump %s's state is changed to %s", pump.Status.NodeID, status.State)
						c.setPumpAvaliable(pump, avaliable)
					}

				case mvccpb.DELETE:
					// now will not delete pump node in fact, just for compatibility.
					nodeID := node.AnalyzeNodeID(string(ev.Kv.Key))
					Logger.Infof("[pumps client] remove pump %s", nodeID)
					c.removePump(nodeID)
				}
			}
		}
	}
}

// detect send detect binlog to pumps with online state in UnAvaliablePumps,
func (c *PumpsClient) detect() {
	defer c.wg.Done()
	for {
		select {
		case <-c.ctx.Done():
			Logger.Infof("[pumps client] heartbeat finished")
			return
		default:
			// send detect binlog to pump, if this pump can return response without error
			// means this pump is avaliable.
			needCheckPumps := make([]*PumpStatus, 0, len(c.Pumps.UnAvaliablePumps))
			checkPassPumps := make([]*PumpStatus, 0, 1)
			req := &pb.WriteBinlogReq{ClusterID: c.ClusterID, Payload: nil}
			c.Pumps.RLock()
			for _, pump := range c.Pumps.UnAvaliablePumps {
				if pump.Status.State == node.Online {
					needCheckPumps = append(needCheckPumps, pump)
				}
			}
			c.Pumps.RUnlock()

			for _, pump := range needCheckPumps {
				_, err := pump.WriteBinlog(req, c.BinlogWriteTimeout)
				if err == nil {
					Logger.Debugf("[pumps client] write detect binlog to unavaliable pump %s success", pump.NodeID)
					checkPassPumps = append(checkPassPumps, pump)
				} else {
					Logger.Debugf("[pumps client] write detect binlog to pump %s error %v", pump.NodeID, err)
				}
			}

			for _, pump := range checkPassPumps {
				c.setPumpAvaliable(pump, true)
			}

			time.Sleep(CheckInterval)
		}
	}
}

// Close closes the PumpsClient.
func (c *PumpsClient) Close() {
	Logger.Infof("[pumps client] is closing")
	c.cancel()
	c.wg.Wait()
	Logger.Infof("[pumps client] is closed")
}

func isRetryableError(err error) bool {
	// ResourceExhausted is a error code in grpc.
	// ResourceExhausted indicates some resource has been exhausted, perhaps
	// a per-user quota, or perhaps the entire file system is out of space.
	// https://github.com/grpc/grpc-go/blob/9cc4fdbde2304827ffdbc7896f49db40c5536600/codes/codes.go#L76
	if strings.Contains(err.Error(), "ResourceExhausted") {
		return false
	}

	return true
}

func copyPumps(pumps map[string]*PumpStatus) []*PumpStatus {
	ps := make([]*PumpStatus, 0, len(pumps))
	for _, pump := range pumps {
		ps = append(ps, pump)
	}

	return ps
}
