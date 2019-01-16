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
	"github.com/pingcap/tidb/util/logutil"
	pb "github.com/pingcap/tipb/go-binlog"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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
	// Logger is ..., obsolete now.
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
}

// NewPumpInfos returns a PumpInfos.
func NewPumpInfos() *PumpInfos {
	return &PumpInfos{
		Pumps:            make(map[string]*PumpStatus),
		AvaliablePumps:   make(map[string]*PumpStatus),
		UnAvaliablePumps: make(map[string]*PumpStatus),
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

	// the max retry time if write binlog failed, obsolete now.
	RetryTime int

	// BinlogWriteTimeout is the max time binlog can use to write to pump.
	BinlogWriteTimeout time.Duration

	// Security is the security config
	Security *tls.Config

	// binlog socket file path, for compatible with kafka version pump.
	binlogSocket string

	nodePath string
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
		nodePath:           path.Join(node.DefaultRootPath, node.NodePrefix[node.PumpNode]),
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
		log.Debugf("[pumps client] get pump %v from pd", status)
		c.addPump(NewPumpStatus(status, c.Security), false)
	}

	return revision, nil
}

// WriteBinlog writes binlog to a situable pump. Tips: will never return error for commit/rollback binlog.
func (c *PumpsClient) WriteBinlog(binlog *pb.Binlog) error {
	var choosePump *PumpStatus
	meetError := false
	defer func() {
		if meetError {
			c.checkPumpAvaliable()
		}

		c.Selector.Feedback(binlog.StartTs, binlog.Tp, choosePump)
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

	c.Pumps.RLock()
	pumpNum := len(c.Pumps.AvaliablePumps)
	c.Pumps.RUnlock()

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
			choosePump = pump
			return nil
		}

		meetError = true
		log.Warnf("[pumps client] write binlog to pump %s (type: %s, start ts: %d, commit ts: %d, length: %d) error %v", pump.NodeID, binlog.Tp, binlog.StartTs, binlog.CommitTs, len(commitData), err)

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

			// make sure already retry every avaliable pump.
			if time.Since(startTime) > c.BinlogWriteTimeout && retryTime > pumpNum {
				break
			}

			if isConnUnAvliable(err) {
				// this kind of error indicate that the grpc connection is not avaliable, may be create the connection again can write success.
				pump.ResetGrpcClient()
			}

			retryTime++
		}

		if binlog.Tp != pb.BinlogType_Prewrite {
			time.Sleep(RetryInterval * 10)
		} else {
			time.Sleep(RetryInterval)
		}
	}

	log.Info("[pumps client] write binlog to avaliable pumps all failed, will try unavaliable pumps")
	pump, err1 := c.backoffWriteBinlog(req, binlog.Tp, binlog.StartTs)
	if err1 == nil {
		return nil
	}
	choosePump = pump

	return errors.Errorf("write binlog failed, the last error %v", err)
}

func (c *PumpsClient) backoffWriteBinlog(req *pb.WriteBinlogReq, binlogType pb.BinlogType, startTS int64) (pump *PumpStatus, err error) {
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
		if !pump.IsUsable() {
			continue
		}

		pump.ResetGrpcClient()

		resp, err = pump.WriteBinlog(req, c.BinlogWriteTimeout)
		if err == nil {
			if resp.Errmsg != "" {
				err = errors.New(resp.Errmsg)
			} else {
				// if this pump can write binlog success, set this pump to avaliable.
				log.Debugf("[pumps client] write binlog to unavaliable pump %s success, set this pump to avaliable", pump.NodeID)
				c.setPumpAvaliable(pump, true)
				return pump, nil
			}
		}
	}

	return nil, errors.New("write binlog to unavaliable pump failed")
}

func (c *PumpsClient) checkPumpAvaliable() {
	c.Pumps.RLock()
	allPumps := copyPumps(c.Pumps.Pumps)
	c.Pumps.RUnlock()

	for _, pump := range allPumps {
		if !pump.IsUsable() {
			c.setPumpAvaliable(pump, false)
		}
	}
}

// setPumpAvaliable set pump's isAvaliable, and modify UnAvaliablePumps or AvaliablePumps.
func (c *PumpsClient) setPumpAvaliable(pump *PumpStatus, avaliable bool) {
	c.Pumps.Lock()
	defer c.Pumps.Unlock()

	pump.Reset()

	if avaliable {
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

	if pump.IsUsable() {
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
			} else if pump.IsUsable() {
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
		pump.Reset()
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
	rch := c.EtcdRegistry.WatchNode(c.ctx, c.nodePath, revision)

	for {
		select {
		case <-c.ctx.Done():
			log.Info("[pumps client] watch status finished")
			return
		case wresp := <-rch:
			if wresp.Err() != nil {
				// meet error, watch from the latest revision.
				log.Warnf("[pumps client] watch status meet error %v", wresp.Err())
				rch = c.EtcdRegistry.WatchNode(c.ctx, c.nodePath, revision)
				continue
			}

			revision = wresp.Header.Revision

			for _, ev := range wresp.Events {
				status := &node.Status{}
				err := json.Unmarshal(ev.Kv.Value, &status)
				if err != nil {
					log.Errorf("[pumps client] unmarshal pump status %q failed", ev.Kv.Value)
					continue
				}

				switch ev.Type {
				case mvccpb.PUT:
					if !c.exist(status.NodeID) {
						log.Infof("[pumps client] find a new pump %s", status.NodeID)
						c.addPump(NewPumpStatus(status, c.Security), true)
						continue
					}

					pump, avaliableChanged, avaliable := c.updatePump(status)
					if avaliableChanged {
						log.Infof("[pumps client] pump %s's state is changed to %s", pump.Status.NodeID, status.State)
						c.setPumpAvaliable(pump, avaliable)
					}

				case mvccpb.DELETE:
					// now will not delete pump node in fact, just for compatibility.
					nodeID := node.AnalyzeNodeID(string(ev.Kv.Key))
					log.Infof("[pumps client] remove pump %s", nodeID)
					c.removePump(nodeID)
				}
			}
		}
	}
}

// detect send detect binlog to pumps with online state in UnAvaliablePumps,
func (c *PumpsClient) detect() {
	checkTick := time.NewTicker(CheckInterval)
	defer func() {
		checkTick.Stop()
		c.wg.Done()
	}()

	for {
		select {
		case <-c.ctx.Done():
			log.Infof("[pumps client] heartbeat finished")
			return
		case <-checkTick.C:
			// send detect binlog to pump, if this pump can return response without error
			// means this pump is avaliable.
			needCheckPumps := make([]*PumpStatus, 0, len(c.Pumps.UnAvaliablePumps))
			checkPassPumps := make([]*PumpStatus, 0, 1)
			req := &pb.WriteBinlogReq{ClusterID: c.ClusterID, Payload: nil}
			c.Pumps.RLock()
			for _, pump := range c.Pumps.UnAvaliablePumps {
				if pump.IsUsable() {
					needCheckPumps = append(needCheckPumps, pump)
				}
			}
			c.Pumps.RUnlock()

			for _, pump := range needCheckPumps {
				_, err := pump.WriteBinlog(req, c.BinlogWriteTimeout)
				if err == nil {
					log.Debugf("[pumps client] write detect binlog to unavaliable pump %s success", pump.NodeID)
					checkPassPumps = append(checkPassPumps, pump)
				} else {
					log.Debugf("[pumps client] write detect binlog to pump %s error %v", pump.NodeID, err)
				}
			}

			for _, pump := range checkPassPumps {
				c.setPumpAvaliable(pump, true)
			}
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

func isConnUnAvliable(err error) bool {
	// Unavailable indicates the service is currently unavailable.
	// This is a most likely a transient condition and may be corrected
	// by retrying with a backoff.
	// https://github.com/grpc/grpc-go/blob/76cc50721c5fde18bae10a36f4c202f5f2f95bb7/codes/codes.go#L139
	if status.Code(err) == codes.Unavailable {
		return true
	}

	return false
}

func copyPumps(pumps map[string]*PumpStatus) []*PumpStatus {
	ps := make([]*PumpStatus, 0, len(pumps))
	for _, pump := range pumps {
		ps = append(ps, pump)
	}

	return ps
}

// InitLogger initializes logger.
func InitLogger(cfg *logutil.LogConfig) error {
	return logutil.InitLogger(cfg)
}
