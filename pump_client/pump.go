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
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb-tools/pkg/etcd"
	pb "github.com/pingcap/tipb/go-binlog"
)

// PumpState is the state of pump.
type PumpState string

const (
	// Online means the pump can receive request.
	Online PumpState = "online"

	// Pausing means the pump is pausing.
	Pausing PumpState = "pausing"

	// Paused means the pump is already paused.
	Paused PumpState = "paused"

	// Closing means the pump is closing, and the state will be Offline when closed.
	Closing PumpState = "closing"

	// Offline means the pump is offline, and will not provide service.
	Offline PumpState = "offline"

	// RootPath is the root path of the keys stored in etcd for pumps.
	RootPath = "/tidb-binlog/pumps"
)

// PumpStatus saves pump's status
type PumpStatus struct {
	// the id of pump.
	NodeID string

	// the state of pump.
	State PumpState

	// the score of pump, it is report by pump, calculated by pump's qps, disk usage and binlog's data size.
	// if Score is less than 0, means the pump is useless.
	Score int64

	// the label of this pump, pump client will only send to a pump which label is matched.
	Label string

	// the pump is avaliable or not.
	IsAvaliable bool

	// the client of this pump
	Client pb.PumpClient

	// UpdateTime is the last update time of pump's status.
	UpdateTime time.Time
}

// LatestPos is the latest position in pump
type LatestPos struct {
	FilePos  pb.Pos `json:"file-position"`
	KafkaPos pb.Pos `json:"kafka-position"`
}

// NodeStatus describes the status information of a node in etcd
// TODO: adjust this struct with PumpStatus.
type NodeStatus struct {
	NodeID         string
	Host           string
	IsAlive        bool
	IsOffline      bool
	LatestFilePos  pb.Pos
	LatestKafkaPos pb.Pos
	OfflineTS      int64
}

func nodeStatusFromEtcdNode(id string, node *etcd.Node) (*NodeStatus, error) {
	var (
		isAlive       bool
		status        = &NodeStatus{}
		latestPos     = &LatestPos{}
		isObjectExist bool
	)
	for key, n := range node.Childs {
		switch key {
		case "object":
			isObjectExist = true
			if err := json.Unmarshal(n.Value, &status); err != nil {
				return nil, errors.Annotatef(err, "error unmarshal NodeStatus with nodeID(%s)", id)
			}
		case "alive":
			isAlive = true
			if err := json.Unmarshal(n.Value, &latestPos); err != nil {
				return nil, errors.Annotatef(err, "error unmarshal NodeStatus with nodeID(%s)", id)
			}
		}
	}

	if !isObjectExist {
		log.Errorf("node %s doesn't exist", id)
		return nil, nil
	}

	status.IsAlive = isAlive
	if isAlive {
		status.LatestFilePos = latestPos.FilePos
		status.LatestKafkaPos = latestPos.KafkaPos
	}
	return status, nil
}

func nodesStatusFromEtcdNode(root *etcd.Node) ([]*NodeStatus, error) {
	var statuses []*NodeStatus
	for id, n := range root.Childs {
		status, err := nodeStatusFromEtcdNode(id, n)
		if err != nil {
			return nil, err
		}
		if status == nil {
			continue
		}
		statuses = append(statuses, status)
	}
	return statuses, nil
}
