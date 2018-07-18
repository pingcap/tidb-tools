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

package pump_client

import (
	"time"

	
	"github.com/pingcap/pd/pd-client"
	binlog "github.com/pingcap/tipb/go-binlog"
)

// PumpState is the state of pump.
type PumpState string

const (
	// Online means the pump can receive request.
	Online   PumpState = "online"

	// Pausing means the pump is pausing.
	Pausing  PumpState = "pausing"

	// Paused means the pump is already paused.
	Paused   PumpState = "paused"

	// Closing means the pump is closing, and the state will be Offline when closed.
	Closing  PumpState = "closing"

	// Offline means the pump is offline, and will not provide service.
	Offline  PumpState = "offline"

	// RootPath is the root path of the keys stored in etcd for binlog.
	RootPath = "/tidb-binlog"
)

// PumpStatus saves pump's status
type PumpStatus struct {
	// the id of pump.
	NodeID string

	// the state of pump.
	State *PumpState

	// the score of pump, it is report by pump, calculated by pump's qps, disk usage and binlog's data size. 
	// if Score is less than 0, means the pump is useless.
	Score  int64

	// the label of this pump, pump client will only send to a pump which label is matched.
	Label string

	// the pump is avaliable or not.
	IsAvaliable bool

	// the client of this pump
	Client binlog.PumpClient

	// UpdateTime is the last update time of pump's status.
	UpdateTime time.Time
}

// PumpInfo is pump's information saved in pd reported by pump.
type PumpInfo struct {

}

func GetPumps(pdClient pd.Client) []*PumpStatus {
	
}