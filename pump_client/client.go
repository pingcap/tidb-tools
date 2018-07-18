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


// PumpsClient is the client of pumps.
type PumpsClient struct {
	// the client of pd.
	PdClient  pd.Client
	
	// Pumps saves the whole pumps' status.
	Pumps     []*PumpStatus

	// AvliablePumps saves the whole avaliable pumps' status.
	AvaliablePumps []*PumpStatus

	// NeedCheckPumps saves the pumps need to be checked.
	NeedCheckPumps []*PumpStatus
}

// NewPumpClient returns a PumpsClient.
func NewPumpsClient(endpoints []string, security *tls.Config) *PumpsClient {


}