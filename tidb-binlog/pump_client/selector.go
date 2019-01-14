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
	"hash/fnv"
	"strconv"
	"sync"

	pb "github.com/pingcap/tipb/go-binlog"
	log "github.com/sirupsen/logrus"
)

const (
	// Range means range algorithm.
	Range = "range"

	// Hash means hash algorithm.
	Hash = "hash"

	// Score means choose pump by it's score.
	Score = "score"

	// LocalUnix means will only use the local pump by unix socket.
	LocalUnix = "local unix"
)

// PumpSelector selects pump for sending binlog.
type PumpSelector interface {
	// SetPumps set pumps to be selected.
	SetPumps([]*PumpStatus)

	// Select returns a situable pump. Tips: should call this function only one time for commit/rollback binlog.
	Select(binlog *pb.Binlog, retryTime int) *PumpStatus

	// Feedback set the corresponding relations between startTS and pump.
	Feedback(startTS int64, binlogType pb.BinlogType, pump *PumpStatus)
}

// HashSelector select a pump by hash.
type HashSelector struct {
	sync.RWMutex

	// TsMap saves the map of start_ts with pump when send prepare binlog.
	// And Commit binlog should send to the same pump.
	TsMap map[int64]*PumpStatus

	// PumpMap saves the map of pump's node id with pump.
	PumpMap map[string]*PumpStatus

	// the pumps to be selected.
	Pumps []*PumpStatus
}

// NewHashSelector returns a new HashSelector.
func NewHashSelector() PumpSelector {
	return &HashSelector{
		TsMap:   make(map[int64]*PumpStatus),
		PumpMap: make(map[string]*PumpStatus),
		Pumps:   make([]*PumpStatus, 0, 10),
	}
}

// SetPumps implement PumpSelector.SetPumps.
func (h *HashSelector) SetPumps(pumps []*PumpStatus) {
	h.Lock()
	h.PumpMap = make(map[string]*PumpStatus)
	h.Pumps = pumps
	for _, pump := range pumps {
		h.PumpMap[pump.NodeID] = pump
	}
	h.Unlock()
}

// Select implement PumpSelector.Select.
func (h *HashSelector) Select(binlog *pb.Binlog, retryTime int) *PumpStatus {
	// TODO: use status' label to match situale pump.
	h.Lock()
	defer h.Unlock()

	if binlog.Tp != pb.BinlogType_Prewrite {
		// binlog is commit binlog or rollback binlog, choose the same pump by start ts map.
		if pump, ok := h.TsMap[binlog.StartTs]; ok {
			return pump
		}

		// this should never happened
		log.Warnf("[pumps client] %s binlog with start ts %d don't have matched prewrite binlog", binlog.Tp, binlog.StartTs)
		return nil
	}

	if len(h.Pumps) == 0 {
		return nil
	}

	pump := h.Pumps[(hashTs(binlog.StartTs)+int(retryTime))%len(h.Pumps)]
	return pump
}

// Feedback implement PumpSelector.Feedback
func (h *HashSelector) Feedback(startTS int64, binlogType pb.BinlogType, pump *PumpStatus) {
	h.Lock()
	if binlogType != pb.BinlogType_Prewrite {
		delete(h.TsMap, startTS)
	} else {
		h.TsMap[startTS] = pump
	}
	h.Unlock()
}

// RangeSelector select a pump by range.
type RangeSelector struct {
	sync.RWMutex

	// Offset saves the offset in Pumps.
	Offset int

	// TsMap saves the map of start_ts with pump when send prepare binlog.
	// And Commit binlog should send to the same pump.
	TsMap map[int64]*PumpStatus

	// PumpMap saves the map of pump's node id with pump.
	PumpMap map[string]*PumpStatus

	// the pumps to be selected.
	Pumps []*PumpStatus
}

// NewRangeSelector returns a new ScoreSelector.
func NewRangeSelector() PumpSelector {
	return &RangeSelector{
		Offset:  0,
		TsMap:   make(map[int64]*PumpStatus),
		PumpMap: make(map[string]*PumpStatus),
		Pumps:   make([]*PumpStatus, 0, 10),
	}
}

// SetPumps implement PumpSelector.SetPumps.
func (r *RangeSelector) SetPumps(pumps []*PumpStatus) {
	r.Lock()
	r.PumpMap = make(map[string]*PumpStatus)
	r.Pumps = pumps
	for _, pump := range pumps {
		r.PumpMap[pump.NodeID] = pump
	}
	r.Offset = 0
	r.Unlock()
}

// Select implement PumpSelector.Select.
func (r *RangeSelector) Select(binlog *pb.Binlog, retryTime int) *PumpStatus {
	// TODO: use status' label to match situable pump.
	r.Lock()
	defer r.Unlock()

	if binlog.Tp != pb.BinlogType_Prewrite {
		// binlog is commit binlog or rollback binlog, choose the same pump by start ts map.
		if pump, ok := r.TsMap[binlog.StartTs]; ok {
			return pump
		}

		// this should never happened
		log.Warnf("[pumps client] %s binlog with start ts %d don't have matched prewrite binlog", binlog.Tp, binlog.StartTs)
		return nil
	}

	if len(r.Pumps) == 0 {
		return nil
	}

	if r.Offset >= len(r.Pumps) {
		r.Offset = 0
	}

	pump := r.Pumps[r.Offset]

	r.Offset++
	return pump
}

// Feedback implement PumpSelector.Select
func (r *RangeSelector) Feedback(startTS int64, binlogType pb.BinlogType, pump *PumpStatus) {
	r.Lock()
	if binlogType != pb.BinlogType_Prewrite {
		delete(r.TsMap, startTS)
	} else {
		r.TsMap[startTS] = pump
	}
	r.Unlock()
}

// LocalUnixSelector will always select the local pump, used for compatible with kafka version tidb-binlog.
type LocalUnixSelector struct {
	sync.RWMutex

	// the pump to be selected.
	Pump *PumpStatus
}

// NewLocalUnixSelector returns a new LocalUnixSelector.
func NewLocalUnixSelector() PumpSelector {
	return &LocalUnixSelector{}
}

// SetPumps implement PumpSelector.SetPumps.
func (u *LocalUnixSelector) SetPumps(pumps []*PumpStatus) {
	u.Lock()
	if len(pumps) == 0 {
		u.Pump = nil
	} else {
		u.Pump = pumps[0]
	}
	u.Unlock()
}

// Select implement PumpSelector.Select.
func (u *LocalUnixSelector) Select(binlog *pb.Binlog, retryTime int) *PumpStatus {
	u.RLock()
	defer u.RUnlock()

	return u.Pump
}

// Feedback implement PumpSelector.Feedback
func (u *LocalUnixSelector) Feedback(startTS int64, binlogType pb.BinlogType, pump *PumpStatus) {
	return
}

// ScoreSelector select a pump by pump's score.
type ScoreSelector struct{}

// NewScoreSelector returns a new ScoreSelector.
func NewScoreSelector() PumpSelector {
	return &ScoreSelector{}
}

// SetPumps implement PumpSelector.SetPumps.
func (s *ScoreSelector) SetPumps(pumps []*PumpStatus) {
	// TODO
}

// Select implement PumpSelector.Select.
func (s *ScoreSelector) Select(binlog *pb.Binlog, retryTime int) *PumpStatus {
	// TODO
	return nil
}

// Feedback implement PumpSelector.Feedback
func (s *ScoreSelector) Feedback(startTS int64, binlogType pb.BinlogType, pump *PumpStatus) {
	// TODO
}

// NewSelector returns a PumpSelector according to the algorithm.
func NewSelector(algorithm string) PumpSelector {
	var selector PumpSelector
	switch algorithm {
	case Range:
		selector = NewRangeSelector()
	case Hash:
		selector = NewHashSelector()
	case Score:
		selector = NewScoreSelector()
	case LocalUnix:
		selector = NewLocalUnixSelector()
	default:
		log.Warnf("[pumps client] unknown algorithm %s, use range as default", algorithm)
		selector = NewRangeSelector()
	}

	return selector
}

func hashTs(ts int64) int {
	h := fnv.New32a()
	h.Write([]byte(strconv.FormatInt(ts, 10)))
	return int(h.Sum32())
}
