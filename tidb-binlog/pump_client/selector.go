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
)

const (
	// Hash means hash algorithm
	Hash = "hash"

	// Score means choose pump by it's score.
	Score = "score"
)

// PumpSelector selects pump for sending binlog.
type PumpSelector interface {
	// SetPumps set pumps to be selected.
	SetPumps(map[string]*PumpStatus)

	// Select returns a situable pump.
	Select(*pb.Binlog) *PumpStatus

	// returns the next pump.
	Next(*PumpStatus, *pb.Binlog, int) *PumpStatus
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
func (h *HashSelector) SetPumps(pumps map[string]*PumpStatus) {
	h.Lock()
	h.PumpMap = pumps
	h.Pumps = make([]*PumpStatus, 0, len(pumps))
	for _, pump := range pumps {
		h.Pumps = append(h.Pumps, pump)
	}
	h.Unlock()
}

// Select implement PumpSelector.Select.
func (h *HashSelector) Select(binlog *pb.Binlog) *PumpStatus {
	h.Lock()
	defer h.Unlock()

	if len(h.Pumps) == 0 {
		return nil
	}

	if binlog.Tp == pb.BinlogType_Prewrite {
		pump := h.Pumps[hashTs(binlog.StartTs)%len(h.Pumps)]
		h.TsMap[binlog.StartTs] = pump
		return pump
	}

	// binlog is commit binlog or rollback binlog, choose the same pump by start ts map.
	if pump, ok := h.TsMap[binlog.StartTs]; ok {
		delete(h.TsMap, binlog.StartTs)
		if _, ok = h.PumpMap[pump.NodeID]; ok {
			return pump
		}
	}

	// can't find pump in ts map, or the pump is not avaliable, choose a new one.
	return h.Pumps[hashTs(binlog.StartTs)%len(h.Pumps)]
}

// Next implement PumpSelector.Next.
func (h *HashSelector) Next(pump *PumpStatus, binlog *pb.Binlog, retryTime int) *PumpStatus {
	if len(h.Pumps) == 0 {
		return nil
	}

	nextPump := h.Pumps[(hashTs(binlog.StartTs)+int(retryTime))%len(h.Pumps)]
	h.Lock()
	if binlog.Tp == pb.BinlogType_Prewrite {
		h.TsMap[binlog.StartTs] = pump
	}
	h.Unlock()

	return nextPump
}

// ScoreSelector select a pump by pump's score.
type ScoreSelector struct{}

// NewScoreSelector returns a new ScoreSelector.
func NewScoreSelector() PumpSelector {
	return &ScoreSelector{}
}

// SetPumps implement PumpSelector.SetPumps.
func (s *ScoreSelector) SetPumps(pumps map[string]*PumpStatus) {
	// TODO
}

// Select implement PumpSelector.Select.
func (s *ScoreSelector) Select(binlog *pb.Binlog) *PumpStatus {
	// TODO
	return nil
}

// Next implement PumpSelector.Next.
func (s *ScoreSelector) Next(pump *PumpStatus, binlog *pb.Binlog, retryTime int) *PumpStatus {
	// TODO
	return nil
}

func hashTs(ts int64) int {
	h := fnv.New32a()
	h.Write([]byte(strconv.FormatInt(ts, 10)))
	return int(h.Sum32())
}
