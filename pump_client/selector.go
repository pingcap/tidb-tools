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
	"sync"

	binlog "github.com/pingcap/tipb/go-binlog"
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
	SetPumps([]*PumpStatus)

	// Select returns a situable pump.
	Select(*binlog.Binlog) *PumpStatus

	// returns the next pump.
	Next(*PumpStatus, *binlog.Binlog, int32) *PumpStatus

	// DeleteTsMap removes the map information of ts.
	DeleteTsMap(ts int64)
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
	h.Pumps = pumps
	for _, pump := range pumps {
		h.PumpMap[pump.NodeID] = pump
	}
	h.Unlock()
}

// Select implement PumpSelector.Select.
func (h *HashSelector) Select(b *binlog.Binlog) *PumpStatus {
	if b.Tp == binlog.BinlogType_Prewrite {
		pump := h.Pumps[int(b.StartTs)%len(h.Pumps)]
		h.Lock()
		h.TsMap[b.StartTs] = pump
		h.Unlock()
		return h.Pumps[int(b.StartTs)%len(h.Pumps)]
	}

	h.RLock()
	pump, ok := h.TsMap[b.StartTs]
	h.Unlock()
	if ok {
		return pump
	}

	return h.Pumps[int(b.StartTs)%len(h.Pumps)]
}

// Next implement PumpSelector.Next.
func (h *HashSelector) Next(pump *PumpStatus, b *binlog.Binlog, retryTime int32) *PumpStatus {
	nextPump := h.Pumps[(int(b.StartTs)+int(retryTime))%len(h.Pumps)]
	h.Lock()
	h.TsMap[b.StartTs] = pump
	h.Unlock()

	return nextPump
}

// DeleteTsMap implement PumpSelector.DeleteTsMap.
func (h *HashSelector) DeleteTsMap(ts int64) {
	h.Lock()
	delete(h.TsMap, ts)
	h.Unlock()
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
func (s *ScoreSelector) Select(b *binlog.Binlog) *PumpStatus {
	// TODO
	return nil
}

// Next implement PumpSelector.Next.
func (s *ScoreSelector) Next(pump *PumpStatus, b *binlog.Binlog, retryTime int32) *PumpStatus {
	// TODO
	return nil
}

// DeleteTsMap implement PumpSelector.DeleteTsMap.
func (s *ScoreSelector) DeleteTsMap(ts int64) {}
