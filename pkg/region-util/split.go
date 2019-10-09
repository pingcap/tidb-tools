package region_util

import (
	"bytes"
	"context"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/pdpb"
)

var SplitWaitMaxRetryTimes = 64
var SplitWaitIntervalMillis = 8
var SplitMaxWaitIntervalMillis = 1000

var ScatterWaitMaxRetryTimes = 128
var ScatterWaitIntervalMillis = 50
var ScatterMaxWaitIntervalMillis = 5000

type RegionSplitter struct {
	client   Client
	limitSize  uint64

	ctx    context.Context
	cancel context.CancelFunc
}

func NewRegionSplitter(ctx context.Context, client Client, limitSize uint64) *RegionSplitter {
	ctx, cancel := context.WithCancel(ctx)
	return &RegionSplitter{
		client:  client,
		limitSize: limitSize,
		ctx:       ctx,
		cancel:    cancel,
	}
}

func (rs *RegionSplitter) Split(ranges []*Range) ([]*Range, error) {
	var start []byte
	var numPrepares int
	var wg sync.WaitGroup
	newRanges := make([]*Range, 0)
	rangeCtx := NewRangeContext(rs.client, rs.limitSize)
	for _, r := range ranges {
		rangeCtx.Add(r.Size)
		if rangeCtx.ShouldStopBefore(r.StartKey) {
			continue
		}
		// Create a new range, maybe overlap multiple regions
		newRange := &Range{
			StartKey: start,
			EndKey:   r.StartKey,
			Size:     rangeCtx.RawSize(),
		}
		newRanges = append(newRanges, newRange)
		// Maybe split a region for the range
		ok, err := rs.maybeSplitRegion(newRange, &wg)
		if err == nil && ok == true {
			numPrepares++
		}
		start = r.StartKey
		// Reset the context for the next range
		err = rangeCtx.Reset(rs.ctx, r.StartKey)
		if err != nil {
			return nil, err
		}
	}
	// Wait for all regions scattering success
	wg.Wait()
	return newRanges, nil
}

func (rs *RegionSplitter) hasRegion(regionID uint64) (bool, error) {
	regionInfo, err := rs.client.GetRegionByID(rs.ctx, regionID)
	if err != nil {
		return false, err
	}
	if regionInfo == nil {
		return false, nil
	}
	return true, nil
}

func (rs *RegionSplitter) isScatterRegionFinished(regionID uint64) (bool, error) {
	resp, err := rs.client.GetOperator(rs.ctx, regionID)
	if err != nil {
		return false, err
	}
	// Heartbeat may not be sent to PD
	if resp.GetHeader().GetError().GetType() == pdpb.ErrorType_REGION_NOT_FOUND {
		return true, nil
	}
	// If the current operator of the region is not 'scatter-region', we could assume
	// that 'scatter-operator' has finished or timeout
	ok := string(resp.GetDesc()) != "scatter-region" || resp.GetStatus() != pdpb.OperatorStatus_RUNNING
	return ok, nil
}

func (rs *RegionSplitter) waitForSplit(regionID uint64) error {
	interval := SplitWaitIntervalMillis
	for i := 0; i < SplitWaitMaxRetryTimes; i++ {
		ok, err := rs.hasRegion(regionID)
		if err != nil {
			return err
		}
		if ok {
			break
		} else {
			interval = 2 * interval
			if interval > SplitMaxWaitIntervalMillis {
				interval = SplitMaxWaitIntervalMillis
			}
			time.Sleep(time.Millisecond * time.Duration(interval))
		}
	}
	return nil
}

func (rs *RegionSplitter) waitForScatter(regionID uint64) error {
	interval := ScatterWaitIntervalMillis
	for i := 0; i < ScatterWaitMaxRetryTimes; i++ {
		ok, err := rs.hasRegion(regionID)
		if err != nil {
			return err
		}
		if ok {
			break
		} else {
			interval = 2 * interval
			if interval > ScatterMaxWaitIntervalMillis {
				interval = ScatterMaxWaitIntervalMillis
			}
			time.Sleep(time.Millisecond * time.Duration(interval))
		}
	}
	return nil
}

func (rs *RegionSplitter) maybeSplitRegion(r *Range, wg *sync.WaitGroup) (bool, error) {
	regionInfo, err := rs.client.GetRegion(rs.ctx, r.StartKey)
	if err != nil {
		return false, errors.Trace(err)
	}
	if !needSplit(r, regionInfo) {
		return false, nil
	}
	newRegion, err := rs.client.SplitRegion(rs.ctx, regionInfo, r.EndKey)
	if err != nil {
		return false, errors.Trace(err)
	}
	// Wait for a while until the region successfully splits
	err = rs.waitForSplit(newRegion.Region.GetId())
	if err != nil {
		return false, errors.Trace(err)
	}
	err = rs.client.ScatterRegion(rs.ctx, newRegion)
	if err != nil {
		return false, errors.Trace(err)
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		rs.waitForScatter(newRegion.Region.GetId())
	}()
	return true, nil
}

func needSplit(r *Range, regionInfo *RegionInfo) bool {
	splitKey := r.EndKey
	if len(splitKey) == 0 {
		return false
	}
	if bytes.Compare(splitKey, regionInfo.Region.GetEndKey()) <= 0 {
		return false
	}
	return beforeEnd(splitKey, regionInfo.Region.GetEndKey())
}
