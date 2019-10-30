package restore_util

import (
	"bytes"
	"context"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/util/codec"
	"go.uber.org/zap"
)

const (
	SplitRetryTimes       = 32
	SplitRetryInterval    = 50 * time.Millisecond
	SplitMaxRetryInterval = time.Second

	SplitCheckMaxRetryTimes = 64
	SplitCheckInterval      = 8 * time.Millisecond
	SplitMaxCheckInterval   = time.Second

	ScatterWaitMaxRetryTimes = 128
	ScatterWaitInterval      = 50 * time.Millisecond
	ScatterMaxWaitInterval   = 5 * time.Second
)

// RegionSplitter is a executor of region split by rules.
type RegionSplitter struct {
	client    Client
	rangeTree *RangeTree
}

// NewRegionSplitter returns a new RegionSplitter.
func NewRegionSplitter(client Client) *RegionSplitter {
	return &RegionSplitter{
		client: client,
	}
}

// Split executes a region split. It will split regions by the rewrite rules,
// then it will split regions by the end key of each range.
// tableRules includes the prefix of a table, since some ranges may have a prefix with record sequence or index sequence.
// note: all ranges and rewrite rules must have raw key.
func (rs *RegionSplitter) Split(ctx context.Context, ranges []Range, rewriteRules *RewriteRules) error {
	var wg sync.WaitGroup
	rangeTree, ok := newRangeTreeWithRewrite(ranges, rewriteRules)
	if !ok {
		return errors.Errorf("ranges overlapped: %v", ranges)
	}
	err := rs.splitByRewriteRules(ctx, &wg, rewriteRules.Data)
	if err != nil {
		return errors.Trace(err)
	}
	rangeTree.Ascend(func(rg *Range) bool {
		if rg == nil {
			return false
		}
		err = rs.maybeSplitRegion(ctx, rg, &wg)
		return err == nil
	})
	if err != nil {
		return errors.Trace(err)
	}

	// Wait for all regions scatter success
	wg.Wait()
	return nil
}

// Split regions by the rewrite rules, to ensure all keys of one region only have one prefix.
func (rs *RegionSplitter) splitByRewriteRules(ctx context.Context, wg *sync.WaitGroup, rules []*import_sstpb.RewriteRule) error {
	for _, rule := range rules {
		err := rs.maybeSplitRegion(ctx, &Range{
			StartKey: rule.GetNewKeyPrefix(),
			EndKey:   rule.GetNewKeyPrefix(),
		}, wg)
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func newRangeTreeWithRewrite(ranges []Range, rewriteRules *RewriteRules) (*RangeTree, bool) {
	rangeTree := NewRangeTree()
	for _, rg := range ranges {
		rg.StartKey = replacePrefix(rg.StartKey, rewriteRules)
		rg.EndKey = replacePrefix(rg.EndKey, rewriteRules)
		if !rangeTree.InsertRange(rg) {
			return nil, false
		}
	}
	return rangeTree, true
}

func (rs *RegionSplitter) hasRegion(ctx context.Context, regionID uint64) (bool, error) {
	regionInfo, err := rs.client.GetRegionByID(ctx, regionID)
	if err != nil {
		return false, err
	}
	return regionInfo != nil, nil
}

func (rs *RegionSplitter) isScatterRegionFinished(ctx context.Context, regionID uint64) (bool, error) {
	resp, err := rs.client.GetOperator(ctx, regionID)
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

func (rs *RegionSplitter) waitForSplit(ctx context.Context, regionID uint64) error {
	interval := SplitCheckInterval
	for i := 0; i < SplitCheckMaxRetryTimes; i++ {
		ok, err := rs.hasRegion(ctx, regionID)
		if err != nil {
			return errors.Trace(err)
		}
		if ok {
			break
		} else {
			interval = 2 * interval
			if interval > SplitMaxCheckInterval {
				interval = SplitMaxCheckInterval
			}
			time.Sleep(interval)
		}
	}
	return nil
}

func (rs *RegionSplitter) tryToScatterRegion(ctx context.Context, wg *sync.WaitGroup, regionInfo *RegionInfo) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := rs.client.ScatterRegion(ctx, regionInfo)
		if err != nil {
			log.Warn("scatter region failed", zap.Error(err), zap.Reflect("region", regionInfo.Region))
			return
		}
		interval := ScatterWaitInterval
		regionID := regionInfo.Region.GetId()
		for i := 0; i < ScatterWaitMaxRetryTimes; i++ {
			ok, err := rs.hasRegion(ctx, regionID)
			if err != nil {
				log.Warn("scatter region failed: do not have the region", zap.Reflect("region", regionInfo.Region))
				return
			}
			if ok {
				break
			} else {
				interval = 2 * interval
				if interval > ScatterMaxWaitInterval {
					interval = ScatterMaxWaitInterval
				}
				time.Sleep(interval)
			}
		}
	}()
}

func (rs *RegionSplitter) maybeSplitRegion(ctx context.Context, r *Range, wg *sync.WaitGroup) error {
	interval := SplitRetryInterval
	var err error
	for i := 0; i < SplitRetryTimes; i++ {
		if i > 0 {
			log.Warn("split region failed, retry it", zap.Error(err), zap.Reflect("key", r.StartKey))
		}
		var regionInfo *RegionInfo
		regionInfo, err = rs.client.GetRegion(ctx, codec.EncodeBytes([]byte{}, r.StartKey))
		if err == nil {
			splitKey := r.EndKey
			if !needSplit(codec.EncodeBytes([]byte{}, splitKey), regionInfo) {
				return nil
			}
			err = rs.splitAndScatterRegion(ctx, regionInfo, splitKey, wg)
			if err == nil {
				return nil
			}
		}
		interval = 2 * interval
		if interval > SplitMaxRetryInterval {
			interval = SplitMaxRetryInterval
		}
		time.Sleep(interval)
	}
	return err
}

func (rs *RegionSplitter) splitAndScatterRegion(ctx context.Context, regionInfo *RegionInfo, key []byte, wg *sync.WaitGroup) error {
	newRegion, err := rs.client.SplitRegion(ctx, regionInfo, key)
	if err != nil {
		return errors.Trace(err)
	}
	// Wait for a while until the region successfully splits.
	err = rs.waitForSplit(ctx, newRegion.Region.GetId())
	if err != nil {
		return errors.Trace(err)
	}
	rs.tryToScatterRegion(ctx, wg, newRegion)
	return err
}

func needSplit(splitKey []byte, regionInfo *RegionInfo) bool {
	// If splitKey is the max key.
	if len(splitKey) == 0 {
		return false
	}
	// If splitKey is not in the region or is the boundary of the region.
	if bytes.Compare(splitKey, regionInfo.Region.GetStartKey()) <= 0 {
		return false
	}
	return beforeEnd(splitKey, regionInfo.Region.GetEndKey())
}

func beforeEnd(key []byte, end []byte) bool {
	return bytes.Compare(key, end) < 0 || len(end) == 0
}

func replacePrefix(s []byte, rewriteRules *RewriteRules) []byte {
	// We should search the dataRules firstly.
	for _, rule := range rewriteRules.Data {
		if bytes.HasPrefix(s, rule.GetOldKeyPrefix()) {
			return append(rule.GetNewKeyPrefix(), s[len(rule.GetOldKeyPrefix()):]...)
		}
	}
	for _, rule := range rewriteRules.Table {
		if bytes.HasPrefix(s, rule.GetOldKeyPrefix()) {
			return append(rule.GetNewKeyPrefix(), s[len(rule.GetOldKeyPrefix()):]...)
		}
	}
	return s
}
