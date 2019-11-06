package restore_util

import (
	"bytes"
	"context"
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

// OnSplitFunc is called before split a range.
type OnSplitFunc func(*Range)

// Split executes a region split. It will split regions by the rewrite rules,
// then it will split regions by the end key of each range.
// tableRules includes the prefix of a table, since some ranges may have
// a prefix with record sequence or index sequence.
// note: all ranges and rewrite rules must have raw key.
func (rs *RegionSplitter) Split(
	ctx context.Context,
	ranges []Range,
	rewriteRules *RewriteRules,
	onSplit OnSplitFunc,
) error {
	rangeTree, ok := newRangeTreeWithRewrite(ranges, rewriteRules)
	if !ok {
		return errors.Errorf("ranges overlapped: %v", ranges)
	}
	scatterRegions, err := rs.splitByRewriteRules(ctx, rewriteRules.Data)
	if err != nil {
		return errors.Trace(err)
	}
	rangeTree.Ascend(func(rg *Range) bool {
		if rg == nil {
			return false
		}
		if onSplit != nil {
			onSplit(rg)
		}

		var newRegion *RegionInfo
		newRegion, err = rs.maybeSplitRegion(ctx, rg)
		if err != nil {
			return false
		}
		if newRegion != nil {
			scatterRegions = append(scatterRegions, newRegion)
		}
		return true
	})
	if err != nil {
		return errors.Trace(err)
	}

	for _, region := range scatterRegions {
		rs.waitForScatterRegion(ctx, region)
	}
	return nil
}

// Split regions by the rewrite rules, to ensure all keys of one region only have one prefix.
func (rs *RegionSplitter) splitByRewriteRules(ctx context.Context, rules []*import_sstpb.RewriteRule) ([]*RegionInfo, error) {
	scatterRegions := make([]*RegionInfo, 0)
	for _, rule := range rules {
		newRegion, err := rs.maybeSplitRegion(ctx, &Range{
			StartKey: rule.GetNewKeyPrefix(),
			EndKey:   rule.GetNewKeyPrefix(),
		})
		if err != nil {
			return nil, errors.Trace(err)
		}
		if newRegion != nil {
			scatterRegions = append(scatterRegions, newRegion)
		}
	}
	return scatterRegions, nil
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

func (rs *RegionSplitter) waitForScatterRegion(ctx context.Context, regionInfo *RegionInfo) {
	interval := ScatterWaitInterval
	regionID := regionInfo.Region.GetId()
	for i := 0; i < ScatterWaitMaxRetryTimes; i++ {
		ok, err := rs.isScatterRegionFinished(ctx, regionID)
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
}

func (rs *RegionSplitter) maybeSplitRegion(ctx context.Context, r *Range) (*RegionInfo, error) {
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
				return nil, nil
			}
			newRegion, err := rs.splitAndScatterRegion(ctx, regionInfo, splitKey)
			if err == nil {
				return newRegion, nil
			}
		}
		interval = 2 * interval
		if interval > SplitMaxRetryInterval {
			interval = SplitMaxRetryInterval
		}
		time.Sleep(interval)
	}
	return nil, err
}

func (rs *RegionSplitter) splitAndScatterRegion(ctx context.Context, regionInfo *RegionInfo, key []byte) (*RegionInfo, error) {
	newRegion, err := rs.client.SplitRegion(ctx, regionInfo, key)
	if err != nil {
		return nil, errors.Trace(err)
	}
	// Wait for a while until the region successfully splits.
	err = rs.waitForSplit(ctx, newRegion.Region.GetId())
	if err != nil {
		return nil, errors.Trace(err)
	}
	return newRegion, rs.client.ScatterRegion(ctx, regionInfo)
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
