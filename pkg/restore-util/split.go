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
	"go.uber.org/zap"
)

const SplitWaitMaxRetryTimes = 64
const SplitWaitInterval = 8 * time.Millisecond
const SplitMaxWaitInterval = time.Second

const ScatterWaitMaxRetryTimes = 128
const ScatterWaitInterval = 50 * time.Millisecond
const ScatterMaxWaitInterval = 5 * time.Second

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
func (rs *RegionSplitter) Split(ctx context.Context, ranges []Range, tableRules []*import_sstpb.RewriteRule, dataRules []*import_sstpb.RewriteRule) error {
	var wg sync.WaitGroup
	rangeTree, ok := newRangeTreeWithRewrite(ranges, tableRules, dataRules)
	if !ok {
		return errors.New("ranges overlapped")
	}
	err := rs.splitByRewriteRules(ctx, &wg, dataRules)
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
		key := rule.GetNewKeyPrefix()
		regionInfo, err := rs.client.GetRegion(ctx, key)
		if err != nil {
			return errors.Trace(err)
		}
		err = rs.splitAndScatterRegion(ctx, regionInfo, key, wg)
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func newRangeTreeWithRewrite(ranges []Range, tableRules []*import_sstpb.RewriteRule, dataRules []*import_sstpb.RewriteRule) (*RangeTree, bool) {
	rangeTree := NewRangeTree()
	for _, rg := range ranges {
		rg.StartKey = replacePrefix(rg.StartKey, tableRules, dataRules)
		rg.EndKey = replacePrefix(rg.EndKey, tableRules, dataRules)
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
	interval := SplitWaitInterval
	for i := 0; i < SplitWaitMaxRetryTimes; i++ {
		ok, err := rs.hasRegion(ctx, regionID)
		if err != nil {
			return err
		}
		if ok {
			break
		} else {
			interval = 2 * interval
			if interval > SplitMaxWaitInterval {
				interval = SplitMaxWaitInterval
			}
			time.Sleep(interval)
		}
	}
	return nil
}

func (rs *RegionSplitter) waitForScatter(ctx context.Context, wg *sync.WaitGroup, regionID uint64) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		interval := ScatterWaitInterval
		for i := 0; i < ScatterWaitMaxRetryTimes; i++ {
			ok, err := rs.hasRegion(ctx, regionID)
			if err != nil {
				log.Error("scatter region failed: do not has the region", zap.Uint64("region_id", regionID))
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
	regionInfo, err := rs.client.GetRegion(ctx, r.StartKey)
	if err != nil {
		return errors.Trace(err)
	}
	if !needSplit(r, regionInfo) {
		return nil
	}
	err = rs.splitAndScatterRegion(ctx, regionInfo, r.EndKey, wg)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (rs *RegionSplitter) splitAndScatterRegion(ctx context.Context, regionInfo *RegionInfo, key []byte, wg *sync.WaitGroup) error {
	newRegion, err := rs.client.SplitRegion(ctx, regionInfo, key)
	if err != nil {
		return errors.Trace(err)
	}
	// Wait for a while until the region successfully splits
	err = rs.waitForSplit(ctx, newRegion.Region.GetId())
	if err != nil {
		return errors.Trace(err)
	}
	err = rs.client.ScatterRegion(ctx, newRegion)
	if err != nil {
		return errors.Trace(err)
	}
	rs.waitForScatter(ctx, wg, newRegion.Region.GetId())
	return err
}

func needSplit(r *Range, regionInfo *RegionInfo) bool {
	splitKey := r.EndKey
	// If splitKey is the max key
	if len(splitKey) == 0 {
		return false
	}
	// If splitKey is not in the region
	if bytes.Compare(splitKey, regionInfo.Region.GetStartKey()) <= 0 {
		return false
	}
	return beforeEnd(splitKey, regionInfo.Region.GetEndKey())
}

func beforeEnd(key []byte, end []byte) bool {
	return bytes.Compare(key, end) < 0 || len(end) == 0
}

func replacePrefix(s []byte, tableRules []*import_sstpb.RewriteRule, dataRules []*import_sstpb.RewriteRule) []byte {
	// We should search the dataRules firstly
	for _, rule := range dataRules {
		if bytes.HasPrefix(s, rule.GetOldKeyPrefix()) {
			return append(rule.GetNewKeyPrefix(), s[len(rule.GetOldKeyPrefix()):]...)
		}
	}
	for _, rule := range tableRules {
		if bytes.HasPrefix(s, rule.GetOldKeyPrefix()) {
			return append(rule.GetNewKeyPrefix(), s[len(rule.GetOldKeyPrefix()):]...)
		}
	}
	return s
}
