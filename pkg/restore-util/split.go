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

	ScatterWaitMaxRetryTimes = 64
	ScatterWaitInterval      = 50 * time.Millisecond
	ScatterMaxWaitInterval   = time.Second

	ScatterWaitUpperInterval = 180 * time.Second
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
type OnSplitFunc func(key [][]byte)

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
	if len(ranges) == 0 {
		return nil
	}
	startTime := time.Now()
	// Sort the range for getting the min and max key of the ranges
	sortedRanges, err := sortRanges(ranges, rewriteRules)
	if err != nil {
		return errors.Trace(err)
	}
	minKey := codec.EncodeBytes([]byte{}, sortedRanges[0].StartKey)
	maxKey := codec.EncodeBytes([]byte{}, sortedRanges[len(sortedRanges)-1].EndKey)
	for _, rule := range rewriteRules.Table {
		if bytes.Compare(minKey, rule.GetNewKeyPrefix()) > 0 {
			minKey = rule.GetNewKeyPrefix()
		}
		if bytes.Compare(maxKey, rule.GetNewKeyPrefix()) < 0 {
			maxKey = rule.GetNewKeyPrefix()
		}
	}
	for _, rule := range rewriteRules.Data {
		if bytes.Compare(minKey, rule.GetNewKeyPrefix()) > 0 {
			minKey = rule.GetNewKeyPrefix()
		}
		if bytes.Compare(maxKey, rule.GetNewKeyPrefix()) < 0 {
			maxKey = rule.GetNewKeyPrefix()
		}
	}
	interval := SplitRetryInterval
	scatterRegions := make([]*RegionInfo, 0)
SplitRegions:
	for i := 0; i < SplitRetryTimes; i++ {
		var regions []*RegionInfo
		regions, err = rs.client.ScanRegions(ctx, minKey, maxKey, 0)
		if err != nil {
			return errors.Trace(err)
		}
		if len(regions) == 0 {
			log.Warn("cannot scan any region")
			return nil
		}
		splitKeyMap := getSplitKeys(rewriteRules, sortedRanges, regions)
		regionMap := make(map[uint64]*RegionInfo)
		for _, region := range regions {
			regionMap[region.Region.GetId()] = region
		}
		for regionID, keys := range splitKeyMap {
			var newRegions []*RegionInfo
			newRegions, err = rs.splitAndScatterRegions(ctx, regionMap[regionID], keys)
			if err != nil {
				interval = 2 * interval
				if interval > SplitMaxRetryInterval {
					interval = SplitMaxRetryInterval
				}
				time.Sleep(interval)
				if i > 3 {
					log.Warn("splitting regions failed, retry it", zap.Error(err))
				}
				continue SplitRegions
			}
			scatterRegions = append(scatterRegions, newRegions...)
			onSplit(keys)
		}
		break
	}
	if err != nil {
		return errors.Trace(err)
	}
	log.Info("splitting regions done, wait for scattering regions",
		zap.Int("regions", len(scatterRegions)), zap.Duration("take", time.Since(startTime)))
	startTime = time.Now()
	scatterCount := 0
	for _, region := range scatterRegions {
		rs.waitForScatterRegion(ctx, region)
		if time.Since(startTime) > ScatterWaitUpperInterval {
			break
		}
		scatterCount++
	}
	if scatterCount == len(scatterRegions) {
		log.Info("waiting for scattering regions done",
			zap.Int("regions", len(scatterRegions)), zap.Duration("take", time.Since(startTime)))
	} else {
		log.Warn("waiting for scattering regions timeout",
			zap.Int("scatterCount", scatterCount),
			zap.Int("regions", len(scatterRegions)),
			zap.Duration("take", time.Since(startTime)))
	}
	return nil
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
	if respErr := resp.GetHeader().GetError(); respErr != nil {
		if respErr.GetType() == pdpb.ErrorType_REGION_NOT_FOUND {
			return true, nil
		}
		return false, errors.Errorf("get operator error: %s", respErr.GetType())
	}
	retryTimes := ctx.Value("retryTimes").(int)
	if retryTimes > 3 {
		log.Warn("get operator", zap.Uint64("regionID", regionID), zap.Stringer("resp", resp))
	}
	// If the current operator of the region is not 'scatter-region', we could assume
	// that 'scatter-operator' has finished or timeout
	ok := string(resp.GetDesc()) != "scatter-region" || resp.GetStatus() != pdpb.OperatorStatus_RUNNING
	return ok, nil
}

func (rs *RegionSplitter) waitForSplit(ctx context.Context, regionID uint64) {
	interval := SplitCheckInterval
	for i := 0; i < SplitCheckMaxRetryTimes; i++ {
		ok, err := rs.hasRegion(ctx, regionID)
		if err != nil {
			log.Warn("wait for split failed", zap.Error(err))
			return
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
	return
}

func (rs *RegionSplitter) waitForScatterRegion(ctx context.Context, regionInfo *RegionInfo) {
	interval := ScatterWaitInterval
	regionID := regionInfo.Region.GetId()
	for i := 0; i < ScatterWaitMaxRetryTimes; i++ {
		ctx = context.WithValue(ctx, "retryTimes", i)
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

func (rs *RegionSplitter) splitAndScatterRegions(ctx context.Context, regionInfo *RegionInfo, keys [][]byte) ([]*RegionInfo, error) {
	newRegions, err := rs.client.BatchSplitRegions(ctx, regionInfo, keys)
	if err != nil {
		return nil, err
	}
	for _, region := range newRegions {
		// Wait for a while until the regions successfully splits.
		rs.waitForSplit(ctx, region.Region.Id)
		if err = rs.client.ScatterRegion(ctx, region); err != nil {
			log.Warn("scatter region failed", zap.Stringer("region", region.Region), zap.Error(err))
		}
	}
	return newRegions, nil
}

// getSplitKeys checks if the regions should be split by the new prefix of the rewrites rule and the end key of
// 	the ranges, groups the split keys by region id
func getSplitKeys(rewriteRules *RewriteRules, ranges []Range, regions []*RegionInfo) map[uint64][][]byte {
	splitKeyMap := make(map[uint64][][]byte)
	checkKeys := make([][]byte, 0)
	for _, rule := range rewriteRules.Table {
		checkKeys = append(checkKeys, rule.GetNewKeyPrefix())
	}
	for _, rule := range rewriteRules.Data {
		checkKeys = append(checkKeys, rule.GetNewKeyPrefix())
	}
	for _, rg := range ranges {
		checkKeys = append(checkKeys, rg.EndKey)
	}
	for _, key := range checkKeys {
		if region := needSplit(key, regions); region != nil {
			splitKeys, ok := splitKeyMap[region.Region.GetId()]
			if !ok {
				splitKeys = make([][]byte, 0, 1)
			}
			splitKeyMap[region.Region.GetId()] = append(splitKeys, key)
		}
	}
	return splitKeyMap
}

// needSplit checks whether a key is necessary to split, if true returns the split region
func needSplit(splitKey []byte, regions []*RegionInfo) *RegionInfo {
	// If splitKey is the max key.
	if len(splitKey) == 0 {
		return nil
	}
	splitKey = codec.EncodeBytes([]byte{}, splitKey)
	for _, region := range regions {
		// If splitKey is the boundary of the region
		if bytes.Compare(splitKey, region.Region.GetStartKey()) == 0 {
			return nil
		}
		// If splitKey is in a region
		if bytes.Compare(splitKey, region.Region.GetStartKey()) > 0 && beforeEnd(splitKey, region.Region.GetEndKey()) {
			return region
		}
	}
	return nil
}

func beforeEnd(key []byte, end []byte) bool {
	return bytes.Compare(key, end) < 0 || len(end) == 0
}

func replacePrefix(s []byte, rewriteRules *RewriteRules) ([]byte, *import_sstpb.RewriteRule) {
	// We should search the dataRules firstly.
	for _, rule := range rewriteRules.Data {
		if bytes.HasPrefix(s, rule.GetOldKeyPrefix()) {
			return append(append([]byte{}, rule.GetNewKeyPrefix()...), s[len(rule.GetOldKeyPrefix()):]...), rule
		}
	}
	for _, rule := range rewriteRules.Table {
		if bytes.HasPrefix(s, rule.GetOldKeyPrefix()) {
			return append(append([]byte{}, rule.GetNewKeyPrefix()...), s[len(rule.GetOldKeyPrefix()):]...), rule
		}
	}

	return s, nil
}
