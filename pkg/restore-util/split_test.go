package restore_util

import (
	"bytes"
	"context"
	"sync"
	"testing"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/pd/server/schedule/placement"
	"github.com/pingcap/tidb/util/codec"
)

type testClient struct {
	mu           sync.RWMutex
	stores       map[uint64]*metapb.Store
	regions      map[uint64]*RegionInfo
	nextRegionID uint64
}

func newTestClient(stores map[uint64]*metapb.Store, regions map[uint64]*RegionInfo, nextRegionID uint64) *testClient {
	return &testClient{
		stores:       stores,
		regions:      regions,
		nextRegionID: nextRegionID,
	}
}

func (c *testClient) GetAllRegions() map[uint64]*RegionInfo {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.regions
}

func (c *testClient) GetStore(ctx context.Context, storeID uint64) (*metapb.Store, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	store, ok := c.stores[storeID]
	if !ok {
		return nil, errors.Errorf("store not found")
	}
	return store, nil
}

func (c *testClient) GetRegion(ctx context.Context, key []byte) (*RegionInfo, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	for _, region := range c.regions {
		if bytes.Compare(key, region.Region.StartKey) >= 0 &&
			(len(region.Region.EndKey) == 0 || bytes.Compare(key, region.Region.EndKey) < 0) {
			return region, nil
		}
	}
	return nil, errors.Errorf("region not found: key=%s", string(key))
}

func (c *testClient) GetRegionByID(ctx context.Context, regionID uint64) (*RegionInfo, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	region, ok := c.regions[regionID]
	if !ok {
		return nil, errors.Errorf("region not found: id=%d", regionID)
	}
	return region, nil
}

func (c *testClient) SplitRegion(ctx context.Context, regionInfo *RegionInfo, key []byte) (*RegionInfo, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	var target *RegionInfo
	splitKey := codec.EncodeBytes([]byte{}, key)
	for _, region := range c.regions {
		if bytes.Compare(splitKey, region.Region.StartKey) >= 0 &&
			(len(region.Region.EndKey) == 0 || bytes.Compare(splitKey, region.Region.EndKey) < 0) {
			target = region
		}
	}
	if target == nil {
		return nil, errors.Errorf("region not found: key=%s", string(key))
	}
	newRegion := &RegionInfo{
		Region: &metapb.Region{
			Peers:    target.Region.Peers,
			Id:       c.nextRegionID,
			StartKey: target.Region.StartKey,
			EndKey:   splitKey,
		},
	}
	c.regions[c.nextRegionID] = newRegion
	c.nextRegionID++
	target.Region.StartKey = splitKey
	c.regions[target.Region.Id] = target
	return newRegion, nil
}

func (c *testClient) ScatterRegion(ctx context.Context, regionInfo *RegionInfo) error {
	return nil
}

func (c *testClient) GetOperator(ctx context.Context, regionID uint64) (*pdpb.GetOperatorResponse, error) {
	return &pdpb.GetOperatorResponse{
		Header: new(pdpb.ResponseHeader),
	}, nil
}

func (c *testClient) ScanRegions(ctx context.Context, key, endKey []byte, limit int) ([]*RegionInfo, error) {
	return nil, nil
}

func (c *testClient) GetPlacementRule(ctx context.Context, groupID, ruleID string) (r placement.Rule, err error) {
	return
}

func (c *testClient) SetPlacementRule(ctx context.Context, rule placement.Rule) error {
	return nil
}

func (c *testClient) DeletePlacementRule(ctx context.Context, groupID, ruleID string) error {
	return nil
}

func (c *testClient) SetStoresLabel(ctx context.Context, stores []uint64, labelKey, labelValue string) error {
	return nil
}

// region: [, aay), [aay, bba), [bba, bbh), [bbh, cca), [cca, )
// range: [aaa, aae), [aae, aaz), [ccd, ccf), [ccf, ccj)
// rewrite rules: aa -> xx,  cc -> bb
// expected regions after split:
// 		[, aay), [aay, bb), [bb, bba), [bba, bbf), [bbf, bbh), [bbh, cca), [cca, xx), [xx, xxe), [xxe, xxz), [xxz, )
func TestSplit(t *testing.T) {
	client := initTestClient()
	ranges := initRanges()
	rewriteRules := initRewriteRules()
	regionSplitter := NewRegionSplitter(client)

	ctx := context.Background()
	length := 0
	err := regionSplitter.Split(ctx, ranges, rewriteRules,
		func(rg *Range) {
			length++
			if rg == nil {
				t.Fatal("nil Range")
			}
		})
	if length != 4+2 { // Ranges + Rules
		t.Fatal("wrong length", length)
	}
	if err != nil {
		t.Fatalf("split regions failed: %v", err)
	}
	regions := client.GetAllRegions()
	if !validateRegions(regions) {
		for _, region := range regions {
			t.Errorf("region: %v", region.Region)
		}
		t.Fatalf("get wrong result")
	}
}

// region: [, aay), [aay, bba), [bba, bbh), [bbh, cca), [cca, )
func initTestClient() *testClient {
	peers := make([]*metapb.Peer, 1)
	peers[0] = &metapb.Peer{
		Id:      1,
		StoreId: 1,
	}
	keys := [6]string{"", "aay", "bba", "bbh", "cca", ""}
	regions := make(map[uint64]*RegionInfo)
	for i := uint64(1); i < 6; i++ {
		startKey := []byte(keys[i-1])
		if len(startKey) != 0 {
			startKey = codec.EncodeBytes([]byte{}, startKey)
		}
		endKey := []byte(keys[i])
		if len(endKey) != 0 {
			endKey = codec.EncodeBytes([]byte{}, endKey)
		}
		regions[i] = &RegionInfo{
			Region: &metapb.Region{
				Id:       i,
				Peers:    peers,
				StartKey: startKey,
				EndKey:   endKey,
			},
		}
	}
	stores := make(map[uint64]*metapb.Store)
	stores[1] = &metapb.Store{
		Id: 1,
	}
	return newTestClient(stores, regions, 6)
}

// range: [aaa, aae), [aae, aaz), [ccd, ccf), [ccf, ccj)
func initRanges() []Range {
	var ranges [4]Range
	ranges[0] = Range{
		StartKey: []byte("aaa"),
		EndKey:   []byte("aae"),
	}
	ranges[1] = Range{
		StartKey: []byte("aae"),
		EndKey:   []byte("aaz"),
	}
	ranges[2] = Range{
		StartKey: []byte("ccd"),
		EndKey:   []byte("ccf"),
	}
	ranges[3] = Range{
		StartKey: []byte("ccf"),
		EndKey:   []byte("ccj"),
	}
	return ranges[:]
}

func initRewriteRules() *RewriteRules {
	var rules [2]*import_sstpb.RewriteRule
	rules[0] = &import_sstpb.RewriteRule{
		OldKeyPrefix: []byte("aa"),
		NewKeyPrefix: []byte("xx"),
	}
	rules[1] = &import_sstpb.RewriteRule{
		OldKeyPrefix: []byte("cc"),
		NewKeyPrefix: []byte("bb"),
	}
	return &RewriteRules{
		Table: rules[:],
		Data:  rules[:],
	}
}

// expected regions after split:
// 		[, aay), [aay, bb), [bb, bba), [bba, bbf), [bbf, bbh), [bbh, cca), [cca, xx), [xx, xxe), [xxe, xxz), [xxz, )
func validateRegions(regions map[uint64]*RegionInfo) bool {
	keys := [11]string{"", "aay", "bb", "bba", "bbf", "bbh", "cca", "xx", "xxe", "xxz", ""}
	if len(regions) != 10 {
		return false
	}
FindRegion:
	for i := 1; i < 11; i++ {
		for _, region := range regions {
			startKey := []byte(keys[i-1])
			if len(startKey) != 0 {
				startKey = codec.EncodeBytes([]byte{}, startKey)
			}
			endKey := []byte(keys[i])
			if len(endKey) != 0 {
				endKey = codec.EncodeBytes([]byte{}, endKey)
			}
			if bytes.Equal(region.Region.GetStartKey(), startKey) &&
				bytes.Equal(region.Region.GetEndKey(), endKey) {
				continue FindRegion
			}
		}
		return false
	}
	return true
}
