package region_util

import (
	"bytes"
	"context"
	"testing"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
)

type testClient struct {
	stores       map[uint64]*metapb.Store
	regions      map[uint64]*RegionInfo
	nextRegionID uint64
}

func NewTestClient(stores map[uint64]*metapb.Store, regions map[uint64]*RegionInfo, nextRegionID uint64) Client {
	return &testClient{
		stores:       stores,
		regions:      regions,
		nextRegionID: nextRegionID,
	}
}

func (c *testClient) GetStore(ctx context.Context, storeID uint64) (*metapb.Store, error) {
	store, ok := c.stores[storeID]
	if !ok {
		return nil, errors.Errorf("store not found")
	}
	return store, nil
}

func (c *testClient) GetRegion(ctx context.Context, key []byte) (*RegionInfo, error) {
	for _, region := range c.regions {
		if bytes.Compare(key, region.Region.StartKey) >= 0 &&
			(len(region.Region.EndKey) == 0 || bytes.Compare(key, region.Region.EndKey) <= 0) {
			return region, nil
		}
	}
	return nil, errors.Errorf("region not found")
}

func (c *testClient) GetRegionByID(ctx context.Context, regionID uint64) (*RegionInfo, error) {
	region, ok := c.regions[regionID]
	if !ok {
		return nil, errors.Errorf("region not found")
	}
	return region, nil
}

func (c *testClient) SplitRegion(ctx context.Context, regionInfo *RegionInfo, key []byte) (*RegionInfo, error) {
	var target *RegionInfo
	for _, region := range c.regions {
		if bytes.Compare(key, region.Region.StartKey) >= 0 && bytes.Compare(key, region.Region.EndKey) <= 0 {
			target = region
		}
	}
	if target == nil {
		return nil, errors.Errorf("region not found")
	}
	newRegion := &RegionInfo{
		Region: &metapb.Region{
			Peers:    target.Region.Peers,
			Id:       c.nextRegionID,
			StartKey: target.Region.StartKey,
			EndKey:   key,
		},
	}
	c.regions[c.nextRegionID] = newRegion
	c.nextRegionID++
	target.Region.StartKey = key
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

// region: [, e), [e, m), [m, p), [p, t), [t, )
// range: [, e), [e, m), [m, p), [p, t), [t, )
// range size: 10
// limit size: 10
// expected region: [, e), [e, m), [m, p), [p, t), [t, )
// expected range: [, e), [e, m), [m, p), [p, t), [t, )
func TestSplit1(t *testing.T) {
}

// region: [, e), [e, m), [m, p), [p, t), [t, )
// range: [, c), [c, f), [f, q), [q, r) [r, s), [s, w), [w, z)
// range size: 10
// limit size: 10
// expected region: [, c), [c, e), [e, f), [f, m), [m, p), [p, q), [q, r), [r, s), [s, t), [t, w), [w, z), [z, )
// expected range: [, c), [c, f), [f, q), [r, s), [s, w), [w, z)
func TestSplit2(t *testing.T) {
}

// region: [, e), [e, m), [m, p), [p, t), [t, )
// range: [, c), [c, f), [f, q), [q, r), [r, s), [s, w), [w, z)
// range size: 2, 8, 5, 10, 3, 4, 2
// limit size: 10
// expected region: [, e), [e, f), [f, m), [m, p), [p, q), [q, r), [r, s), [s, t), [t, z), [z, )
// expected range: [, f), [f, q), [q, r), [r, s), [s, z)
func TestSplit3(t *testing.T) {
}

// region: [, e), [e, m), [m, p), [p, t), [t, )
func InitTestClient() Client {
	peers := make([]*metapb.Peer, 0, 1)
	peers = append(peers, &metapb.Peer{
		Id:      1,
		StoreId: 1,
	})
	regions := make(map[uint64]*RegionInfo)
	regions[1] = &RegionInfo{
		Region: &metapb.Region{
			Id:     1,
			Peers:  peers,
			EndKey: []byte("e"),
		},
	}
	regions[2] = &RegionInfo{
		Region: &metapb.Region{
			Id:       2,
			Peers:    peers,
			StartKey: []byte("e"),
			EndKey:   []byte("m"),
		},
	}
	regions[3] = &RegionInfo{
		Region: &metapb.Region{
			Id:       3,
			Peers:    peers,
			StartKey: []byte("m"),
			EndKey:   []byte("p"),
		},
	}
	regions[4] = &RegionInfo{
		Region: &metapb.Region{
			Id:       4,
			Peers:    peers,
			StartKey: []byte("p"),
			EndKey:   []byte("t"),
		},
	}
	regions[5] = &RegionInfo{
		Region: &metapb.Region{
			Id:       5,
			Peers:    peers,
			StartKey: []byte("t"),
		},
	}
	stores := make(map[uint64]*metapb.Store)
	stores[1] = &metapb.Store{
		Id: 1,
	}
	return NewTestClient(stores, regions, 6)
}
