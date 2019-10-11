package restore_util

import (
	"bytes"
	"context"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/kvproto/pkg/tikvpb"
	pd "github.com/pingcap/pd/client"
	"google.golang.org/grpc"
)

type Client interface {
	GetStore(ctx context.Context, storeID uint64) (*metapb.Store, error)
	GetRegion(ctx context.Context, key []byte) (*RegionInfo, error)
	GetRegionByID(ctx context.Context, regionID uint64) (*RegionInfo, error)
	SplitRegion(ctx context.Context, regionInfo *RegionInfo, key []byte) (*RegionInfo, error)
	ScatterRegion(ctx context.Context, regionInfo *RegionInfo) error
	GetOperator(ctx context.Context, regionID uint64) (*pdpb.GetOperatorResponse, error)
}

type pdClient struct {
	client     pd.Client
	storeCache map[uint64]*metapb.Store
}

func NewClient(client pd.Client) (Client, error) {
	return &pdClient{
		client:     client,
		storeCache: make(map[uint64]*metapb.Store),
	}, nil
}

func (c *pdClient) GetStore(ctx context.Context, storeID uint64) (*metapb.Store, error) {
	store, ok := c.storeCache[storeID]
	if ok {
		return store, nil
	}
	store, err := c.client.GetStore(ctx, storeID)
	if err != nil {
		return nil, err
	}
	return store, nil

}

func (c *pdClient) GetRegion(ctx context.Context, key []byte) (*RegionInfo, error) {
	region, leader, err := c.client.GetRegion(ctx, key)
	if err != nil {
		return nil, err
	}
	return &RegionInfo{
		Region: region,
		Leader: leader,
	}, nil
}

func (c *pdClient) GetRegionByID(ctx context.Context, regionID uint64) (*RegionInfo, error) {
	region, leader, err := c.client.GetRegionByID(ctx, regionID)
	if err != nil {
		return nil, err
	}
	return &RegionInfo{
		Region: region,
		Leader: leader,
	}, nil
}

func (c *pdClient) SplitRegion(ctx context.Context, regionInfo *RegionInfo, key []byte) (*RegionInfo, error) {
	var peer *metapb.Peer
	if regionInfo.Leader != nil {
		peer = regionInfo.Leader
	} else {
		peer = regionInfo.Region.Peers[0]
	}
	reqCtx := &kvrpcpb.Context{
		RegionId:    regionInfo.Region.Id,
		RegionEpoch: regionInfo.Region.RegionEpoch,
		Peer:        peer,
	}
	storeID := peer.GetStoreId()
	store, err := c.GetStore(ctx, storeID)
	if err != nil {
		return nil, err
	}
	req := &kvrpcpb.SplitRegionRequest{
		Context:  reqCtx,
		SplitKeys: [][]byte{key},
	}
	conn, err := grpc.Dial(store.GetAddress(), grpc.WithInsecure())
	client := tikvpb.NewTikvClient(conn)
	resp, err := client.SplitRegion(ctx, req)
	if err != nil {
		return nil, err
	}
	if resp.RegionError != nil {
		return nil, errors.Errorf("split region %d failed, got region error: %v", regionInfo.Region.GetId(), resp.RegionError)
	}

	regions := resp.GetRegions()
	var newRegion *metapb.Region
	for _, r := range regions {
		// Assume the new region is the left one.
		if bytes.Compare(r.GetStartKey(), regionInfo.Region.GetStartKey()) == 0 {
			newRegion = r
		}
	}
	if newRegion == nil {
		return nil, errors.Errorf("split region failed")
	}
	var leader *metapb.Peer
	// Assume the leaders will be at the same store.
	if regionInfo.Leader != nil {
		for _, p := range newRegion.GetPeers() {
			if p.GetStoreId() == regionInfo.Leader.GetStoreId() {
				leader = p
			}
		}
	}
	return &RegionInfo{
		Region: newRegion,
		Leader: leader,
	}, nil
}

func (c *pdClient) ScatterRegion(ctx context.Context, regionInfo *RegionInfo) error {
	return c.client.ScatterRegion(ctx, regionInfo.Region.GetId())
}

func (c *pdClient) GetOperator(ctx context.Context, regionID uint64) (*pdpb.GetOperatorResponse, error) {
	return c.client.GetOperator(ctx, regionID)
}
