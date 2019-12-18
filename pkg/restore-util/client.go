package restore_util

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"path"
	"strconv"
	"strings"
	"sync"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/kvproto/pkg/tikvpb"
	pd "github.com/pingcap/pd/client"
	"github.com/pingcap/pd/server/schedule/placement"
	"google.golang.org/grpc"
)

// Client is an external client used by RegionSplitter.
type Client interface {
	// GetStore gets a store by a store id.
	GetStore(ctx context.Context, storeID uint64) (*metapb.Store, error)
	// GetRegion gets a region which includes a specified key.
	GetRegion(ctx context.Context, key []byte) (*RegionInfo, error)
	// GetRegionByID gets a region by a region id.
	GetRegionByID(ctx context.Context, regionID uint64) (*RegionInfo, error)
	// SplitRegion splits a region from a key, if key is not included in the region, it will return nil.
	// note: the key should not be encoded
	SplitRegion(ctx context.Context, regionInfo *RegionInfo, key []byte) (*RegionInfo, error)
	// ScatterRegion scatters a specified region.
	ScatterRegion(ctx context.Context, regionInfo *RegionInfo) error
	// GetOperator gets the status of operator of the specified region.
	GetOperator(ctx context.Context, regionID uint64) (*pdpb.GetOperatorResponse, error)
	// ScanRegion gets a list of regions, starts from the region that contains key.
	// Limit limits the maximum number of regions returned.
	ScanRegions(ctx context.Context, key, endKey []byte, limit int) ([]*RegionInfo, error)
	// GetPlacementRule loads a placement rule from PD.
	GetPlacementRule(ctx context.Context, groupID, ruleID string) (placement.Rule, error)
	// SetPlacementRule insert or update a placement rule to PD.
	SetPlacementRule(ctx context.Context, rule placement.Rule) error
	// DeletePlacementRule removes a placement rule from PD.
	DeletePlacementRule(ctx context.Context, groupID, ruleID string) error
	// SetStoreLabel add or update specified label of stores. If labelValue
	// is empty, it clears the label.
	SetStoresLabel(ctx context.Context, stores []uint64, labelKey, labelValue string) error
}

// pdClient is a wrapper of pd client, can be used by RegionSplitter.
type pdClient struct {
	mu         sync.Mutex
	client     pd.Client
	storeCache map[uint64]*metapb.Store
}

// NewClient returns a client used by RegionSplitter.
func NewClient(client pd.Client) Client {
	return &pdClient{
		client:     client,
		storeCache: make(map[uint64]*metapb.Store),
	}
}

func (c *pdClient) GetStore(ctx context.Context, storeID uint64) (*metapb.Store, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	store, ok := c.storeCache[storeID]
	if ok {
		return store, nil
	}
	store, err := c.client.GetStore(ctx, storeID)
	if err != nil {
		return nil, err
	}
	c.storeCache[storeID] = store
	return store, nil

}

func (c *pdClient) GetRegion(ctx context.Context, key []byte) (*RegionInfo, error) {
	region, leader, err := c.client.GetRegion(ctx, key)
	if err != nil {
		return nil, err
	}
	if region == nil {
		return nil, errors.Errorf("region not found: key=%x", key)
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
	if region == nil {
		return nil, errors.Errorf("region not found: id=%d", regionID)
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
		if len(regionInfo.Region.Peers) == 0 {
			return nil, errors.New("region does not have peer")
		}
		peer = regionInfo.Region.Peers[0]
	}
	storeID := peer.GetStoreId()
	store, err := c.GetStore(ctx, storeID)
	if err != nil {
		return nil, err
	}
	conn, err := grpc.Dial(store.GetAddress(), grpc.WithInsecure())
	defer conn.Close()
	if err != nil {
		return nil, err
	}
	client := tikvpb.NewTikvClient(conn)
	resp, err := client.SplitRegion(ctx, &kvrpcpb.SplitRegionRequest{
		Context: &kvrpcpb.Context{
			RegionId:    regionInfo.Region.Id,
			RegionEpoch: regionInfo.Region.RegionEpoch,
			Peer:        peer,
		},
		SplitKey: key,
	})
	if err != nil {
		return nil, err
	}
	if resp.RegionError != nil {
		return nil, errors.Errorf("split region failed: region=%v, key=%x, err=%v", regionInfo.Region, key, resp.RegionError)
	}

	// Assume the new region is the left one.
	newRegion := resp.GetLeft()
	if newRegion == nil {
		regions := resp.GetRegions()
		for _, r := range regions {
			if bytes.Equal(r.GetStartKey(), regionInfo.Region.GetStartKey()) {
				newRegion = r
				break
			}
		}
	}
	if newRegion == nil {
		return nil, errors.New("split region failed: new region is nil")
	}
	var leader *metapb.Peer
	// Assume the leaders will be at the same store.
	if regionInfo.Leader != nil {
		for _, p := range newRegion.GetPeers() {
			if p.GetStoreId() == regionInfo.Leader.GetStoreId() {
				leader = p
				break
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

func (c *pdClient) ScanRegions(ctx context.Context, key, endKey []byte, limit int) ([]*RegionInfo, error) {
	regions, leaders, err := c.client.ScanRegions(ctx, key, endKey, limit)
	if err != nil {
		return nil, err
	}
	regionInfos := make([]*RegionInfo, 0, len(regions))
	for i := range regions {
		regionInfos = append(regionInfos, &RegionInfo{
			Region: regions[i],
			Leader: leaders[i],
		})
	}
	return regionInfos, nil
}

func (c *pdClient) GetPlacementRule(ctx context.Context, groupID, ruleID string) (placement.Rule, error) {
	var rule placement.Rule
	addr := c.getPDAPIAddr()
	if addr == "" {
		return rule, errors.New("failed to add stores labels: no leader")
	}
	req, _ := http.NewRequestWithContext(ctx, "GET", path.Join(addr, "pd/api/v1/config/rule", groupID, ruleID), nil)
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return rule, errors.WithStack(err)
	}
	b, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return rule, errors.WithStack(err)
	}
	res.Body.Close()
	err = json.Unmarshal(b, &rule)
	if err != nil {
		return rule, errors.WithStack(err)
	}
	return rule, nil
}

func (c *pdClient) SetPlacementRule(ctx context.Context, rule placement.Rule) error {
	addr := c.getPDAPIAddr()
	if addr == "" {
		return errors.New("failed to add stores labels: no leader")
	}
	m, _ := json.Marshal(rule)
	req, _ := http.NewRequestWithContext(ctx, "POST", path.Join(addr, "pd/api/v1/config/rule"), bytes.NewReader(m))
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return errors.WithStack(err)
	}
	ioutil.ReadAll(res.Body)
	res.Body.Close()
	return nil
}

func (c *pdClient) DeletePlacementRule(ctx context.Context, groupID, ruleID string) error {
	addr := c.getPDAPIAddr()
	if addr == "" {
		return errors.New("failed to add stores labels: no leader")
	}
	req, _ := http.NewRequestWithContext(ctx, "DELETE", path.Join(addr, "pd/api/v1/config/rule", groupID, ruleID), nil)
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return errors.WithStack(err)
	}
	ioutil.ReadAll(res.Body)
	res.Body.Close()
	return nil
}

func (c *pdClient) SetStoresLabel(ctx context.Context, stores []uint64, labelKey, labelValue string) error {
	b := []byte(fmt.Sprintf(`{"%s": "%s"}`, labelKey, labelValue))
	addr := c.getPDAPIAddr()
	if addr == "" {
		return errors.New("failed to add stores labels: no leader")
	}
	for _, id := range stores {
		req, _ := http.NewRequestWithContext(ctx, "POST", path.Join(addr, "pd/api/v1/store", strconv.FormatUint(id, 10), "label"), bytes.NewReader(b))
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			return errors.WithStack(err)
		}
		ioutil.ReadAll(res.Body)
		res.Body.Close()

	}
	return nil
}

func (c *pdClient) getPDAPIAddr() string {
	addr := c.client.GetLeaderAddr()
	if addr != "" && !strings.HasPrefix(addr, "http") {
		addr = "http://" + addr
	}
	return addr
}
