package region_util

import (
	"bytes"
	"context"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/metapb"
)

type Range struct {
	StartKey []byte
	EndKey   []byte
	Size     uint64
}

// RegionInfo includes a region and the meta of its leader.
type RegionInfo struct {
	Region *metapb.Region
	Leader *metapb.Peer
}

type RangeContext struct {
	client   Client
	regionInfo *RegionInfo
	limitSize  uint64
	rawSize    uint64
}

func NewRangeContext(client Client, limitSize uint64) *RangeContext {
	return &RangeContext{
		client:  client,
		limitSize: limitSize,
	}
}

// Add adds the size to the counter of the range size.
func (rc *RangeContext) Add(size uint64) {
	rc.rawSize += size
}

// Reset resets the counter of the range size, and updates to the next region if it's necessary.
func (rc *RangeContext) Reset(ctx context.Context, key []byte) error {
	rc.rawSize = 0
	if rc.regionInfo.Region != nil {
		if beforeEnd(key, rc.regionInfo.Region.GetEndKey()) {
			// Still belongs in this region, do not need to update.
			return nil
		}
	}
	regionInfo, err := rc.client.GetRegion(ctx, key)
	if err != nil {
		return errors.Trace(err)
	}
	rc.regionInfo = regionInfo

	return nil
}

// RawSize returns the current value of the counter of the range size.
func (rc *RangeContext) RawSize() uint64 {
	return rc.rawSize
}

// Check if the range size <= limit size, and if key is before the end of the region.
func (rc *RangeContext) ShouldStopBefore(key []byte) bool {
	if rc.rawSize >= rc.limitSize {
		return true
	}
	if rc.regionInfo.Region != nil {
		return !beforeEnd(key, rc.regionInfo.Region.GetEndKey())
	}
	return false
}

func beforeEnd(key []byte, end []byte) bool {
	return bytes.Compare(key, end) < 0 || len(end) == 0
}
