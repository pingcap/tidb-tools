package restore_util

import (
	"bytes"
	"fmt"

	"github.com/google/btree"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/tablecodec"
	"go.uber.org/zap"
)

// Range represents a range of keys.
type Range struct {
	StartKey []byte
	EndKey   []byte
}

// String formats a range to a string
func (r *Range) String() string {
	return fmt.Sprintf("[%x %x]", r.StartKey, r.EndKey)
}

// Less compares a range with a btree.Item
func (r *Range) Less(than btree.Item) bool {
	t := than.(*Range)
	return len(r.EndKey) != 0 && bytes.Compare(r.EndKey, t.StartKey) <= 0
}

// contains returns if a key is included in the range.
func (r *Range) contains(key []byte) bool {
	start, end := r.StartKey, r.EndKey
	return bytes.Compare(key, start) >= 0 &&
		(len(end) == 0 || bytes.Compare(key, end) < 0)
}

// sortRanges checks if the range overlapped and sort them
func sortRanges(ranges []Range, rewriteRules *RewriteRules) ([]Range, error) {
	rangeTree := NewRangeTree()
	for _, rg := range ranges {
		if rewriteRules != nil {
			startID := tablecodec.DecodeTableID(rg.StartKey)
			endID := tablecodec.DecodeTableID(rg.EndKey)
			var rule *import_sstpb.RewriteRule
			if startID == endID {
				rg.StartKey, rule = replacePrefix(rg.StartKey, rewriteRules)
				if rule == nil {
					log.Warn("cannot find rewrite rule", zap.Binary("key", rg.StartKey))
				} else {
					log.Debug(
						"rewrite start key",
						zap.Binary("key", rg.StartKey),
						zap.Stringer("rule", rule))
				}
				rg.EndKey, rule = replacePrefix(rg.EndKey, rewriteRules)
				if rule == nil {
					log.Warn("cannot find rewrite rule", zap.Binary("key", rg.EndKey))
				} else {
					log.Debug(
						"rewrite end key",
						zap.Binary("key", rg.EndKey),
						zap.Stringer("rule", rule))
				}
			} else {
				log.Warn("table id does not match",
					zap.Binary("startKey", rg.StartKey),
					zap.Binary("endKey", rg.EndKey),
					zap.Int64("startID", startID),
					zap.Int64("endID", endID))
				return nil, errors.New("table id does not match")
			}
		}
		if out := rangeTree.InsertRange(rg); out != nil {
			return nil, errors.Errorf("ranges overlapped: %s, %s", out, rg)
		}
	}
	sortedRanges := make([]Range, 0, len(ranges))
	rangeTree.Ascend(func(rg *Range) bool {
		if rg == nil {
			return false
		}
		sortedRanges = append(sortedRanges, *rg)
		return true
	})
	return sortedRanges, nil
}

// RangeTree stores the ranges in an orderly manner.
// All the ranges it stored do not overlap.
type RangeTree struct {
	tree *btree.BTree
}

// NewRangeTree returns a new RangeTree.
func NewRangeTree() *RangeTree {
	return &RangeTree{tree: btree.New(32)}
}

// Find returns nil or a range in the range tree
func (rt *RangeTree) Find(key []byte) *Range {
	var ret *Range
	r := &Range{
		StartKey: key,
	}
	rt.tree.DescendLessOrEqual(r, func(i btree.Item) bool {
		ret = i.(*Range)
		return false
	})
	if ret == nil || !ret.contains(key) {
		return nil
	}
	return ret
}

// InsertRanges inserts ranges into the range tree.
// it returns true if all ranges inserted successfully.
// it returns false if there are some overlapped ranges.
func (rt *RangeTree) InsertRange(rg Range) btree.Item {
	return rt.tree.ReplaceOrInsert(&rg)
}

// RangeIterator allows callers of Ascend to iterate in-order over portions of
// the tree. When this function returns false, iteration will stop and the
// associated Ascend function will immediately return.
type RangeIterator func(rg *Range) bool

// Ascend calls the iterator for every value in the tree within [first, last],
// until the iterator returns false.
func (rt *RangeTree) Ascend(iterator RangeIterator) {
	rt.tree.Ascend(func(i btree.Item) bool {
		return iterator(i.(*Range))
	})
}

// RegionInfo includes a region and the leader of the region.
type RegionInfo struct {
	Region *metapb.Region
	Leader *metapb.Peer
}

type RewriteRules struct {
	Table []*import_sstpb.RewriteRule
	Data  []*import_sstpb.RewriteRule
}
