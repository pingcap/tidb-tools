package restore_util

import (
	"bytes"
	"fmt"

	"github.com/google/btree"
	"github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/kvproto/pkg/metapb"
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
func (rt *RangeTree) InsertRange(rg Range) bool {
	return rt.tree.ReplaceOrInsert(&rg) == nil
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
