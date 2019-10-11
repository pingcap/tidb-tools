package restore_util

import (
	"bytes"

	"github.com/google/btree"
	"github.com/pingcap/kvproto/pkg/metapb"
)

type Range struct {
	StartKey []byte
	EndKey   []byte
}

// Less compares a range with a btree.Item
func (r *Range) Less(than btree.Item) bool {
	t := than.(*Range)
	return len(r.EndKey) != 0 && bytes.Compare(r.EndKey, t.StartKey) <= 0
}

func (r *Range) contains(key []byte) bool {
	start, end := r.StartKey, r.EndKey
	return bytes.Compare(key, start) >= 0 &&
		(len(end) == 0 || bytes.Compare(key, end) < 0)
}

type RangeTree struct {
	tree *btree.BTree
}

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

// InsertRanges inserts ranges into the range tree
// return true if all ranges inserted successfully
// return false if there are some overlapped ranges
func (rt *RangeTree) InsertRange(rg Range) bool {
	if rt.tree.ReplaceOrInsert(&rg) != nil {
		return false
	}
	return true
}

type RangeIterator func(rg *Range) bool

func (rt *RangeTree) Ascend(iterator RangeIterator) {
	rt.tree.Ascend(func(i btree.Item) bool {
		return iterator(i.(*Range))
	})
}

// RegionInfo includes a region and the meta of its leader.
type RegionInfo struct {
	Region *metapb.Region
	Leader *metapb.Peer
}
