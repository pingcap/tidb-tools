package restore_util

import (
	"bytes"
	"testing"
)

func TestRangeTreeNormal(t *testing.T) {
	ranges := initRanges()
	rangeTree := NewRangeTree()
	for _, rg := range ranges {
		if out := rangeTree.InsertRange(rg); out != nil {
			t.Fatalf("insert range failed: %s %s", out.(*Range).String(), rg.String())
		}
	}
	resRanges := make([]Range, 0)
	rangeTree.Ascend(func(rg *Range) bool {
		if rg == nil {
			return false
		}
		resRanges = append(resRanges, Range{
			StartKey: rg.StartKey,
			EndKey:   rg.EndKey,
		})
		return true
	})
	if len(ranges) != len(resRanges) {
		t.Fatalf("some inserted ranges missed: rg=%v res=%v", ranges, resRanges)
	}
	for i, rg := range ranges {
		res := resRanges[i]
		if !bytes.Equal(rg.StartKey, res.StartKey) || !bytes.Equal(rg.EndKey, res.EndKey) {
			t.Fatalf("some inserted ranges missed: rg=%v res=%v", ranges, resRanges)
		}
	}
}

func TestRangeOverlapped(t *testing.T) {
	rg1 := Range{
		StartKey: []byte("aaa"),
		EndKey:   []byte("aaz"),
	}
	rg2 := Range{
		StartKey: []byte("aab"),
		EndKey:   []byte("aag"),
	}
	rangeTree := NewRangeTree()
	if out := rangeTree.InsertRange(rg1); out != nil {
		t.Fatalf("insert range failed: %s %s", out.(*Range).String(), rg1.String())
	}
	if out := rangeTree.InsertRange(rg2); out == nil {
		t.Fatalf("overlapping not detected: %s %s", rg1.String(), rg2.String())
	}
}
