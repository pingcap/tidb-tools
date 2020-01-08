package restore_util

import (
	"bytes"
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/tidb/tablecodec"
)

func TestRestoreUtil(t *testing.T) {
	TestingT(t)
}

type testRestoreUtilSuite struct{}

var _ = Suite(&testRestoreUtilSuite{})

type rangeEquals struct {
	*CheckerInfo
}

var RangeEquals Checker = &rangeEquals{
	&CheckerInfo{Name: "RangeEquals", Params: []string{"obtained", "expected"}},
}

func (checker *rangeEquals) Check(params []interface{}, names []string) (result bool, error string) {
	obtained := params[0].([]Range)
	expected := params[1].([]Range)
	if len(obtained) != len(expected) {
		return false, ""
	}
	for i := range obtained {
		if !bytes.Equal(obtained[i].StartKey, expected[i].StartKey) ||
			!bytes.Equal(obtained[i].EndKey, expected[i].EndKey) {
			return false, ""
		}
	}
	return true, ""
}

func (s *testRestoreUtilSuite) TestSortRange(c *C) {
	dataRules := []*import_sstpb.RewriteRule{
		{OldKeyPrefix: tablecodec.GenTableRecordPrefix(1), NewKeyPrefix: tablecodec.GenTableRecordPrefix(4)},
		{OldKeyPrefix: tablecodec.GenTableRecordPrefix(2), NewKeyPrefix: tablecodec.GenTableRecordPrefix(5)},
	}
	rewriteRules := &RewriteRules{
		Table: make([]*import_sstpb.RewriteRule, 0),
		Data:  dataRules,
	}
	ranges1 := []Range{
		{append(tablecodec.GenTableRecordPrefix(1), []byte("aaa")...), append(tablecodec.GenTableRecordPrefix(1), []byte("bbb")...)},
	}
	rs1, err := sortRanges(ranges1, rewriteRules)
	c.Assert(err, IsNil, Commentf("sort range1 failed: %v", err))
	c.Assert(rs1, RangeEquals, []Range{
		{append(tablecodec.GenTableRecordPrefix(4), []byte("aaa")...), append(tablecodec.GenTableRecordPrefix(4), []byte("bbb")...)},
	})

	ranges2 := []Range{
		{append(tablecodec.GenTableRecordPrefix(1), []byte("aaa")...), append(tablecodec.GenTableRecordPrefix(2), []byte("bbb")...)},
	}
	_, err = sortRanges(ranges2, rewriteRules)
	c.Assert(err, ErrorMatches, ".*table id does not match.*")

	ranges3 := initRanges()
	rewriteRules1 := initRewriteRules()
	rs3, err := sortRanges(ranges3, rewriteRules1)
	c.Assert(err, IsNil, Commentf("sort range1 failed: %v", err))
	c.Assert(rs3, RangeEquals, []Range{
		{[]byte("bbd"), []byte("bbf")},
		{[]byte("bbf"), []byte("bbj")},
		{[]byte("xxa"), []byte("xxe")},
		{[]byte("xxe"), []byte("xxz")},
	})
}
