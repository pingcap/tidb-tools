package schemacmp_test

import (
	. "github.com/pingcap/check"

	. "github.com/pingcap/tidb-tools/pkg/schemacmp"
)

type charsetCollationSuite struct{}

var _ = Suite(&charsetCollationSuite{})

func (*charsetCollationSuite) TestCharsetCompareUsesKind(c *C) {
	// Ensure Compare only depends on the normalized kind, not the original input string.
	cmp, err := Charset("UTF8").Compare(Charset("utf8"))
	c.Assert(err, IsNil)
	c.Assert(cmp, Equals, 0)

	cmp, err = Charset("UTF8MB3").Compare(Charset("utf8"))
	c.Assert(err, IsNil)
	c.Assert(cmp, Equals, 0)

	// Ensure error messages keep the original values.
	_, err = Charset("UTF8").Compare(Charset("GBK"))
	c.Assert(err, ErrorMatches, `distinct singletons \(UTF8 vs GBK\)`)
}

func (*charsetCollationSuite) TestCollationCompareUsesKind(c *C) {
	// Ensure Compare only depends on the normalized kind, not the original input string.
	cmp, err := Collation("UTF8_BIN").Compare(Collation("utf8_bin"))
	c.Assert(err, IsNil)
	c.Assert(cmp, Equals, 0)

	cmp, err = Collation("UTF8MB3_BIN").Compare(Collation("utf8_bin"))
	c.Assert(err, IsNil)
	c.Assert(cmp, Equals, 0)

	cmp, err = Collation("LATIN1_BIN").Compare(Collation("utf8mb4_bin"))
	c.Assert(err, IsNil)
	c.Assert(cmp, Equals, -1)

	// Ensure error messages keep the original values.
	_, err = Collation("UTF8_BIN").Compare(Collation("GBK_BIN"))
	c.Assert(err, ErrorMatches, `distinct singletons \(UTF8_BIN vs GBK_BIN\)`)
}
