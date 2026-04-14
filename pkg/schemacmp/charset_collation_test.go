package schemacmp_test

import (
	. "github.com/pingcap/check"

	. "github.com/pingcap/tidb-tools/pkg/schemacmp"
)

type charsetCollationSuite struct{}

var _ = Suite(&charsetCollationSuite{})

func (*charsetCollationSuite) TestCharsetCompareUsesFamily(c *C) {
	// Ensure Compare only depends on the normalized kind, not the original input string.
	cmp, err := Charset("UTF8").Compare(Charset("utf8"))
	c.Assert(err, IsNil)
	c.Assert(cmp, Equals, 0)

	cmp, err = Charset("UTF8MB3").Compare(Charset("utf8"))
	c.Assert(err, IsNil)
	c.Assert(cmp, Equals, 0)

	// Ensure error messages keep the original values.
	_, err = Charset("uTF8").Compare(Charset("GBK"))
	c.Assert(err, ErrorMatches, `incompatible mysql charset \(uTF8 vs GBK\)`)

	cmp, err = Charset("latin1").Compare(Charset("utf8mb4"))
	c.Assert(err, IsNil)
	c.Assert(cmp, Equals, -1)

	cmp, err = Charset("utf8mb4").Compare(Charset("utf8mb3"))
	c.Assert(err, IsNil)
	c.Assert(cmp, Equals, 1)
}

func (*charsetCollationSuite) TestCollationCompareUsesFamily(c *C) {
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
	c.Assert(err, ErrorMatches, `incompatible mysql collation \(UTF8_BIN vs GBK_BIN\)`)

	cmp, err = Collation("utf8mb4_general_ci").Compare(Collation("utf8_general_ci"))
	c.Assert(err, IsNil)
	c.Assert(cmp, Equals, 1)

	_, err = Collation("utf8mb4_general_ci").Compare(Collation("utf8mb4_0900_ai_ci"))
	c.Assert(err, ErrorMatches, `incompatible mysql collation \(utf8mb4_general_ci vs utf8mb4_0900_ai_ci\)`)

	_, err = Collation("other_cs_bin").Compare(Collation("other_cs_ci"))
	c.Assert(err, ErrorMatches, `incompatible mysql collation \(other_cs_bin vs other_cs_ci\)`)
}
