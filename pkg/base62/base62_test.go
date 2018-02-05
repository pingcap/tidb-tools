package base62

import (
	"testing"

	. "github.com/pingcap/check"
)

func TestClient(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testBase62{})

type testBase62 struct{}

func (*testBase62) TestBase62(c *C) {
	str := Encode(28445, 8)
	c.Assert(str, Equals, "nO700000")

	num := Decode("nO700000")
	c.Assert(num, Equals, int64(28445))
}
