package client

import (
	. "github.com/pingcap/check"
)

type testSelectorSuite struct{}

var _ = Suite(&testSelectorSuite{})

func (t *testSelectorSuite) TestHashTS(c *C) {
	const (
		nPumps = 11
		nTS    = 1024
	)
	counter := [nPumps]int{}
	for i := 0; i < nTS; i++ {
		counter[uint(hashTs(int64(i)))%nPumps]++
	}
	avg := nTS / 11
	for i := 0; i < nPumps; i++ {
		diff := counter[i] - avg
		if diff < 0 {
			diff = -diff
		}
		c.Assert(diff, Less, 300)
	}
}
