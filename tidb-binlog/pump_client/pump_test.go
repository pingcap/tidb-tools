// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package client

import (
	"math"

	. "github.com/pingcap/check"
)

var _ = Suite(&testPumpClient{})

type testPumpClient struct{}

func (t *testPumpClient) TestPumpState(c *C) {
	s := &realPumpState{}

	c.Assert(s.failedTS, Equals, int64(0))
	c.Assert(s.isAvaliable(), IsTrue)

	s.markUnAvailable()
	c.Assert(s.isAvaliable(), IsFalse)

	s.markAvailable()
	c.Assert(s.isAvaliable(), IsTrue)

	c.Assert(s.failedTS, Equals, int64(0))
	s.updateFailedTS(1)
	c.Assert(s.failedTS, Equals, int64(1))
	c.Assert(s.isAvaliable(), IsTrue)

	s.updateFailedTS(2)
	c.Assert(s.failedTS, Equals, int64(1))
	c.Assert(s.isAvaliable(), IsTrue)

	s.updateFailedTS(301)
	c.Assert(s.failedTS, Equals, int64(1))
	c.Assert(s.isAvaliable(), IsTrue)

	s.updateFailedTS(302)
	c.Assert(s.failedTS, Equals, int64(math.MaxInt64))
	c.Assert(s.isAvaliable(), IsFalse)

	s.markAvailable()
	c.Assert(s.isAvaliable(), IsTrue)
	c.Assert(s.failedTS, Equals, int64(0))
}
