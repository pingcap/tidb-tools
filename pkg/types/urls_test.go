package types

import (
	"strings"
	"testing"

	. "github.com/pingcap/check"
)

func Test(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testTypesSuite{})

type testTypesSuite struct{}

func (s *testTypesSuite) TestURLs(c *C) {
	urlstrs := []string{
		"http://192.168.199.111:1080",
		"http://hostname:9000",
		"http://www.google.com:12306",
	}

	urls, err := NewURLs(urlstrs)
	c.Assert(err, IsNil)
	c.Assert(urls.String(), Equals, strings.Join(urlstrs, ","))
}

func (s *testTypesSuite) TestBadURLs(c *C) {
	badurls := [][]string{
		{"http://192.168.199.111"},
		{"127.0.0.1:1080"},
		{"http://192.168.199.112:8080/api/v1"},
	}

	for _, badurl := range badurls {
		_, err := NewURLs(badurl)
		c.Assert(err, NotNil)
	}
}
