package check

import (
	tc "github.com/pingcap/check"
)

func (t *testCheckSuite) TestVersionComparsion(c *tc.C) {
	// test normal cases
	cases := []struct {
		rawVersion     string
		be, bg, lt, le bool
	}{
		{"5.5.0", false, false, true, true},
		{"5.6.0", true, false, true, true},
		{"5.7.0", true, true, true, true},
		{"5.8.0", true, true, false, true},
		{"5.8.1", true, true, false, false},
	}

	for _, cs := range cases {
		version, err := toMySQLVersion(cs.rawVersion)
		c.Assert(err, tc.IsNil)

		c.Assert(version.Be(SupportedVersion["mysql"].Min), tc.Equals, cs.be)
		c.Assert(version.Bg(SupportedVersion["mysql"].Min), tc.Equals, cs.bg)
		c.Assert(version.Lt(SupportedVersion["mysql"].Max), tc.Equals, cs.lt)
		c.Assert(version.Le(SupportedVersion["mysql"].Max), tc.Equals, cs.le)
		// test unlimit version
		c.Assert(version.Be(UnlimitVersion), tc.Equals, true)
		c.Assert(version.Bg(UnlimitVersion), tc.Equals, true)
		c.Assert(version.Lt(UnlimitVersion), tc.Equals, true)
		c.Assert(version.Le(UnlimitVersion), tc.Equals, true)
	}
}

func (t *testCheckSuite) TestToVersion(c *tc.C) {
	// test normal cases
	cases := []struct {
		rawVersion      string
		expectedVersion MySQLVersion
		hasError        bool
	}{
		{"", UnlimitVersion, true},
		{"1.2.3.4", UnlimitVersion, true},
		{"1.x.3", MySQLVersion{1, 0, 0}, true},
		{"5.7.18-log", MySQLVersion{5, 7, 18}, false},
		{"5.5.50-MariaDB-1~wheezy", MySQLVersion{5, 5, 50}, false},
		{"5.7.19-17-log", MySQLVersion{5, 7, 19}, false},
		{"5.7.18-log", MySQLVersion{5, 7, 18}, false},
		{"5.7.16-log", MySQLVersion{5, 7, 16}, false},
	}

	for _, cs := range cases {
		version, err := toMySQLVersion(cs.rawVersion)
		c.Assert(version, tc.Equals, cs.expectedVersion)
		c.Assert(err != nil, tc.Equals, cs.hasError)
	}
}

func (t *testCheckSuite) TestIsMariaDB(c *tc.C) {
	c.Assert(IsMariaDB("5.5.50-MariaDB-1~wheezy"), tc.IsTrue)
	c.Assert(IsMariaDB("5.7.19-17-log"), tc.IsFalse)
}
