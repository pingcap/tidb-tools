package check

import (
	tc "github.com/pingcap/check"
)

func (t *testCheckSuite) TestVersionComparsion(c *tc.C) {
	// test normal cases
	cases := []struct {
		rawVersion     string
		ge, gt, lt, le bool
	}{
		{"0.0.0", false, false, true, true},
		{"5.5.0", false, false, true, true},
		{"5.6.0", true, false, true, true},
		{"5.7.0", true, true, true, true},
		{"5.8.0", true, true, true, true},
		{"8.0.1", true, true, true, true},
		{"255.255.255", true, true, false, true},
	}

	var (
		version MySQLVersion
		err     error
	)
	for _, cs := range cases {
		version, err = toMySQLVersion(cs.rawVersion)
		c.Assert(err, tc.IsNil)

		c.Assert(version.Ge(SupportedVersion["mysql"].Min), tc.Equals, cs.ge)
		c.Assert(version.Gt(SupportedVersion["mysql"].Min), tc.Equals, cs.gt)
		c.Assert(version.Lt(SupportedVersion["mysql"].Max), tc.Equals, cs.lt)
		c.Assert(version.Le(SupportedVersion["mysql"].Max), tc.Equals, cs.le)
	}

	c.Assert(version.Lt(SupportedVersion["mariadb"].Max), tc.Equals, false)
	c.Assert(version.Le(SupportedVersion["mariadb"].Max), tc.Equals, true)
}

func (t *testCheckSuite) TestToVersion(c *tc.C) {
	// test normal cases
	cases := []struct {
		rawVersion      string
		expectedVersion MySQLVersion
		hasError        bool
	}{
		{"", MinVersion, true},
		{"1.2.3.4", MinVersion, true},
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
