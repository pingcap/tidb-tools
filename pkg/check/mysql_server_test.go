package check

import (
	tc "github.com/pingcap/check"
)

func (t *testCheckSuite) TestMysqlVersion(c *tc.C) {
	var versionChecker = &MySQLVersionChecker{}

	cases := []struct {
		rawVersion string
		pass       bool
	}{
		{"5.5.0-log", false},
		{"5.6.0-log", true},
		{"5.7.0-log", true},
		{"5.8.0-log", true}, // although it does not exist
		{"8.0.1-log", true},
		{"8.0.20", true},
		{"5.5.50-MariaDB-1~wheezy", false},
		{"10.1.1-MariaDB-1~wheezy", false},
		{"10.1.2-MariaDB-1~wheezy", true},
		{"10.13.1-MariaDB-1~wheezy", true},
	}

	for _, cs := range cases {
		result := &Result{
			State: StateFailure,
		}
		versionChecker.checkVersion(cs.rawVersion, result)
		c.Assert(result.State == StateSuccess, tc.Equals, cs.pass)
	}
}
