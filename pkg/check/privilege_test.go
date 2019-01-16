package check

import (
	"testing"

	tc "github.com/pingcap/check"

	"github.com/pingcap/tidb-tools/pkg/dbutil"
)

func TestClient(t *testing.T) {
	tc.TestingT(t)
}

var _ = tc.Suite(&testPrivilegeSuite{})

type testPrivilegeSuite struct{}

func (t *testPrivilegeSuite) TestVerifyPrivileges(c *tc.C) {
	dbConfig := &dbutil.DBConfig{
		Host: "127.0.0.1",
		Port: 3306,
	}
	pc := NewSourcePrivilegeChecker(nil, dbConfig).(*SourcePrivilegeChecker)

	cases := []struct {
		grants []string
		state  State
	}{
		{
			grants: nil, // non grants
			state:  StateFailure,
		},
		{
			grants: []string{"invalid SQL statement"},
			state:  StateFailure,
		},
		{
			grants: []string{"CREATE DATABASE db1"}, // non GRANT statement
			state:  StateFailure,
		},
		{
			grants: []string{"GRANT SELECT ON *.* TO 'user'@'%'"}, // lack necessary privilege
			state:  StateFailure,
		},
		{
			grants: []string{"GRANT REPLICATION SLAVE ON *.* TO 'user'@'%'"}, // lack necessary privilege
			state:  StateFailure,
		},
		{
			grants: []string{ // lack optional privilege
				"GRANT REPLICATION SLAVE ON *.* TO 'user'@'%'",
				"GRANT REPLICATION CLIENT ON *.* TO 'user'@'%'",
			},
			state: StateWarning,
		},
		{
			grants: []string{ // have privileges
				"GRANT REPLICATION SLAVE ON *.* TO 'user'@'%'",
				"GRANT REPLICATION CLIENT ON *.* TO 'user'@'%'",
				"GRANT RELOAD ON *.* TO 'user'@'%'",
			},
			state: StateSuccess,
		},
		{
			grants: []string{ // have privileges
				"GRANT REPLICATION SLAVE, REPLICATION CLIENT, RELOAD ON *.* TO 'user'@'%'",
			},
			state: StateSuccess,
		},
		{
			grants: []string{ // have privileges
				"GRANT ALL PRIVILEGES ON *.* TO 'user'@'%'",
			},
			state: StateSuccess,
		},
		{
			grants: []string{ // lower case not supported yet
				"GRANT all privileges ON *.* TO 'user'@'%'",
			},
			state: StateFailure,
		},
		{
			grants: []string{ // IDENTIFIED BY PASSWORD
				"GRANT ALL PRIVILEGES ON *.* TO 'user'@'%' IDENTIFIED BY PASSWORD 'password'",
			},
			state: StateSuccess,
		},
		{
			grants: []string{ // IDENTIFIED BY PASSWORD with <secret> mark
				"GRANT ALL PRIVILEGES ON *.* TO 'user'@'%' IDENTIFIED BY PASSWORD <secret>",
			},
			state: StateSuccess,
		},
	}

	for _, cs := range cases {
		result := pc.VerifyPrivileges(cs.grants)
		c.Assert(result.State, tc.Equals, cs.state)
	}
}
