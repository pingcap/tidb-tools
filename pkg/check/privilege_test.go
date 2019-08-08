package check

import (
	"testing"

	tc "github.com/pingcap/check"
)

func TestClient(t *testing.T) {
	tc.TestingT(t)
}

var _ = tc.Suite(&testCheckSuite{})

type testCheckSuite struct{}

func (t *testCheckSuite) TestVerifyPrivileges(c *tc.C) {
	var (
		dumpPrivileges        = []string{"RELOAD", "SELECT"}
		replicationPrivileges = []string{"REPLICATION SLAVE", "REPLICATION CLIENT"}
	)

	cases := []struct {
		grants          []string
		dumpState       State
		replcationState State
	}{
		{
			grants:          nil, // non grants
			dumpState:       StateFailure,
			replcationState: StateFailure,
		},
		{
			grants:          []string{"invalid SQL statement"},
			dumpState:       StateFailure,
			replcationState: StateFailure,
		},
		{
			grants:          []string{"CREATE DATABASE db1"}, // non GRANT statement
			dumpState:       StateFailure,
			replcationState: StateFailure,
		},
		{
			grants:          []string{"GRANT SELECT ON *.* TO 'user'@'%'"}, // lack necessary privilege
			dumpState:       StateFailure,
			replcationState: StateFailure,
		},
		{
			grants:          []string{"GRANT REPLICATION SLAVE ON *.* TO 'user'@'%'"}, // lack necessary privilege
			dumpState:       StateFailure,
			replcationState: StateFailure,
		},
		{
			grants: []string{ // lack optional privilege
				"GRANT REPLICATION SLAVE ON *.* TO 'user'@'%'",
				"GRANT REPLICATION CLIENT ON *.* TO 'user'@'%'",
			},
			dumpState:       StateFailure,
			replcationState: StateSuccess,
		},
		{
			grants: []string{ // have privileges
				"GRANT REPLICATION SLAVE ON *.* TO 'user'@'%'",
				"GRANT REPLICATION CLIENT ON *.* TO 'user'@'%'",
				"GRANT RELOAD ON *.* TO 'user'@'%'",
			},
			dumpState:       StateFailure,
			replcationState: StateSuccess,
		},
		{
			grants: []string{ // have privileges
				"GRANT REPLICATION SLAVE, REPLICATION CLIENT, RELOAD, SELECT ON *.* TO 'user'@'%'",
			},
			dumpState:       StateSuccess,
			replcationState: StateSuccess,
		},
		{
			grants: []string{ // have privileges
				"GRANT ALL PRIVILEGES ON *.* TO 'user'@'%'",
			},
			dumpState:       StateSuccess,
			replcationState: StateSuccess,
		},
		{
			grants: []string{ // lower case not supported yet
				"GRANT all privileges ON *.* TO 'user'@'%'",
			},
			dumpState:       StateFailure,
			replcationState: StateFailure,
		},
		{
			grants: []string{ // IDENTIFIED BY PASSWORD
				"GRANT ALL PRIVILEGES ON *.* TO 'user'@'%' IDENTIFIED BY PASSWORD",
			},
			dumpState:       StateSuccess,
			replcationState: StateSuccess,
		},
		{
			grants: []string{ // IDENTIFIED BY PASSWORD
				"GRANT ALL PRIVILEGES ON *.* TO 'user'@'%' IDENTIFIED BY PASSWORD WITH GRANT OPTION",
			},
			dumpState:       StateSuccess,
			replcationState: StateSuccess,
		},
		{
			grants: []string{ // IDENTIFIED BY PASSWORD
				"GRANT ALL PRIVILEGES ON *.* TO 'user'@'%' IDENTIFIED BY PASSWORD 'password'",
			},
			dumpState:       StateSuccess,
			replcationState: StateSuccess,
		},
		{
			grants: []string{ // IDENTIFIED BY PASSWORD
				"GRANT ALL PRIVILEGES ON *.* TO 'user'@'%' IDENTIFIED BY PASSWORD 'password' WITH GRANT OPTION",
			},
			dumpState:       StateSuccess,
			replcationState: StateSuccess,
		},
		{
			grants: []string{ // IDENTIFIED BY PASSWORD with <secret> mark
				"GRANT ALL PRIVILEGES ON *.* TO 'user'@'%' IDENTIFIED BY PASSWORD <secret>",
			},
			dumpState:       StateSuccess,
			replcationState: StateSuccess,
		},
		{
			grants: []string{ // IDENTIFIED BY PASSWORD with <secret> mark
				"GRANT ALL PRIVILEGES ON *.* TO 'user'@'%' IDENTIFIED BY PASSWORD <secret> WITH GRANT OPTION",
			},
			dumpState:       StateSuccess,
			replcationState: StateSuccess,
		},
	}

	for _, cs := range cases {
		result := &Result{
			State: StateFailure,
		}
		verifyPrivileges(result, cs.grants, dumpPrivileges)
		c.Assert(result.State, tc.Equals, cs.dumpState)
		verifyPrivileges(result, cs.grants, replicationPrivileges)
		c.Assert(result.State, tc.Equals, cs.replcationState)
	}
}
