package node

import (
	"path"
	"testing"
	"time"

	"github.com/coreos/etcd/integration"
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb-tools/pkg/etcd"
	"golang.org/x/net/context"
)

var _ = Suite(&testRegistrySuite{})
var nodePrefix = path.Join(DefaultRootPath, NodePrefix[PumpNode])

type testRegistrySuite struct{}

type RegisrerTestClient interface {
	Node(context.Context, string, string) (*Status, int64, error)
}

var testEtcdCluster *integration.ClusterV3

func TestNode(t *testing.T) {
	testEtcdCluster = integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1})
	defer testEtcdCluster.Terminate(t)

	TestingT(t)
}

func (t *testRegistrySuite) TestUpdateNodeInfo(c *C) {
	etcdclient := etcd.NewClient(testEtcdCluster.RandClient(), DefaultRootPath)
	r := NewEtcdRegistry(etcdclient, time.Duration(5)*time.Second)
	ns := &Status{
		NodeID:  "test",
		Addr:    "test",
		State:   Online,
		IsAlive: true,
	}

	err := r.UpdateNode(context.Background(), nodePrefix, ns)
	c.Assert(err, IsNil)
	mustEqualStatus(c, r, ns.NodeID, ns)

	ns.Addr = "localhost:1234"
	err = r.UpdateNode(context.Background(), nodePrefix, ns)
	c.Assert(err, IsNil)
	mustEqualStatus(c, r, ns.NodeID, ns)
	// use Nodes() to query node status
	ss, _, err := r.Nodes(context.Background(), nodePrefix)
	c.Assert(err, IsNil)
	c.Assert(ss, HasLen, 1)
}

func (t *testRegistrySuite) TestRegisterNode(c *C) {
	etcdclient := etcd.NewClient(testEtcdCluster.RandClient(), DefaultRootPath)
	r := NewEtcdRegistry(etcdclient, time.Duration(5)*time.Second)

	ns := &Status{
		NodeID:  "test",
		Addr:    "test",
		State:   Online,
		IsAlive: true,
	}
	err := r.UpdateNode(context.Background(), nodePrefix, ns)
	c.Assert(err, IsNil)
	mustEqualStatus(c, r, ns.NodeID, ns)

	ns.State = Offline
	err = r.UpdateNode(context.Background(), nodePrefix, ns)
	c.Assert(err, IsNil)
	mustEqualStatus(c, r, ns.NodeID, ns)

	// TODO: now don't have function to delete node, maybe do it later
	//err = r.UnregisterNode(context.Background(), nodePrefix, ns.NodeID)
	//c.Assert(err, IsNil)
	//exist, err := r.checkNodeExists(context.Background(), nodePrefix, ns.NodeID)
	//c.Assert(err, IsNil)
	//c.Assert(exist, IsFalse)
}

func (t *testRegistrySuite) TestRefreshNode(c *C) {
	etcdclient := etcd.NewClient(testEtcdCluster.RandClient(), DefaultRootPath)
	r := NewEtcdRegistry(etcdclient, time.Duration(5)*time.Second)

	ns := &Status{
		NodeID:  "test",
		Addr:    "test",
		State:   Online,
		IsAlive: true,
	}
	err := r.UpdateNode(context.Background(), nodePrefix, ns)
	c.Assert(err, IsNil)

	ns.IsAlive = true
	mustEqualStatus(c, r, ns.NodeID, ns)

	// TODO: fix it later
	//time.Sleep(2 * time.Second)
	//ns.IsAlive = false
	//mustEqualStatus(c, r, ns.NodeID, ns)
}

func mustEqualStatus(c *C, r RegisrerTestClient, nodeID string, status *Status) {
	ns, _, err := r.Node(context.Background(), nodePrefix, nodeID)
	c.Assert(err, IsNil)
	c.Assert(ns, DeepEquals, status)
}
