package node

import (
	"time"

	pb "github.com/pingcap/tipb/go-binlog"
)

// Node desribe a service node for tidb-binlog
type Node struct {
	*EtcdRegistry
	id                string
	host              string
	heartbeatTTL      int64
	heartbeatInterval time.Duration
}

// Status describes the status information of a node in etcd
type Status struct {
	NodeID         string
	Host           string
	IsAlive        bool
	IsOffline      bool
	LatestFilePos  pb.Pos
	LatestKafkaPos pb.Pos
	OfflineTS      int64
}