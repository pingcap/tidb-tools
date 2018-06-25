package node

import (
	pb "github.com/pingcap/tipb/go-binlog"
)

// Status describes the status information of a tidb-binlog node in etcd
type Status struct {
	NodeID string
	Host   string
	// send hearbeats to keep alive
	IsAlive bool
	// denote whether node is offline
	IsOffline bool
	//  position of latest binlog that write into binlog file of pump
	LatestFilePos pb.Pos
	// offset of latest binlog that write into kafka by pump
	LatestKafkaPos pb.Pos
	//  offline TSO
	OfflineTS int64
}
