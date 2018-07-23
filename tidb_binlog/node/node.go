package node

import (
	"time"
)

// State is the state of node.
type State string

const (
	// Online means the node can receive request.
	Online State = "online"

	// Pausing means the node is pausing.
	Pausing State = "pausing"

	// Paused means the node is already paused.
	Paused State = "paused"

	// Closing means the node is closing, and the state will be Offline when closed.
	Closing State = "closing"

	// Offline means the node is offline, and will not provide service.
	Offline State = "offline"

	// Unknow means the node's state is unkonw.
	Unknow State = "unknow"
)

// Status describes the status information of a tidb-binlog node in etcd.
type Status struct {
	// the id of node.
	NodeID string

	// the host of pump or node.
	Host string

	// the state of pump.
	State State

	// the score of node, it is report by node, calculated by node's qps, disk usage and binlog's data size.
	// if Score is less than 0, means this node is useless. Now only used for pump.
	Score int64

	// the label of this node. Now only used for pump.
	// pump client will only send to a pump which label is matched.
	Label string

	// UpdateTime is the last update time of pump's status.
	UpdateTime time.Time
}
