// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb-tools/pkg/etcd"
	"github.com/pingcap/tidb-tools/pkg/utils"
	"github.com/pingcap/tidb-tools/tidb_binlog/node"
	"golang.org/x/net/context"
)

var (
	etcdDialTimeout = 5 * time.Second

	// kind of node
	pumpNode    = "pump"
	drainerNode = "drainer"

	nodePrefix = map[string]string{
		pumpNode:    "pumps",
		drainerNode: "cisterns",
	}
)

// queryNodesByKind returns specified nodes, like pumps/drainers
func queryNodesByKind(urls string, kind string) error {
	registry, err := createRegistry(urls)
	if err != nil {
		return errors.Trace(err)
	}

	nodes, err := registry.Nodes(context.Background(), nodePrefix[kind])
	if err != nil {
		return errors.Trace(err)
	}

	for _, n := range nodes {
		log.Infof("%s: %+v", kind, n)
	}

	return nil
}

// unregisterNode unregisters specified node
func unregisterNode(urls, kind, nodeID string) error {
	registry, err := createRegistry(urls)
	if err != nil {
		return errors.Trace(err)
	}

	nodes, err := registry.Nodes(context.Background(), nodePrefix[kind])
	if err != nil {
		return errors.Trace(err)
	}

	for _, n := range nodes {
		if n.NodeID == nodeID {
			if n.IsAlive {
				return errors.Errorf("kind %s is alive, don't allow to delete it", n.NodeID)
			}

			switch kind {
			case pumpNode:
				return registry.MarkOfflineNode(context.Background(), nodePrefix[kind], n.NodeID, n.Host, n.LatestKafkaPos, n.LatestFilePos, n.OfflineTS)
			case drainerNode:
				return registry.UnregisterNode(context.Background(), nodePrefix[kind], n.NodeID)
			default:
				return errors.NotSupportedf("node %s", kind)
			}
		}
	}

	return errors.NotFoundf("node %s, id %s from etcd %s", kind, nodeID, urls)
}

// createRegistry returns an ectd registry
func createRegistry(urls string) (*node.EtcdRegistry, error) {
	ectdEndpoints, err := utils.ParseHostPortAddr(urls)
	if err != nil {
		return nil, errors.Trace(err)
	}
	cli, err := etcd.NewClientFromCfg(ectdEndpoints, etcdDialTimeout, etcd.DefaultRootPath, nil)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return node.NewEtcdRegistry(cli, etcdDialTimeout), nil
}
