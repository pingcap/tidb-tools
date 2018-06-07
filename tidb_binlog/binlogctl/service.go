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
	"github.com/pingcap/tidb-tools/pkg/flags"
	"github.com/pingcap/tidb-tools/tidb_binlog/node"
	"golang.org/x/net/context"
)

var (
	etcdDialTimeout = 5 * time.Second

	pumpService    = "pump"
	drainerService = "drainer"
	nodePrefix     = map[string]string{
		pumpService:    "pumps",
		drainerService: "cisterns",
	}
)

func queryService(urls string, kind string) error {
	registry, err := createRegistry(urls)
	if err != nil {
		return errors.Trace(err)
	}

	services, err := registry.Nodes(context.Background(), nodePrefix[kind])
	if err != nil {
		return errors.Trace(err)
	}

	for _, s := range services {
		log.Infof("%s: %+v", kind, s)
	}

	return nil
}

func unregisterService(urls, kind, id string) error {
	registry, err := createRegistry(urls)
	if err != nil {
		return errors.Trace(err)
	}

	services, err := registry.Nodes(context.Background(), nodePrefix[kind])
	if err != nil {
		return errors.Trace(err)
	}

	for _, s := range services {
		if s.NodeID == id {
			if s.IsAlive {
				return errors.Errorf("kind %s is alive, don't allow to delete it", s.NodeID)
			}

			switch kind {
			case pumpService:
				return registry.MarkOfflineNode(context.Background(), nodePrefix[kind], s.NodeID, s.Host, s.LatestKafkaPos, s.LatestFilePos, s.OfflineTS)
			case drainerService:
				return registry.UnregisterNode(context.Background(), nodePrefix[kind], s.NodeID)
			default:
				return errors.NotSupportedf("service %s", kind)
			}
		}
	}

	return errors.NotFoundf("service %s, id %s from etcd %s", kind, id, urls)
}

func createRegistry(urls string) (*node.EtcdRegistry, error) {
	urlv, err := flags.NewURLsValue(urls)
	if err != nil {
		return nil, errors.Trace(err)
	}
	cli, err := etcd.NewClientFromCfg(urlv.StringSlice(), etcdDialTimeout, etcd.DefaultRootPath, nil)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return node.NewEtcdRegistry(cli, etcdDialTimeout), nil
}
