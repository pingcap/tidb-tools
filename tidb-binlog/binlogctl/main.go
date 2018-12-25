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
	"flag"
	"os"

	"github.com/juju/errors"
	"github.com/pingcap/tidb-tools/tidb-binlog/node"
	log "github.com/sirupsen/logrus"
)

const (
	pause = "pause"
	close = "close"
)

func main() {
	cfg := NewConfig()
	err := cfg.Parse(os.Args[1:])
	switch err {
	case nil:
	case flag.ErrHelp:
		os.Exit(0)
	default:
		log.Errorf("parse cmd flags err %v", err)
		os.Exit(2)
	}

	switch cfg.Command {
	case generateMeta:
		err = generateMetaInfo(cfg)
	case queryPumps:
		err = queryNodesByKind(cfg.EtcdURLs, node.PumpNode)
	case queryDrainers:
		err = queryNodesByKind(cfg.EtcdURLs, node.DrainerNode)
	case updatePump:
		err = updateNodeState(cfg.EtcdURLs, node.PumpNode, cfg.NodeID, cfg.State)
	case updateDrainer:
		err = updateNodeState(cfg.EtcdURLs, node.DrainerNode, cfg.NodeID, cfg.State)
	case pausePump:
		err = applyAction(cfg.EtcdURLs, node.PumpNode, cfg.NodeID, pause)
	case pauseDrainer:
		err = applyAction(cfg.EtcdURLs, node.DrainerNode, cfg.NodeID, pause)
	case offlinePump:
		err = applyAction(cfg.EtcdURLs, node.PumpNode, cfg.NodeID, close)
	case offlineDrainer:
		err = applyAction(cfg.EtcdURLs, node.DrainerNode, cfg.NodeID, close)
	}

	if err != nil {
		log.Fatalf("fail to execute %s error %v", cfg.Command, errors.ErrorStack(err))
	}
}
