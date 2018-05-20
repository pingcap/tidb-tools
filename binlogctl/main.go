// Copyright 2016 PingCAP, Inc.
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
	"os"

	"github.com/juju/errors"
	"github.com/ngaut/log"
)

func main() {
	cfg := NewConfig()
	if err := cfg.Parse(os.Args[1:]); err != nil {
		log.Infof("verifying flags error, See 'drainer --help'. %s", errors.ErrorStack(err))
	}

	var err error
	switch cfg.Command {
	case generateSavepoint:
		err = GenSavepointInfo(cfg)
	case queryPumps:
		err = queryService(cfg.EtcdURLs, pumpService)
	case queryDrainer:
		err = queryService(cfg.EtcdURLs, drainerService)
	case unregisterPumps:
		err = unregisterService(cfg.EtcdURLs, pumpService, cfg.NodeID)
	case unregisterDrainer:
		err = unregisterService(cfg.EtcdURLs, drainerService, cfg.NodeID)
	}

	if err != nil {
		log.Fatalf("fail to execute %s error %v", cfg.Command, err)
	}
}
