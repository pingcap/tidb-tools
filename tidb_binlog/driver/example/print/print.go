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

	"github.com/Shopify/sarama"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb-tools/tidb_binlog/driver/reader"
)

var (
	offset    = flag.Int64("offset", sarama.OffsetNewest, "offset")
	commitTS  = flag.Int64("commitTS", 0, "commitTS")
	clusterID = flag.String("clusterID", "6561373978432450126", "clusterID")
)

func main() {
	flag.Parse()

	cfg := &reader.Config{
		KafakaAddr: []string{"127.0.0.1:9092"},
		Offset:     *offset,
		CommitTS:   *commitTS,
		ClusterID:  *clusterID,
	}

	breader, err := reader.NewReader(cfg)
	if err != nil {
		panic(err)
	}

	for {
		select {
		case msg := <-breader.Messages():
			log.Info("recv: ", msg.Binlog.String())
		}
	}
}
