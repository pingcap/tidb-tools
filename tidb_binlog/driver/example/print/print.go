/*************************************************************************
 *
 * PingCAP CONFIDENTIAL
 * __________________
 *
 *  [2015] - [2018] PingCAP Incorporated
 *  All Rights Reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of PingCAP Incorporated and its suppliers,
 * if any.  The intellectual and technical concepts contained
 * herein are proprietary to PingCAP Incorporated
 * and its suppliers and may be covered by P.R.China and Foreign Patents,
 * patents in process, and are protected by trade secret or copyright law.
 * Dissemination of this information or reproduction of this material
 * is strictly forbidden unless prior written permission is obtained
 * from PingCAP Incorporated.
 */

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
		KafkaAddr: []string{"127.0.0.1:9092"},
		Offset:    *offset,
		CommitTS:  *commitTS,
		ClusterID: *clusterID,
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
