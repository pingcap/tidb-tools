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
	"os"

	"github.com/juju/errors"
	"github.com/ngaut/log"
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
		err = queryNodesByKind(cfg.EtcdURLs, pumpNode)
	case queryDrainer:
		err = queryNodesByKind(cfg.EtcdURLs, drainerNode)
	case unregisterPumps:
		err = unregisterNode(cfg.EtcdURLs, pumpNode, cfg.NodeID)
	case unregisterDrainer:
		err = unregisterNode(cfg.EtcdURLs, drainerNode, cfg.NodeID)
	}

	if err != nil {
		log.Fatalf("fail to execute %s error %v", cfg.Command, errors.ErrorStack(err))
	}
}
