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
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"os"

	"github.com/pingcap/kvproto/pkg/metapb"
	pd "github.com/pingcap/pd/pd-client"
	"github.com/pingcap/tidb-tools/pkg/utils"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/util/codec"
	"golang.org/x/net/context"
)

var (
	pdAddr       = flag.String("pd", "http://127.0.0.1:2379", "PD address")
	tableID      = flag.Int64("table", 0, "table ID")
	indexID      = flag.Int64("index", 0, "index ID")
	limit        = flag.Int("limit", 10000, "limit")
	printVersion = flag.Bool("V", false, "prints version and exit")
)

func exitWithErr(err error) {
	if err != nil {
		fmt.Printf("%v\n", err)
		os.Exit(1)
	}
}

type regionInfo struct {
	Region *metapb.Region
	Leader *metapb.Peer
}

type keyRange struct {
	startKey []byte
	endKey   []byte
}

func main() {
	flag.Parse()
	if *printVersion {
		fmt.Printf(utils.GetRawInfo("dump_region"))
		return
	}

	if *tableID == 0 {
		exitWithErr(fmt.Errorf("need table ID"))
	}

	// TODO: support tsl
	client, err := pd.NewClient([]string{*pdAddr}, pd.SecurityOption{
		CAPath:   "",
		CertPath: "",
		KeyPath:  "",
	})
	exitWithErr(err)

	defer client.Close()

	var (
		startKey []byte
		endKey   []byte
	)

	if *indexID == 0 {
		// dump table region
		startKey = tablecodec.GenTableRecordPrefix(*tableID)
		endKey = tablecodec.GenTableRecordPrefix(*tableID + 1)
	} else {
		// dump table index region
		startKey = tablecodec.EncodeTableIndexPrefix(*tableID, *indexID)
		endKey = tablecodec.EncodeTableIndexPrefix(*tableID, *indexID+1)
	}

	startKey = codec.EncodeBytes([]byte(nil), startKey)
	endKey = codec.EncodeBytes([]byte(nil), endKey)

	rangeInfo, err := json.Marshal(&keyRange{
		startKey: startKey,
		endKey:   endKey,
	})
	exitWithErr(err)

	fmt.Println(string(rangeInfo))

	ctx := context.Background()
	for i := 0; i < *limit; i++ {
		region, leader, err := client.GetRegion(ctx, startKey)
		exitWithErr(err)

		if bytes.Compare(region.GetStartKey(), endKey) >= 0 {
			break
		}

		startKey = region.GetEndKey()

		r := &regionInfo{
			Region: region,
			Leader: leader,
		}

		infos, err := json.MarshalIndent(r, "", "  ")
		exitWithErr(err)

		fmt.Println(string(infos))
	}
}
