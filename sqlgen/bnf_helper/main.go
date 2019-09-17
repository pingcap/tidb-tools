// Copyright 2019 PingCAP, Inc.
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
	"bufio"
	"flag"
	"log"
	"os"

	"github.com/pingcap/tidb-tools/sqlgen/sqlgen"
)

var (
	sourceBnfFile  string
	rewriteBnfFile string
)

func parseFlags() {
	flag.StringVar(&sourceBnfFile, "src", "", "The complete bnf file")
	flag.StringVar(&rewriteBnfFile, "rewrite", "", "The bnf file to be rewrite")
	flag.Parse()
	if len(sourceBnfFile) == 0 || len(rewriteBnfFile) == 0 {
		flag.Usage()
		os.Exit(0)
	}
}

func main() {
	parseFlags()
	completeProds, err := sqlgen.ParseYacc(sourceBnfFile)
	if err != nil {
		log.Fatal(err)
	}
	prodMap := sqlgen.BuildProdMapWithCheck(completeProds)
	newProds, err := sqlgen.ParseYacc(rewriteBnfFile)
	if err != nil {
		log.Fatal(err)
	}

	if len(newProds) == 0 {
		return
	}
	newProds = sqlgen.RemoveUnused(newProds[0].Head(), newProds)
	result := sqlgen.CompleteProds(newProds, prodMap)

	oFile, err := os.OpenFile(rewriteBnfFile, os.O_WRONLY|os.O_TRUNC, 0755)
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = oFile.Close() }()
	w := bufio.NewWriter(oFile)

	for _, p := range result {
		_, err := w.WriteString(p.StringWithoutOptNum())
		if err != nil {
			log.Fatal(err)
		}
		_, err = w.WriteString("\n")
		if err != nil {
			log.Fatal(err)
		}
	}
	_ = w.Flush()
}
