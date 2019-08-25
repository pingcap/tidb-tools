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
	"flag"
	"fmt"
	"github.com/pingcap/tidb-tools/sqlgen/sqlgen"
	"log"
	"os"
	"path/filepath"
	"strings"
)

var (
	bnfFile        string
	productionName string
	destination    string
	packageName    string
	plugins        []string
)

func parseFlags() {
	flag.StringVar(&bnfFile, "bnf", "", "The bnf file")
	flag.StringVar(&productionName, "prod", "", "The production to be generated")
	flag.StringVar(&destination, "dest", ".", "The destination of generated package located")
	flag.StringVar(&packageName, "pkgname", "", "The name of package to be generated")
	var pluginStrs string
	flag.StringVar(&pluginStrs, "plugin", "", fmt.Sprintf(
		"Plugins for the generated package. %s", supportedPluginMessage()))
	flag.Parse()
	plugins = strings.Split(pluginStrs, " ")

	if len(bnfFile) == 0 || len(productionName) == 0 || len(packageName) == 0 {
		flag.Usage()
		os.Exit(0)
	}
}

var validPlugins = map[string]string{
	"maxLoopLimit": "GenPlugins = append(GenPlugins, NewMaxLoopCounter(3))",
}

func supportedPluginMessage() string {
	pluginNames := make([]string, 0, len(validPlugins))
	for k := range validPlugins {
		pluginNames = append(pluginNames, k)
	}
	return fmt.Sprintf("Supported plugin(s): {%s}", strings.Join(pluginNames, " | "))
}

func checkPluginsValidation() (pluginInitSnippet []string) {
	for _, plugin := range plugins {
		if initSnippet, ok := validPlugins[plugin]; ok {
			pluginInitSnippet = append(pluginInitSnippet, initSnippet)
		} else {
			fmt.Println(supportedPluginMessage())
			os.Exit(0)
		}
	}
	return
}

func main() {
	parseFlags()
	pluginInits := checkPluginsValidation()
	destination = absolutePath(destination)
	sqlgen.BuildLib(bnfFile, productionName, packageName, destination, pluginInits)
	fmt.Printf("The package '%s' is created at %s\n", packageName, destination)
}

func absolutePath(p string) string {
	abs, err := filepath.Abs(p)
	if err != nil {
		log.Fatal(err)
	}
	return abs
}
