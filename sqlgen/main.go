package main

import (
	"flag"
	"fmt"
	"github.com/pingcap/tidb-tools/sqlgen/sqlgen"
	"log"
	"os"
	"path/filepath"
)

var (
	bnfFile        string
	productionName string
	destination    string
	packageName    string
)

func parseFlags() {
	flag.StringVar(&bnfFile, "bnf", "", "A list of bnf files")
	flag.StringVar(&productionName, "production", "", "The production to be generated")
	flag.StringVar(&destination, "destination", ".", "The destination of generated package located")
	flag.StringVar(&packageName, "pkgname", "", "The name of package to be generated")
	flag.Parse()
	if len(bnfFile) == 0 || len(productionName) == 0 || len(packageName) == 0 {
		flag.Usage()
		os.Exit(0)
	}
}

func main() {
	parseFlags()
	sqlgen.BuildLib(bnfFile, productionName, packageName, destination)
	destination = explicitPath(destination)
	fmt.Printf("The package is created at %s\n", destination)
}

func explicitPath(p string) string {
	abs, err := filepath.Abs(p)
	if err != nil {
		log.Fatal(err)
	}
	return abs
}
