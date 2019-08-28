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

package sqlgen

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
)

// BuildLib is used to build a random generator library.
func BuildLib(yaccFilePath, prodName, packageName, outputFilePath string, pluginInitStr []string) {
	yaccFilePath = absolute(yaccFilePath)
	outputFilePath = absolute(filepath.Join(outputFilePath, packageName))
	prods, err := ParseYacc(yaccFilePath)
	if err != nil {
		log.Fatal(err)
	}
	prodMap := BuildProdMapWithCheck(prods)

	must(os.Mkdir(outputFilePath, 0755))
	must(os.Chdir(outputFilePath))

	allProds := writeGenerate(prodName, prodMap, packageName, pluginInitStr)
	writeDeclarations(allProds, packageName)
	writeTest(packageName)
}

func writeGenerate(prodName string, prodMap map[string]*Production, packageName string, pluginInitStr []string) map[string]struct{} {
	var allProds map[string]struct{}
	openAndWrite(prodName+".go", packageName, func(w *bufio.Writer) {
		var sb strings.Builder
		visitor := func(p *Production) {
			sb.WriteString(convertProdToCode(p))
		}
		ps, err := breadthFirstSearch(prodName, prodMap, visitor)
		allProds = ps
		if err != nil {
			log.Fatal(err)
		}
		mustWrite(w, fmt.Sprintf(templateMain, strings.Join(pluginInitStr, "\n\t"), prodName, sb.String()))
	})
	return allProds
}

func writeDeclarations(allProds map[string]struct{}, packageName string) {
	openAndWrite("declarations.go", packageName, func(w *bufio.Writer) {
		mustWrite(w, declarationsImport)
		for p := range allProds {
			mustWrite(w, fmt.Sprintf("var %s Fn\n", p))
		}
	})
}

func writeTest(packageName string) {
	openAndWrite(packageName+"_test.go", packageName, func(w *bufio.Writer) {
		mustWrite(w, testSnippet)
	})
}

func openAndWrite(path string, pkgName string, doWrite func(*bufio.Writer)) {
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = file.Close() }()
	writer := bufio.NewWriter(file)
	mustWrite(writer, fmt.Sprintf("package %s\n", pkgName))
	doWrite(writer)
	must(writer.Flush())
}

const templateMain = `
import (
	"log"
	"math/rand"
	"time"

	. "github.com/pingcap/tidb-tools/sqlgen/sqlgen"
)

// Generate is used to generate a string according to bnf grammar.
var Generate = generate()

func generate() func() string {
	rand.Seed(time.Now().UnixNano())
	%s
	retFn := func() string {
		res := %s.F()
		switch res.Tp {
		case PlainString:
			return res.Value
		case Invalid:
			log.Println("Invalid SQL")
			return ""
		default:
			log.Fatalf("Unsupported result type '%%v'", res.Tp)
			return ""
		}
	}
%s
	return retFn
}
`

const declarationsImport = `
import (
	. "github.com/pingcap/tidb-tools/sqlgen/sqlgen"
)

`

const templateR = `
	%s = NewFn("%s",
		Or(
			%s
		),
	)
`

const templateS = `
	%s = NewFn("%s", Str("%s"))
`

func convertProdToCode(p *Production) string {
	if len(p.bodyList) == 1 {
		allLiteral := true
		seqs := p.bodyList[0].seq
		for _, s := range seqs {
			if !isLiteral(s) {
				allLiteral = false
				break
			}
		}

		trimmedSeqs := trimmedStrs(seqs)
		if allLiteral {
			return fmt.Sprintf(templateS, p.head, p.head, strings.Join(trimmedSeqs, " "))
		}
	}

	sb := &strings.Builder{}
	for i, body := range p.bodyList {
		needsAnd := len(body.seq) > 1
		wrapIf(sb, needsAnd, writeAnd, func() {
			writeProdBody(sb, body.seq)
			if !needsAnd {
				sb.WriteString(",")
			}
		})
		if i != len(p.bodyList)-1 {
			sb.WriteString("\n\t\t\t")
		}
	}

	return fmt.Sprintf(templateR, p.head, p.head, sb.String())
}

func trimmedStrs(origin []string) []string {
	ret := make([]string, len(origin))
	for i, s := range origin {
		if lit, ok := literal(s); ok {
			ret[i] = lit
		}
	}
	return ret
}

func writeProdBody(sb *strings.Builder, strs []string) {
	for i, s := range strs {
		wrapIf(sb, isLiteral(s), writeStr, func() {
			sb.WriteString(strings.Trim(s, "'"))
		})
		if i != len(strs)-1 {
			sb.WriteString(", ")
		}
	}
}

func writeAnd(sb *strings.Builder, fn func()) {
	sb.WriteString("And(")
	fn()
	sb.WriteString("),")
}

func writeStr(sb *strings.Builder, fn func()) {
	sb.WriteString("Str(\"")
	fn()
	sb.WriteString("\")")
}

func wrapIf(sb *strings.Builder, condition bool, wrapper func(*strings.Builder, func()), beingWrap func()) {
	if condition {
		wrapper(sb, beingWrap)
	} else {
		beingWrap()
	}
}

func mustWrite(oFile *bufio.Writer, str string) {
	_, err := oFile.WriteString(str)
	if err != nil {
		log.Fatal(err)
	}
}

func must(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func absolute(p string) string {
	abs, err := filepath.Abs(p)
	if err != nil {
		log.Fatal(err)
	}
	return abs
}

const testSnippet = `
import (
	"fmt"
	"testing"
)

func TestA(t *testing.T) {
	for i := 0; i < 10; i++ {
		fmt.Println(Generate())
	}
}
`
