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
func BuildLib(yaccFilePath, prodName, packageName, outputFilePath string) {
	yaccFilePath = absolute(yaccFilePath)
	outputFilePath = absolute(filepath.Join(outputFilePath, packageName))
	prods, err := ParseYacc(yaccFilePath)
	if err != nil {
		log.Fatal(err)
	}
	prodMap := BuildProdMap(prods)

	must(os.Mkdir(outputFilePath, 0755))
	must(os.Chdir(outputFilePath))

	allProds := writeGenerate(prodName, prodMap, packageName)
	writeUtil(packageName)
	writeDeclarations(allProds, packageName)
	writeTest(packageName)
}

func writeGenerate(prodName string, prodMap map[string]*Production, packageName string) map[string]struct{} {
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
		mustWrite(w, fmt.Sprintf(templateMain, prodName, sb.String()))
	})
	return allProds
}

func writeUtil(packageName string) {
	openAndWrite("util.go", packageName, func(w *bufio.Writer) {
		mustWrite(w, utilSnippet)
	})
}

func writeDeclarations(allProds map[string]struct{}, packageName string) {
	openAndWrite("declarations.go", packageName, func(w *bufio.Writer) {
		mustWrite(w, "\n")
		for p := range allProds {
			p = convertHead(p)
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
	retFn := func() string {
		res := %s.f()
		switch res.Tp {
		case PlainString:
			return res.Value
		case Invalid:
			log.Println("Invalid SQL")
			return ""
		default:
			log.Fatalf("Unsupported result type '%%v'", res.Tp)
		}
		panic("impossible to reach")
	}

	%s

	return retFn
}
`

const utilSnippet = `
import (
	. "github.com/pingcap/tidb-tools/sqlgen/sqlgen"
)

var counter = map[string]int{}
const maxLoopback = 2

type Fn struct {
	name string
	f    func() Result
}

func (fn Fn) Name() string {
	return fn.name
}

func (fn Fn) Call() Result {
	fnName := fn.name
	// Before calling function.
	counter[fnName]++
	if counter[fnName] > maxLoopback {
		return Result{Tp: Invalid}
	}

	ret := fn.f()
	// After calling function.
	counter[fnName]--
	return ret
}

func (fn Fn) Cancel() {
	counter[fn.name]--
}

func Const(str string) Fn {
	return Fn{name: str, f: func() Result {
		return Result{Tp: PlainString, Value: str}
	}}
}
`

const templateR = `
	%s = Fn{
		name: "%s",
		f: func() Result {
			return Br(
				%s
			).Eval()
		},
	}
`

const templateS = `
	%s = Fn{
		name: "%s",
		f: func() Result {
			return Str("%s")
		},
	}
`

func convertProdToCode(p *Production) string {
	prodHead := convertHead(p.head)
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
			return fmt.Sprintf(templateS, prodHead, prodHead, strings.Join(trimmedSeqs, " "))
		}
	}

	var bodyStr strings.Builder
	for i, body := range p.bodyList {
		for _, s := range body.seq {
			if isLit, ok := literal(s); ok {
				s = fmt.Sprintf("Const(\"%s\")", isLit)
			} else {
				s = convertHead(s)
			}
			bodyStr.WriteString(s)
			bodyStr.WriteString(", ")
		}
		if i != len(p.bodyList)-1 {
			bodyStr.WriteString("Or, \n\t\t\t\t")
		}
	}

	return fmt.Sprintf(templateR, prodHead, p.head, bodyStr.String())
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

// convertHead to avoid keyword clash.
func convertHead(str string) string {
	if strings.HasPrefix(str, "$@") {
		return "num" + strings.TrimPrefix(str, "$@")
	}

	switch str {
	case "type":
		return "utype"
	case "%empty":
		return "empty"
	default:
		return str
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
