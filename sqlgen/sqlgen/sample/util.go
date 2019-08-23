package sample

import (
	. "github.com/pingcap/tidb-tools/sqlgen/sqlgen"
	"log"
	"math/rand"
	"strings"
	"time"
)

var state = State{
	Choices:           nil,
	Counter:           map[string]int{},
	TotalCounter:      map[string]int{},
	CurrentProduction: nil,

	ProductionMap:       nil,
	BeginProductionName: "",
	IsInitialize:        false,
}

// Fn is able to manipulate global state, simulating calling stack.
type Fn struct {
	name        string
	f           func() Result
	isBranchTag bool // Mark for splitter '|'.
}

func (fn *Fn) callWithLoc(branchNum, SeqNum int) Result {
	state.EnsureInitialized()
	if fn.isBranchTag {
		log.Fatal("Cannot call on Branch tag")
	}

	choice := Choice{Branch: branchNum, SeqNum: SeqNum}
	state.Choices = append(state.Choices, choice)

	fnName := fn.name
	// Before calling function.
	state.Counter[fnName] += 1
	state.TotalCounter[fnName] += 1
	state.CurrentProduction = findProductionAndUnwrap(fnName, state.ProductionMap)

	ret := fn.f()
	// After calling function.
	parent := state.Parent()
	state.Choices = state.Choices[:len(state.Choices)-1]
	state.Counter[fnName] -= 1
	state.CurrentProduction = parent
	return ret
}

func (fn *Fn) discard() {
	state.EnsureInitialized()

	fnName := fn.name
	state.Counter[fnName] -= 1
	state.TotalCounter[fnName] -= 1
}

// ----- utilities ------

func random(symbols ...Fn) Result {
	branches := splitBranches(symbols)
	return randomBranch(branches)
}

func randomBranch(branches [][]Fn) Result {
	branchNum := len(branches)
	if branchNum <= 0 {
		return Result{Tp: Invalid}
	}
	chosenBranchNum := rand.Intn(branchNum)
	chosenBranch := branches[chosenBranchNum]

	var doneF []Fn
	var resStr strings.Builder
	for i, f := range chosenBranch {
		res := f.callWithLoc(chosenBranchNum, i)
		switch res.Tp {
		case PlainString:
			doneF = append(doneF, f)
			if i != 0 {
				resStr.WriteString(" ")
			}
			resStr.WriteString(res.Value)
		case NonExist:
			log.Fatalf("Production '%s' not found", f.name)
		case Invalid:
			for _, df := range doneF {
				df.discard()
			}
			branches[chosenBranchNum], branches[0] = branches[0], branches[chosenBranchNum]
			return randomBranch(branches[1:])
		default:
			log.Fatalf("Unsupported result type '%v'", res.Tp)
		}
	}
	return Str(resStr.String())
}

func splitBranches(fns []Fn) [][]Fn {
	var ret [][]Fn
	var Branch []Fn
	for _, f := range append(fns, Or) {
		if f.isBranchTag {
			if len(Branch) == 0 {
				log.Fatal("Empty Branch is impossible to split")
			}
			ret = append(ret, Branch)
			Branch = nil
		} else {
			Branch = append(Branch, f)
		}
	}
	return ret
}

func findProductionAndUnwrap(name string, prodMap map[string]*Production) *Production {
	ret, ok := prodMap[name]
	if !ok {
		return nil
	}
	return ret
}

func initState(bnfFileName string, beginProdName string) {
	prods, err := ParseYacc(bnfFileName)
	if err != nil {
		log.Fatal(err)
	}
	prodMap := BuildProdMap(prods)
	beginProd, ok := prodMap[beginProdName]
	if !ok {
		log.Fatalf("Begin production name '%s' not found", beginProdName)
	}

	state.ProductionMap = prodMap
	state.CurrentProduction = beginProd
	state.BeginProductionName = beginProdName
	rand.Seed(time.Now().UnixNano())
	state.IsInitialize = true
}

func constFn(str string) Fn {
	return Fn{name: str, f: func() Result {
		return Result{Tp: PlainString, Value: str}
	}}
}

func Str(str string) Result {
	return Result{Tp: PlainString, Value: str}
}

var Or = Fn{isBranchTag: true}

