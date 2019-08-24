package sqlgen

import (
	"log"
	"math/rand"
	"strings"
)

func Random(symbols ...Function) Result {
	branches := splitBranches(symbols)
	return randomBranch(branches)
}

func randomBranch(branches [][]Function) Result {
	branchNum := len(branches)
	if branchNum <= 0 {
		return Result{Tp: Invalid}
	}
	chosenBranchNum := rand.Intn(branchNum)
	chosenBranch := branches[chosenBranchNum]

	var doneF []Function
	var resStr strings.Builder
	for i, f := range chosenBranch {
		res := f.Call()
		switch res.Tp {
		case PlainString:
			doneF = append(doneF, f)
			if i != 0 {
				resStr.WriteString(" ")
			}
			resStr.WriteString(res.Value)
		case Invalid:
			for _, df := range doneF {
				df.Cancel()
			}
			branches[chosenBranchNum], branches[0] = branches[0], branches[chosenBranchNum]
			return randomBranch(branches[1:])
		default:
			log.Fatalf("Unsupported result type '%v'", res.Tp)
		}
	}
	return Str(resStr.String())
}

func splitBranches(fns []Function) [][]Function {
	var ret [][]Function
	var Branch []Function
	for _, f := range append(fns, Or) {
		if isBranch(f) {
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
