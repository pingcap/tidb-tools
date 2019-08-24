package sqlgen

import (
	"log"
	"math/rand"
	"strings"
)

func randomBranch(branches [][]Function, randomFactors []int) Result {
	if len(branches) <= 0 {
		return Result{Tp: Invalid}
	}
	chosenBranchNum := randomSelectByFactor(randomFactors)
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
			randomFactors[chosenBranchNum], randomFactors[0] = randomFactors[0], randomFactors[chosenBranchNum]
			return randomBranch(branches[1:], randomFactors[1:])
		default:
			log.Fatalf("Unsupported result type '%v'", res.Tp)
		}
	}
	return Str(resStr.String())
}

func randomSelectByFactor(factors []int) int {
	num := rand.Intn(len(factors))
	acc := 0
	for i, f := range factors {
		acc += f
		if acc > num {
			return i
		}
	}
	return len(factors) - 1
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
