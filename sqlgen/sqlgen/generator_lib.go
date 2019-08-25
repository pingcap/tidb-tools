package sqlgen

import (
	"log"
	"math/rand"
	"strings"
)

func randomBranch(branches []AndType, randomFactors []int) Result {
	if len(branches) <= 0 {
		return Result{Tp: Invalid}
	}
	chosenBranchNum := randomSelectByFactor(randomFactors)
	chosenBranch := branches[chosenBranchNum]

	var doneF []Function
	var resStr strings.Builder
	for i, f := range chosenBranch.item {
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
	num := rand.Intn(sum(factors))
	acc := 0
	for i, f := range factors {
		acc += f
		if acc > num {
			return i
		}
	}
	return len(factors) - 1
}

func sum(is []int) int {
	total := 0
	for _, v := range is {
		total += v
	}
	return total
}
