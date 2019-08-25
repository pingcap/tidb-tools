package sqlgen

import (
	"log"
	"math/rand"
	"strings"
)

var GenPlugins []Plugin

type Plugin interface{}

type ProductionListener interface {
	Plugin
	BeforeProductionGen(fn *Fn)
	AfterProductionGen(fn *Fn, result *Result)
	ProductionCancel(fn *Fn)
}

func randomBranch(branches []AndType, randomFactors []int) Result {
	if len(branches) <= 0 {
		return Result{Tp: Invalid}
	}
	chosenBranchNum := randomSelectByFactor(randomFactors)
	chosenBranch := branches[chosenBranchNum]

	var doneF []Fn
	var resStr strings.Builder
	for i, f := range chosenBranch.item {
		forEachProdListener(func(p ProductionListener) {
			p.BeforeProductionGen(&f)
		})
		res := f.F()
		forEachProdListener(func(p ProductionListener) {
			p.AfterProductionGen(&f, &res)
		})
		switch res.Tp {
		case PlainString:
			doneF = append(doneF, f)
			if i != 0 {
				resStr.WriteString(" ")
			}
			resStr.WriteString(res.Value)
		case Invalid:
			for _, df := range doneF {
				forEachProdListener(func(p ProductionListener) {
					p.ProductionCancel(&df)
				})
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

func forEachProdListener(fn func(ProductionListener)) {
	for _, p := range GenPlugins {
		if lp, ok := p.(ProductionListener); ok {
			fn(lp)
		}
	}
}

func sum(is []int) int {
	total := 0
	for _, v := range is {
		total += v
	}
	return total
}
