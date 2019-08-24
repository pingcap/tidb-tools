package sqlgen

import "log"

type ResultType int

const (
	PlainString ResultType = iota
	Invalid
)

type Result struct {
	Tp    ResultType
	Value string
}

func Str(str string) Result {
	return Result{Tp: PlainString, Value: str}
}

type Function interface {
	Name() string
	Call() Result
	Cancel()
}

type OrType struct{}

func (o OrType) Name() string {
	return "or"
}
func (o OrType) Call() Result {
	log.Fatal("Calling a OrType is impossible")
	return Result{Tp: Invalid}
}
func (o OrType) Cancel() {}

func isBranch(fn Function) bool {
	_, ok := fn.(OrType)
	return ok
}

var Or = OrType{}

type Branch struct {
	fns          [][]Function
	randomFactor []int
}

func Br(fns ...Function) *Branch {
	brs := splitBranches(fns)
	brsLen := len(brs)
	rfs := make([]int, brsLen)
	for i := 0; i < brsLen; i++ {
		rfs[i] = 1
	}
	return &Branch{
		fns:          brs,
		randomFactor: rfs,
	}
}

func (b *Branch) RandomFactor(factor ...int) *Branch {
	if len(factor) != len(b.randomFactor) {
		log.Fatalf("Incorrect random factor length in {%v}. Branches number: %d, random factor number: %d", b.fns, len(b.randomFactor), len(factor))
	}
	for i, _ := range b.randomFactor {
		b.randomFactor[i] = factor[i]
	}
	return b
}

func (b *Branch) Eval() Result {
	return randomBranch(b.fns, b.randomFactor)
}
