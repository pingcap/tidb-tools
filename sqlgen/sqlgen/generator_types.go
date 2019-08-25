package sqlgen

import "log"

// ResultType is used to determine whether a Result is valid.
type ResultType int

const (
	// PlainString indicates the result is a plain string
	PlainString ResultType = iota
	// Invalid indicates the result is invalid.
	Invalid
)

// Result stands for the result of Function evaluation.
type Result struct {
	Tp    ResultType
	Value string
}

// Str returns a PlainString Result.
func Str(str string) Result {
	return Result{Tp: PlainString, Value: str}
}

// Function is a representation of a productions in bnf file.
type Function interface {
	Name() string
	Call() Result
	Cancel()
}

// OrType is used to mark the end of a production body.
type OrType struct{}

// Name implement Function's Name.
func (o OrType) Name() string {
	return "or"
}

// Call implement Function's Call.
func (o OrType) Call() Result {
	log.Fatal("Calling a OrType is impossible")
	return Result{Tp: Invalid}
}

// Cancel implement Function's Cancel.
func (o OrType) Cancel() {}

// isBranch is used to check whether a Function is OrType.
func isBranch(fn Function) bool {
	_, ok := fn.(OrType)
	return ok
}

// Or is a instance of OrType.
var Or = OrType{}

// Branch represents a branch of production.
type Branch struct {
	fns          [][]Function
	randomFactor []int
}

// Br is a constructor of Branch.
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

// RandomFactor is used to specifiy the random probability of each branch.
func (b *Branch) RandomFactor(factor ...int) *Branch {
	if len(factor) != len(b.randomFactor) {
		log.Fatalf("Incorrect random factor length in {%v}. Branches number: %d, random factor number: %d", b.fns, len(b.randomFactor), len(factor))
	}
	for i := range b.randomFactor {
		b.randomFactor[i] = factor[i]
	}
	return b
}

// Eval is used to drive the selection of a random branch.
func (b *Branch) Eval() Result {
	return randomBranch(b.fns, b.randomFactor)
}
