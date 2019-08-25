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

func Or(branches ...AndType) Result {
	var rfs []int
	for _, b := range branches {
		rfs = append(rfs, b.randFactor)
	}
	return randomBranch(branches, rfs)
}

func And(item ...Function) AndType {
	return AndType{item: item, randFactor: 1}
}

type AndType struct {
	item []Function
	randFactor int
}

func (at AndType) RandomFactor(randomFactor int) AndType {
	at.randFactor = randomFactor
	return AndType{at.item, at.randFactor}
}

