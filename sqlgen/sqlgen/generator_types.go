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

// InvalidF return a functions that returns invalid result.
func InvalidF() func() Result {
	return func() Result {
		return Result{Tp: Invalid}
	}
}

// Str returns a PlainString Result.
func Str(str string) Result {
	return Result{Tp: PlainString, Value: str}
}

// Fn is callable object, an implementation for sqlgen.Function.
type Fn struct {
	Name string
	F    func() Result
}

// Const is a Fn, which simply returns str.
func Const(str string) Fn {
	return Fn{Name: str, F: func() Result {
		return Result{Tp: PlainString, Value: str}
	}}
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

func Or(branches ...AndType) Result {
	var rfs []int
	for _, b := range branches {
		rfs = append(rfs, b.randFactor)
	}
	return randomBranch(branches, rfs)
}

func And(item ...Fn) AndType {
	return AndType{item: item, randFactor: 1}
}

type AndType struct {
	item       []Fn
	randFactor int
}

func (at AndType) RandomFactor(randomFactor int) AndType {
	at.randFactor = randomFactor
	return AndType{at.item, at.randFactor}
}
