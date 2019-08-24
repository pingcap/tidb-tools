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
