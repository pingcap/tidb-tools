package sample

import (
	. "github.com/pingcap/tidb-tools/sqlgen/sqlgen"
)

var counter = map[string]int{}
const maxLoopback = 4

type Fn struct {
	name string
	f    func() Result
}

func (fn Fn) Name() string {
	return fn.name
}

func (fn Fn) Call() Result {
	fnName := fn.name
	// Before calling function.
	counter[fnName]++
	if counter[fnName] > maxLoopback {
		return Result{Tp: Invalid}
	}

	ret := fn.f()
	// After calling function.
	counter[fnName]--
	return ret
}

func (fn Fn) Cancel() {
	counter[fn.name]--
}

func Const(str string) Fn {
	return Fn{name: str, f: func() Result {
		return Result{Tp: PlainString, Value: str}
	}}
}
