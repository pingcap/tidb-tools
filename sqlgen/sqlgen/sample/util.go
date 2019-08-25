package sample

import (
	. "github.com/pingcap/tidb-tools/sqlgen/sqlgen"
)

var counter = map[string]int{}

const maxLoopback = 2

// Fn is callable object, an implementation for sqlgen.Function.
type Fn struct {
	name string
	f    func() Result
}

// Name implements Function.Name.
func (fn Fn) Name() string {
	return fn.name
}

// Call implements Function.Call.
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

// Cancel implements Function.Cancel.
func (fn Fn) Cancel() {
	counter[fn.name]--
}

// Const is a Fn, which simply returns str.
func Const(str string) Fn {
	return Fn{name: str, f: func() Result {
		return Result{Tp: PlainString, Value: str}
	}}
}
