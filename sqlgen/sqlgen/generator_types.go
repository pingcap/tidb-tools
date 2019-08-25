// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package sqlgen

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
