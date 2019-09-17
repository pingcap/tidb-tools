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

import "strings"

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

func InvalidResult() Result {
	return innerInvalidResult
}

var innerInvalidResult = Result{Tp: Invalid}

// InvalidFn return a functions that returns invalid result.
func InvalidFn() func() Result {
	return innerInvalidFn
}

var innerInvalidFn = func() Result {
	return Result{Tp: Invalid}
}

// StrResult returns a PlainString Result.
func StrResult(str string) Result {
	return Result{Tp: PlainString, Value: str}
}

// Fn is a callable object.
type Fn struct {
	Name         string
	F            func() Result
	RandomFactor int
}

func NewFn(name string, fn func() Fn) Fn {
	return Fn{
		Name: name,
		F: func() Result {
			return fn().F()
		},
		RandomFactor: 1,
	}
}

func NewConstFn(name string, fn Fn) Fn {
	return Fn{
		Name: name,
		F: func() Result {
			return fn.F()
		},
		RandomFactor: 1,
	}
}

func (f Fn) SetRF(randomFactor int) Fn {
	return Fn{
		Name:         f.Name,
		F:            f.F,
		RandomFactor: randomFactor,
	}
}

// Str is a Fn which simply returns str.
func Str(str string) Fn {
	return Fn{
		RandomFactor: 1,
		F: func() Result {
			return StrResult(str)
		}}
}

func Strs(strs ...string) Fn {
	return Fn{
		RandomFactor: 1,
		F: func() Result {
			return StrResult(strings.Join(strs, " "))
		},
	}
}

// EmptyFn is a Fn which simply returns empty string.
func EmptyFn() Fn {
	return innerEmptyFn
}

var innerEmptyFn = Fn{
	RandomFactor: 1,
	F: func() Result {
		return Result{Tp: PlainString, Value: ""}
	},
}
