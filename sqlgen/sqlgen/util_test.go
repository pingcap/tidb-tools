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

import (
	"bufio"
	"bytes"
	"testing"
)

func TestBreadthFirstSearch(t *testing.T) {
	bnf := `
		start: A | B | C
		
		A: 'a' | 'a' B 
		
		B: 'b' | A 
		
		C: 'C'
		
		D: 'D'`
	p, err := parseProdStr(bufio.NewReader(bytes.NewBufferString(bnf)))

	if err != nil {
		t.Error(err)
	}
	prodMap := BuildProdMap(p)
	rs, err := breadthFirstSearch("start", prodMap)
	if err != nil {
		t.Error(err)
	}
	if len(p) == len(rs) || len(rs) == 0 {
		t.Errorf("wrong length, origin: %d, pruned: %d", len(p), len(rs))
	}
}

func TestParseProdStr(t *testing.T) {
	bnf := `
		start: A | B | C
		
		A: 'a' | 'a' B 
		
		B: 'b' | A 
		
		C: 'C'
		
		D: 'D'`
	expected := []Production{
		{head: "start", bodyList: BodyList{body("A"), body("B"), body("C")}},
		{head: "A", bodyList: BodyList{body("'a'"), body("'a'", "B")}},
		{head: "B", bodyList: BodyList{body("'b'"), body("A")}},
		{head: "C", bodyList: BodyList{body("'C'")}},
		{head: "D", bodyList: BodyList{body("'D'")}},
	}
	prods, err := parseProdStr(bufio.NewReader(bytes.NewBufferString(bnf)))
	if err != nil {
		t.Error(err)
	}
	if len(expected) != len(prods) {
		t.Error("The length of parseProdStr result is not expected")
	}
	for i, e := range expected {
		if prods[i].head != e.head || len(prods[i].bodyList) != len(e.bodyList) {
			for j, b := range e.bodyList {
				if !equalStrArr(prods[i].bodyList[j].seq, b.seq) {
					t.Error("Incorrect body seq")
				}
			}
		}
	}
}

func body(str ...string) Body {
	return Body{seq: str}
}

func equalStrArr(a1, a2 []string) bool {
	if len(a1) != len(a2) {
		return false
	}
	for i := range a1 {
		if a1[i] != a2[i] {
			return false
		}
	}
	return true
}

func TestProdSplitter(t *testing.T) {
	buf := bufio.NewReader(bytes.NewBufferString(`deallocate_or_drop: DEALLOCATE_SYM
| DROP

prepare: PREPARE_SYM ident FROM prepare_src`))
	res := splitProdStr(buf)

	expected := []string{"deallocate_or_drop: DEALLOCATE_SYM\n| DROP\n", "prepare: PREPARE_SYM ident FROM prepare_src"}
	for i, v := range res {
		if v != expected[i] {
			t.Errorf("expect: '%s', get: '%s'", expected[i], v)
		}
	}
}

func TestIsWhitespace(t *testing.T) {
	if !isWhitespace("\t \n") {
		t.Error()
	}
	if isWhitespace("  t  ") {
		t.Error()
	}
	if !isWhitespace("\n  \t  ") {
		t.Error()
	}
}

func TestIsIdentifier(t *testing.T) {
	invalidCases := []string{"%empty", "3sdaf", "sfsge^se", "xe#", "THIS_IS_NOT+ID"}
	validCases := []string{"ident", "ident2", "a2f", "com_t", "THIS_IS_ID"}

	for _, ic := range invalidCases {
		if isIdentifier(ic) {
			t.Errorf("%s is not an identifier, however isIdentifier report true", ic)
		}
	}
	for _, c := range validCases {
		if !isIdentifier(c) {
			t.Errorf("%s is an identifier, however isIdentifier report false", c)
		}
	}
}
