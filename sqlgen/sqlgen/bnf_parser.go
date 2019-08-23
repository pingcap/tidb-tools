// Code generated by goyacc DO NOT EDIT.
// CAUTION: Generated file - DO NOT EDIT.

// Copyright 2013 The ql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSES/QL-LICENSE file.

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

import __yyfmt__ "fmt"

import (
	"strconv"
)

type yySymType struct {
	yys    int
	offset int // offset
	item   interface{}
	ident  string
}

type yyXError struct {
	state, xsym int
}

const (
	yyDefault  = 57352
	yyEOFCode  = 57344
	Colon      = 57346
	LeftBr     = 57348
	OrBranch   = 57347
	RightBr    = 57349
	yyErrCode  = 57345
	identifier = 57350
	number     = 57351

	yyMaxDepth = 200
	yyTabOfs   = -9
)

var (
	yyXLAT = map[int]int{
		57344: 0,  // $end (10x)
		57350: 1,  // identifier (8x)
		57347: 2,  // OrBranch (8x)
		57348: 3,  // LeftBr (5x)
		57346: 4,  // Colon (3x)
		57355: 5,  // NumberOpt (3x)
		57353: 6,  // Body (2x)
		57354: 7,  // BodyList (1x)
		57356: 8,  // Production (1x)
		57349: 9,  // RightBr (1x)
		57357: 10, // Start (1x)
		57352: 11, // $default (0x)
		57345: 12, // error (0x)
		57351: 13, // number (0x)
	}

	yySymNames = []string{
		"$end",
		"identifier",
		"OrBranch",
		"LeftBr",
		"Colon",
		"NumberOpt",
		"Body",
		"BodyList",
		"Production",
		"RightBr",
		"Start",
		"$default",
		"error",
		"number",
	}

	yyReductions = []struct{ xsym, components int }{
		{0, 1},
		{10, 1},
		{8, 4},
		{7, 2},
		{7, 4},
		{6, 2},
		{6, 1},
		{5, 0},
		{5, 3},
	}

	yyXErrors = map[yyXError]string{}

	yyParseTab = [17][]uint8{
		// 0
		{1: 12, 8: 11, 10: 10},
		{9},
		{8},
		{3: 14, 2, 13},
		{4: 17},
		// 5
		{1: 15},
		{9: 16},
		{1, 2: 1, 4: 1},
		{1: 20, 6: 19, 18},
		{7, 2: 23},
		// 10
		{2, 22, 2, 14, 5: 21},
		{3, 3, 3, 3},
		{6, 2: 6},
		{4, 4, 4, 4},
		{1: 20, 6: 24},
		// 15
		{2, 22, 2, 14, 5: 25},
		{5, 2: 5},
	}
)

var yyDebug = 0

type yyLexer interface {
	Lex(lval *yySymType) int
	Errorf(format string, a ...interface{}) error
	AppendError(err error)
	Errors() (warns []error, errs []error)
}

type yyLexerEx interface {
	yyLexer
	Reduced(rule, state int, lval *yySymType) bool
}

func yySymName(c int) (s string) {
	x, ok := yyXLAT[c]
	if ok {
		return yySymNames[x]
	}

	return __yyfmt__.Sprintf("%d", c)
}

func yylex1(yylex yyLexer, lval *yySymType) (n int) {
	n = yylex.Lex(lval)
	if n <= 0 {
		n = yyEOFCode
	}
	if yyDebug >= 3 {
		__yyfmt__.Printf("\nlex %s(%#x %d), lval: %+v\n", yySymName(n), n, n, lval)
	}
	return n
}

func yyParse(yylex yyLexer, parser *Parser) int {
	const yyError = 12

	yyEx, _ := yylex.(yyLexerEx)
	var yyn int
	parser.yylval = yySymType{}
	parser.yyVAL = yySymType{}
	yyS := parser.cache

	Nerrs := 0   /* number of errors */
	Errflag := 0 /* error recovery flag */
	yyerrok := func() {
		if yyDebug >= 2 {
			__yyfmt__.Printf("yyerrok()\n")
		}
		Errflag = 0
	}
	_ = yyerrok
	yystate := 0
	yychar := -1
	var yyxchar int
	var yyshift int
	yyp := -1
	goto yystack

ret0:
	return 0

ret1:
	return 1

yystack:
	/* put a state and value onto the stack */
	yyp++
	if yyp >= len(yyS) {
		nyys := make([]yySymType, len(yyS)*2)
		copy(nyys, yyS)
		yyS = nyys
		parser.cache = yyS
	}
	yyS[yyp] = parser.yyVAL
	yyS[yyp].yys = yystate

yynewstate:
	if yychar < 0 {
		yychar = yylex1(yylex, &parser.yylval)
		var ok bool
		if yyxchar, ok = yyXLAT[yychar]; !ok {
			yyxchar = len(yySymNames) // > tab width
		}
	}
	if yyDebug >= 4 {
		var a []int
		for _, v := range yyS[:yyp+1] {
			a = append(a, v.yys)
		}
		__yyfmt__.Printf("state stack %v\n", a)
	}
	row := yyParseTab[yystate]
	yyn = 0
	if yyxchar < len(row) {
		if yyn = int(row[yyxchar]); yyn != 0 {
			yyn += yyTabOfs
		}
	}
	switch {
	case yyn > 0: // shift
		yychar = -1
		parser.yyVAL = parser.yylval
		yystate = yyn
		yyshift = yyn
		if yyDebug >= 2 {
			__yyfmt__.Printf("shift, and goto state %d\n", yystate)
		}
		if Errflag > 0 {
			Errflag--
		}
		goto yystack
	case yyn < 0: // reduce
	case yystate == 1: // accept
		if yyDebug >= 2 {
			__yyfmt__.Println("accept")
		}
		goto ret0
	}

	if yyn == 0 {
		/* error ... attempt to resume parsing */
		switch Errflag {
		case 0: /* brand new error */
			if yyDebug >= 1 {
				__yyfmt__.Printf("no action for %s in state %d\n", yySymName(yychar), yystate)
			}
			msg, ok := yyXErrors[yyXError{yystate, yyxchar}]
			if !ok {
				msg, ok = yyXErrors[yyXError{yystate, -1}]
			}
			if !ok && yyshift != 0 {
				msg, ok = yyXErrors[yyXError{yyshift, yyxchar}]
			}
			if !ok {
				msg, ok = yyXErrors[yyXError{yyshift, -1}]
			}
			if !ok || msg == "" {
				msg = "syntax error"
			}
			// ignore goyacc error message
			yylex.AppendError(yylex.Errorf(""))
			Nerrs++
			fallthrough

		case 1, 2: /* incompletely recovered error ... try again */
			Errflag = 3

			/* find a state where "error" is a legal shift action */
			for yyp >= 0 {
				row := yyParseTab[yyS[yyp].yys]
				if yyError < len(row) {
					yyn = int(row[yyError]) + yyTabOfs
					if yyn > 0 { // hit
						if yyDebug >= 2 {
							__yyfmt__.Printf("error recovery found error shift in state %d\n", yyS[yyp].yys)
						}
						yystate = yyn /* simulate a shift of "error" */
						goto yystack
					}
				}

				/* the current p has no shift on "error", pop stack */
				if yyDebug >= 2 {
					__yyfmt__.Printf("error recovery pops state %d\n", yyS[yyp].yys)
				}
				yyp--
			}
			/* there is no state on the stack with an error shift ... abort */
			if yyDebug >= 2 {
				__yyfmt__.Printf("error recovery failed\n")
			}
			goto ret1

		case 3: /* no shift yet; clobber input char */
			if yyDebug >= 2 {
				__yyfmt__.Printf("error recovery discards %s\n", yySymName(yychar))
			}
			if yychar == yyEOFCode {
				goto ret1
			}

			yychar = -1
			goto yynewstate /* try again in the same state */
		}
	}

	r := -yyn
	x0 := yyReductions[r]
	x, n := x0.xsym, x0.components
	yypt := yyp
	_ = yypt // guard against "declared and not used"

	yyp -= n
	if yyp+1 >= len(yyS) {
		nyys := make([]yySymType, len(yyS)*2)
		copy(nyys, yyS)
		yyS = nyys
		parser.cache = yyS
	}
	parser.yyVAL = yyS[yyp+1]

	/* consult goto table to find next state */
	exState := yystate
	yystate = int(yyParseTab[yyS[yyp].yys][x]) + yyTabOfs
	/* reduction by production r */
	if yyDebug >= 2 {
		__yyfmt__.Printf("reduce using rule %v (%s), and goto state %d\n", r, yySymNames[x], yystate)
	}

	switch r {
	case 1:
		{
			parser.result = yyS[yypt-0].item.(*Production)
		}
	case 2:
		{
			parser.yyVAL.item = &Production{head: yyS[yypt-3].ident, maxLoop: yyS[yypt-2].item.(int), bodyList: yyS[yypt-0].item.(BodyList)}
		}
	case 3:
		{
			body := yyS[yypt-1].item.(Body)
			body.randomFactor = yyS[yypt-0].item.(int)
			parser.yyVAL.item = BodyList{body}
		}
	case 4:
		{
			body := yyS[yypt-1].item.(Body)
			body.randomFactor = yyS[yypt-0].item.(int)
			parser.yyVAL.item = append(yyS[yypt-3].item.(BodyList), body)
		}
	case 5:
		{
			body := yyS[yypt-1].item.(Body)
			body.seq = append(body.seq, yyS[yypt-0].ident)
			parser.yyVAL.item = body
		}
	case 6:
		{
			parser.yyVAL.item = Body{seq: []string{yyS[yypt-0].ident}}
		}
	case 7:
		{
			parser.yyVAL.item = 1
		}
	case 8:
		{
			num, err := strconv.ParseInt(yyS[yypt-1].ident, 10, 32)
			if err != nil {
				yylex.AppendError(yylex.Errorf(err.Error()))
				return 1
			}
			parser.yyVAL.item = int(num)
		}

	}

	if yyEx != nil && yyEx.Reduced(r, exState, &parser.yyVAL) {
		return -1
	}
	goto yystack /* stack new state and value */
}
