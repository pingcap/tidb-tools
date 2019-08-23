%{
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

import (
	"strconv"
)

%}

%union {
	offset int // offset
	item	   interface{}
	ident      string
}

%type 	<item>
	Production
	BodyList
	Body
	NumberOpt

%token	<item>
	Colon
	OrBranch
	LeftBr
	RightBr

%type	<ident>
	identifier      "identifier"

%token  <ident>
	identifier
	number

%right identifier

%start	Start

%%

Start: 	Production
	{
		parser.result = $1.(*Production)
	}

Production:
	identifier NumberOpt Colon BodyList
	{
		$$ = &Production{ head: $1, maxLoop: $2.(int), bodyList: $4.(BodyList) }
	}

BodyList:
	Body NumberOpt
	{
		body := $1.(Body)
		body.randomFactor = $2.(int)
		$$ = BodyList{body}
	}
|	BodyList OrBranch Body NumberOpt
	{
		body := $3.(Body)
        	body.randomFactor = $4.(int)
		$$ = append($1.(BodyList), body)
	}

Body:
	Body identifier
	{
		body := $1.(Body)
		body.seq = append(body.seq, $2)
		$$ = body
	}
|	identifier
	{
		$$ = Body{seq: []string{$1}}
	}

NumberOpt:
	{
		$$ = 1
	}
|	LeftBr identifier RightBr
	{
		num, err := strconv.ParseInt($2, 10, 32)
		if err != nil {
			yylex.AppendError(yylex.Errorf(err.Error()))
			return 1
		}
		$$ = int(num)
	}

%%
