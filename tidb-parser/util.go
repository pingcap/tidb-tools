// Copyright 2018 PingCAP, Inc.
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

package parser

import (
	"fmt"

	"github.com/pingcap/tidb/ast"
)

// TableRowFormat is mysql table row format
var TableRowFormat = map[uint64]string{
	ast.RowFormatDefault:    "ROW_FORMAT=DEFAULT",
	ast.RowFormatDynamic:    "ROW_FORMAT=DYNAMIC",
	ast.RowFormatFixed:      "ROW_FORMAT=FIXED",
	ast.RowFormatCompressed: "ROW_FORMAT=COMPRESSED",
	ast.RowFormatRedundant:  "ROW_FORMAT=REDUNDANT",
	ast.RowFormatCompact:    "ROW_FORMAT=COMPACT",
}

// TableName returns `schema`.`table` or `table`
func TableName(schema, table string) string {
	if len(schema) == 0 {
		return fmt.Sprintf("`%s`", table)
	}

	return fmt.Sprintf("`%s`.`%s`", schema, table)
}

func findLastWord(literal string) int {
	index := len(literal) - 1
	for index >= 0 && literal[index] == ' ' {
		index--
	}

	for index >= 0 {
		if literal[index-1] == ' ' {
			return index
		}
		index--
	}
	return index
}

func findTableDefineIndex(literal string) string {
	for i := range literal {
		if literal[i] == '(' {
			return literal[i:]
		}
	}
	return ""
}
