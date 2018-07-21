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
	"strings"

	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/types"
)

func analyzeIndexColNames(cols []*ast.IndexColName) string {
	indexColumnList := make([]string, 0, len(cols))
	for _, indexCol := range cols {
		length := ""
		if indexCol.Length != types.UnspecifiedLength {
			length = fmt.Sprintf("(%d)", indexCol.Length)
		}

		indexColumnList = append(indexColumnList, fmt.Sprintf("%s%s", analyzeColumnName(indexCol.Column), length))
	}

	return strings.Join(indexColumnList, ",")
}

// AnalyzeIndexOption returns index option text
func AnalyzeIndexOption(option *ast.IndexOption) string {
	if option == nil {
		return ""
	}

	tp := option.Tp.String()
	if len(tp) > 0 {
		tp = fmt.Sprintf("USING %s", tp)
	}

	if len(option.Comment) > 0 {
		return fmt.Sprintf("%s COMMENT \"%s\"", tp, option.Comment)
	}

	return tp
}
