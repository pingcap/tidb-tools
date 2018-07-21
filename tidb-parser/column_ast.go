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
	"bytes"
	"fmt"
	"strings"

	"github.com/pingcap/tidb/ast"
)

func analyzeColumnPosition(pos *ast.ColumnPosition) string {
	var sql string
	switch pos.Tp {
	case ast.ColumnPositionNone:
	case ast.ColumnPositionFirst:
		sql = " FIRST"
	case ast.ColumnPositionAfter:
		colName := pos.RelativeColumn.Name.O
		sql = fmt.Sprintf(" AFTER `%s`", escapeName(colName))
	}
	return sql
}

func analyzeColumnName(name *ast.ColumnName) string {
	nameStr := ""
	if name.Schema.O != "" {
		nameStr += fmt.Sprintf("`%s`.", escapeName(name.Schema.O))
	}
	if name.Table.O != "" {
		nameStr += fmt.Sprintf("`%s`.", escapeName(name.Table.O))
	}
	nameStr += fmt.Sprintf("`%s`", escapeName(name.Name.O))
	return nameStr
}

// FIXME: tidb's AST is error-some to handle more condition
func analyzeColumnOptions(options []*ast.ColumnOption) string {
	optStrs := make([]string, 0, len(options))
	for _, opt := range options {
		str := analyzeColumnOption(opt)
		if str == "" {
			continue
		}

		optStrs = append(optStrs, str)
	}

	return strings.Join(optStrs, " ")
}

func analyzeColumnOption(opt *ast.ColumnOption) string {
	str := bytes.Buffer{}
	switch opt.Tp {
	case ast.ColumnOptionNotNull:
		return "NOT NULL"
	case ast.ColumnOptionNull:
		return "NULL"
	case ast.ColumnOptionDefaultValue:
		opt.Expr.Format(&str)
		return fmt.Sprintf("DEFAULT %s", str.String())
	case ast.ColumnOptionAutoIncrement:
		return "AUTO_INCREMENT"
	case ast.ColumnOptionUniqKey:
		return "UNIQUE KEY"
	case ast.ColumnOptionPrimaryKey:
		return "PRIMARY KEY"
	case ast.ColumnOptionComment:
		opt.Expr.Format(&str)
		return fmt.Sprintf("COMMENT '%s'", str.String())
	case ast.ColumnOptionOnUpdate:
		opt.Expr.Format(&str)
		return fmt.Sprintf("ON UPDATE %s", str.String())
	case ast.ColumnOptionReference:
		return analyzeColumnReference(opt.Refer)
	case ast.ColumnOptionGenerated:
		return fmt.Sprintf("GENERATED ALWAYS AS (%s) %s", opt.Expr.Text(), analyzeVirtualOrStored(opt.Stored))
	}

	return ""
}

func analyzeColumnReference(def *ast.ReferenceDef) string {
	ondelete := ""
	if def.OnDelete.ReferOpt != ast.ReferOptionNoOption {
		ondelete = fmt.Sprintf("ON DELETE %s", def.OnDelete.ReferOpt)
	}

	onUpdate := ""
	if def.OnUpdate.ReferOpt != ast.ReferOptionNoOption {
		onUpdate = fmt.Sprintf("ON UPDATE %s", def.OnUpdate.ReferOpt)
	}

	return fmt.Sprintf("REFERENCES %s (%s)%s%s", TableName(def.Table.Schema.O, def.Table.Name.O), analyzeIndexColNames(def.IndexColNames), ondelete, onUpdate)
}

func analyzeVirtualOrStored(stored bool) string {
	if stored {
		return "STORED"
	}

	return "VIRTUAL"
}

func analyzeColumnDef(def *ast.ColumnDef) string {
	optStrs := analyzeColumnOptions(def.Options)
	return fmt.Sprintf("%s %s %s", analyzeColumnName(def.Name), def.Tp, optStrs)
}

func analyzeConstraint(constraint *ast.Constraint) string {
	switch constraint.Tp {
	case ast.ConstraintIndex:
		return fmt.Sprintf("ADD INDEX %s (%s) %s", escapeName(constraint.Name), analyzeIndexColNames(constraint.Keys), AnalyzeIndexOption(constraint.Option))

	case ast.ConstraintUniq:
		str := "ADD CONSTRAINT"
		if constraint.Name != "" {
			str = fmt.Sprintf("%s `%s`", str, escapeName(constraint.Name))
		}
		return fmt.Sprintf("%s UNIQUE INDEX (%s) %s", str, analyzeIndexColNames(constraint.Keys), AnalyzeIndexOption(constraint.Option))

	case ast.ConstraintForeignKey:
		str := "ADD CONSTRAINT"
		if constraint.Name != "" {
			str = fmt.Sprintf("%s `%s`", str, escapeName(constraint.Name))
		}

		return fmt.Sprintf("%s FOREIGN KEY (%s) %s", str, analyzeIndexColNames(constraint.Keys), analyzeColumnReference(constraint.Refer))

	case ast.ConstraintPrimaryKey:
		str := "ADD CONSTRAINT"
		if constraint.Name != "" {
			str = fmt.Sprintf("%s `%s`", str, escapeName(constraint.Name))
		}
		return fmt.Sprintf("%s PRIMARY KEY (%s) %s", str, analyzeIndexColNames(constraint.Keys), AnalyzeIndexOption(constraint.Option))

	case ast.ConstraintFulltext:
		str := "ADD FULLTEXT INDEX"
		if constraint.Name != "" {
			str = fmt.Sprintf("%s `%s`", str, escapeName(constraint.Name))
		}
		return fmt.Sprintf("%s (%s) %s", str, analyzeIndexColNames(constraint.Keys), AnalyzeIndexOption(constraint.Option))
	}

	return ""
}
