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

	"github.com/juju/errors"
	"github.com/pingcap/tidb/ast"
)

func analyzeTableOptions(options []*ast.TableOption) (string, error) {
	if len(options) == 0 {
		return "", nil
	}

	optStrs := make([]string, 0, len(options))
	for _, opt := range options {
		optStr, err := analyzeTableOption(opt)
		if err != nil {
			return "", errors.Trace(err)
		}

		optStrs = append(optStrs, optStr)
	}

	return strings.Join(optStrs, " "), nil
}

func analyzeTableOption(option *ast.TableOption) (string, error) {
	switch option.Tp {
	case ast.TableOptionEngine:
		if option.StrValue == "" {
			return fmt.Sprintf("ENGINE=''"), nil
		}
		return fmt.Sprintf("ENGINE=%s", option.StrValue), nil
	case ast.TableOptionCollate:
		return fmt.Sprintf("DEFAULT COLLATE %s", option.StrValue), nil
	case ast.TableOptionCharset:
		return fmt.Sprintf("DEFAULT CHARACTER SET %s", option.StrValue), nil
	case ast.TableOptionAutoIncrement:
		return fmt.Sprintf("AUTO_INCREMENT=%d", option.UintValue), nil
	case ast.TableOptionComment:
		return fmt.Sprintf("COMMENT='%s'", option.StrValue), nil
	case ast.TableOptionAvgRowLength:
		return fmt.Sprintf("AVG_ROW_LENGTH=%d", option.UintValue), nil
	case ast.TableOptionConnection:
		return fmt.Sprintf("CONNECTION='%s'", option.StrValue), nil
	case ast.TableOptionCheckSum:
		return fmt.Sprintf("CHECKSUM=%d", option.UintValue), nil
	case ast.TableOptionPassword:
		return fmt.Sprintf("PASSWORD='%s'", option.StrValue), nil
	case ast.TableOptionCompression:
		return fmt.Sprintf("COMPRESSION='%s'", option.StrValue), nil
	case ast.TableOptionKeyBlockSize:
		return fmt.Sprintf("KEY_BLOCK_SIZE=%d", option.UintValue), nil
	case ast.TableOptionMaxRows:
		return fmt.Sprintf("MAX_ROWS=%d", option.UintValue), nil
	case ast.TableOptionMinRows:
		return fmt.Sprintf("MIN_ROWS=%d", option.UintValue), nil
	case ast.TableOptionDelayKeyWrite:
		return fmt.Sprintf("DELAY_KEY_WRITE=%d", option.UintValue), nil
	case ast.TableOptionRowFormat:
		return fmt.Sprintf("%s", TableRowFormat[option.UintValue]), nil
	case ast.TableOptionStatsPersistent:
		return fmt.Sprintf("STATS_PERSISTENT=DEFAULT"), nil
	case ast.TableOptionShardRowID:
		return fmt.Sprintf("SHARD_ROW_ID_BITS=%d", option.UintValue), nil
	}

	return "", nil
}

// AnalyzeAlterTableSpec returns spec text
func AnalyzeAlterTableSpec(spec *ast.AlterTableSpec) (string, error) {
	switch spec.Tp {
	case ast.AlterTableOption:
		return analyzeTableOptions(spec.Options)

	case ast.AlterTableAddColumns:
		colDefStrs := make([]string, 0, len(spec.NewColumns))
		for _, newColumn := range spec.NewColumns {
			colDefStr := analyzeColumnDef(newColumn)
			if spec.Position != nil {
				colDefStr += analyzeColumnPosition(spec.Position)
			}
			colDefStrs = append(colDefStrs, colDefStr)
		}

		if len(colDefStrs) == 1 {
			return fmt.Sprintf("ADD COLUMN %s", colDefStrs[0]), nil
		}
		return fmt.Sprintf("ADD COLUMN (%s)", strings.Join(colDefStrs, ",")), nil

	case ast.AlterTableDropColumn:
		return fmt.Sprintf("DROP COLUMN %s", analyzeColumnName(spec.OldColumnName)), nil

	case ast.AlterTableDropIndex:
		return fmt.Sprintf("DROP INDEX `%s`", escapeName(spec.Name)), nil

	case ast.AlterTableAddConstraint:
		return analyzeConstraint(spec.Constraint), nil

	case ast.AlterTableDropForeignKey:
		return fmt.Sprintf("DROP FOREIGN KEY `%s`", escapeName(spec.Name)), nil

	case ast.AlterTableModifyColumn:
		// TiDB doesn't support alter table modify column charset and collation.
		defStr := fmt.Sprintf("MODIFY COLUMN %s", analyzeColumnDef(spec.NewColumns[0]))
		if spec.Position != nil {
			defStr += analyzeColumnPosition(spec.Position)
		}
		return defStr, nil

	case ast.AlterTableChangeColumn:
		// TiDB doesn't support alter table change column charset and collation.
		defStr := fmt.Sprintf("CHANGE COLUMN  %s %s",
			analyzeColumnName(spec.OldColumnName),
			analyzeColumnDef(spec.NewColumns[0]))
		if spec.Position != nil {
			defStr += analyzeColumnPosition(spec.Position)
		}
		return defStr, nil

	case ast.AlterTableRenameTable:
		return fmt.Sprintf("RENAME TO %s", TableName(spec.NewTable.Schema.O, spec.NewTable.Name.O)), nil

	case ast.AlterTableAlterColumn:
		defStr := bytes.Buffer{}
		fmt.Fprintf(&defStr, "ALTER COLUMN %s ", analyzeColumnName(spec.NewColumns[0].Name))
		if options := spec.NewColumns[0].Options; options != nil {
			fmt.Fprint(&defStr, "SET DEFAULT ")
			options[0].Expr.Format(&defStr)
		} else {
			fmt.Fprint(&defStr, "DROP DEFAULT")
		}
		return defStr.String(), nil

	case ast.AlterTableDropPrimaryKey:
		return "DROP PRIMARY KEY", nil

	}

	return "", nil
}
