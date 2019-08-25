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
	"fmt"
	"testing"
)

func TestParser(t *testing.T) {
	parser := NewParser()
	bnf := `simple_statement[3]: alter_database_stmt test [5]
| alter_event_stmt
| alter_function_stmt
| alter_instance_stmt [2]
| alter_logfile_stmt
| alter_procedure_stmt
| alter_resource_group_stmt`
	prod, _, _ := parser.Parse(bnf)
	expect := `simple_statement [3]: alter_database_stmt test
| alter_event_stmt
| alter_function_stmt
| alter_instance_stmt
| alter_logfile_stmt
| alter_procedure_stmt
| alter_resource_group_stmt
`
	res := fmt.Sprintf("%v", prod)
	if res != expect {
		t.Errorf("expect \n%s\n, but get \n%s\n", expect, res)
	}
}

func TestParser2(t *testing.T) {
	parser := NewParser()
	bnf := `create_table_stmt: CREATE opt_temporary TABLE_SYM opt_if_not_exists table_ident '(' table_element_list ')' opt_create_table_options_etc
| CREATE opt_temporary TABLE_SYM opt_if_not_exists table_ident opt_create_table_options_etc
| CREATE opt_temporary TABLE_SYM opt_if_not_exists table_ident LIKE table_ident
| CREATE opt_temporary TABLE_SYM opt_if_not_exists table_ident '(' LIKE table_ident ')'`
	_, _, err := parser.Parse(bnf)
	if err != nil {
		t.Error(err)
	}
}

func TestProductionString(t *testing.T) {
	parser := NewParser()
	bnf := `simple_statement[3]: alter_database_stmt test [5]
| alter_event_stmt
| alter_function_stmt
| alter_instance_stmt [2]
| alter_logfile_stmt
| alter_procedure_stmt
| alter_resource_group_stmt`
	prod, _, _ := parser.Parse(bnf)
	prodStr := prod.String()
	prod2, _, _ := parser.Parse(prodStr)
	prodStr2 := prod2.String()
	if prodStr != prodStr2 {
		t.Errorf("output string inconsistent: %s, %s", prodStr, prodStr2)
	}
}
