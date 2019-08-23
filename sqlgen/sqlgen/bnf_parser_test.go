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
	if fmt.Sprintf("%v", prod) != `&{simple_statement 3 [{[ test  alter_database_stmt] 5} {[ alter_event_stmt] 0} {[ alter_function_stmt] 0} {[ alter_instance_stmt] 2} {[ alter_logfile_stmt] 0} {[ alter_procedure_stmt] 0} {[ alter_resource_group_stmt] 0}]}` {
		t.Fail()
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
