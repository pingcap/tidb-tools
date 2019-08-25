package sqlgen

import (
	"bufio"
	"bytes"
	"testing"
)

func TestBreadthFirstSearch(t *testing.T) {
	p, err := ParseYacc("mysql80_bnf_complete.txt")
	if err != nil {
		t.Error(err)
	}
	prodMap := BuildProdMap(p)
	rs, err := breadthFirstSearch("create_table_stmt", prodMap)
	if err != nil {
		t.Error(err)
	}
	if len(p) == len(rs) || len(rs) == 0 {
		t.Errorf("wrong length, origin: %d, pruned: %d", len(p), len(rs))
	}
}

func TestParseYacc(t *testing.T) {
	p, err := ParseYacc("mysql80_bnf_complete.txt")
	if err != nil {
		t.Errorf("%v %v", p, err)
	}

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