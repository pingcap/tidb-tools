package sqlgen

import "testing"

func TestNewGenerator(t *testing.T) {
	BuildLib("sample_bnf.txt", "start", "sample", ".", []string{"maxLoopLimit"})
}
