package sample

import (
	"testing"
)

func TestA(t *testing.T) {
	for i := 0; i < 10; i++ {
		t.Log(Generate())
	}
}
