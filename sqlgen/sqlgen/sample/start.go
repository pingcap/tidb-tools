package sample

import (
	"log"
	"math/rand"
	"time"

	. "github.com/pingcap/tidb-tools/sqlgen/sqlgen"
)

// Generate is used to generate a string according to bnf grammar.
var Generate = generate()

func generate() func() string {
	rand.Seed(time.Now().UnixNano())
	GenPlugins = append(GenPlugins, NewMaxLoopCounter(3))
	retFn := func() string {
		res := start.F()
		switch res.Tp {
		case PlainString:
			return res.Value
		case Invalid:
			log.Println("Invalid SQL")
			return ""
		default:
			log.Fatalf("Unsupported result type '%v'", res.Tp)
			return ""
		}
	}

	start = NewFn("start",
		Or(
			A,
			B,
			C,
		),
	)

	A = NewFn("A",
		Or(
			Str("a"),
			And(Str("a"), B),
		),
	)

	B = NewFn("B",
		Or(
			Str("b"),
			A,
		),
	)

	C = NewFn("C", Str("C"))

	return retFn
}
