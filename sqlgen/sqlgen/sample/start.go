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
		}
		panic("impossible to reach")
	}

	start = Fn{
		Name: "start",
		F: func() Result {
			return Or(
				And(A),
				And(B),
				And(C),
			)
		},
	}

	A = Fn{
		Name: "A",
		F: func() Result {
			return Or(
				And(Const("a")),
				And(Const("a"), B),
			)
		},
	}

	B = Fn{
		Name: "B",
		F: func() Result {
			return Or(
				And(Const("b")),
				And(A),
			)
		},
	}

	C = Fn{
		Name: "C",
		F: func() Result {
			return Str("C")
		},
	}

	return retFn
}
