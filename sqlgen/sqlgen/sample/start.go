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
		res := start.f()
		switch res.Tp {
		case PlainString:
			return res.Value
		case Invalid:
			log.Println("Invalid SQL")
			return ""
		default:
			log.Fatalf("Unsupported result type '%v'", res.Tp)
		}
		return "impossible to reach"
	}

	
	start = Fn{
		name: "start",
		f: func() Result {
			return Br(
				A, Or, 
				B, 
			).Eval()
		},
	}

	A = Fn{
		name: "A",
		f: func() Result {
			return Br(
				Const("a"), Or, 
				Const("a"), B, 
			).RandomFactor(1, 0).Eval()
		},
	}

	B = Fn{
		name: "B",
		f: func() Result {
			return Br(
				Const("b"), Or, 
				A, 
			).Eval()
		},
	}


	return retFn
}
