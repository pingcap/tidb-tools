package sample

import (
	. "github.com/pingcap/tidb-tools/sqlgen/sqlgen"
	"log"
)

var Generate = generate()

func generate() func() string {
	initState("/home/tangenta/go/src/github.com/pingcap/tidb-tools/sqlgen/sqlgen/sample_bnf.txt", "start")
	retFn := func() string {
		res := start.f()
		switch res.Tp {
		case PlainString:
			return res.Value
		case Invalid:
			log.Println("Invalid SQL")
			return ""
		case NonExist:
			log.Fatalf("Production '%s' not found", start.name)
		default:
			log.Fatalf("Unsupported result type '%v'", res.Tp)
		}
		return "impossible to reach"
	}

	start = Fn{
		name: "start",
		f: func() Result {
			return random(a, Or,
				b,
			)
		},
	}

	a = Fn{
		name: "a",
		f: func() Result {
			return Str("A")
		},
	}

	b = Fn{
		name: "b",
		f: func() Result {
			return Str("B")
		},
	}

	return retFn
}
