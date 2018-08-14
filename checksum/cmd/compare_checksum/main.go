package main

import (
	"flag"
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"os"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb-tools/checksum"
)

func main() {
	cfg := checksum.NewConfig()
	err := cfg.Parse(os.Args[1:])
	switch errors.Cause(err) {
	case nil:
	case flag.ErrHelp:
		os.Exit(0)
	default:
		log.Errorf("parse cmd flags err %s\n", errors.ErrorStack(err))
		os.Exit(2)
	}

	go func() {
		if cfg.ProfilePort != 0 {
			http.ListenAndServe(fmt.Sprintf(":%d", cfg.ProfilePort), nil)
		}
	}()

	log.SetLevelByString(cfg.LogLevel)
	if len(cfg.LogFile) > 0 {
		log.SetOutputByName(cfg.LogFile)
		log.SetHighlighting(false)
	}

	comparer := checksum.NewComparer(cfg.Compares)
	err = comparer.Compare()
	if err != nil {
		log.Fatal(errors.ErrorStack(err))
	}
}
