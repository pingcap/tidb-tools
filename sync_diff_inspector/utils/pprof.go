// Copyright 2021 PingCAP, Inc.
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

package utils

import (
	"fmt"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"

	// #nosec
	// register HTTP handler for /debug/pprof
	"net/http"
	_ "net/http/pprof"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

const startPProfSignal = syscall.SIGUSR1

var (
	startedPProf = ""
	mu           sync.Mutex
)

func listen(statusAddr string) (net.Listener, error) {
	mu.Lock()
	defer mu.Unlock()
	if startedPProf != "" {
		log.Warn("Try to start pprof when it has been started, nothing will happen", zap.String("address", startedPProf))
		return nil, errors.Errorf("try to start pprof when it has been started at " + startedPProf)
	}
	listener, err := net.Listen("tcp", statusAddr)
	if err != nil {
		log.Warn("failed to start pprof", zap.String("addr", statusAddr), zap.Error(err))
		return nil, errors.Trace(err)
	}
	startedPProf = listener.Addr().String()
	log.Info("bound pprof to addr", zap.String("addr", startedPProf))
	_, _ = fmt.Fprintf(os.Stderr, "bound pprof to addr %s\n", startedPProf)
	return listener, nil
}

// StartPProfListener forks a new goroutine listening on specified port and provide pprof info.
func StartPProfListener(statusAddr string) error {
	listener, err := listen(statusAddr)
	if err != nil {
		return err
	}

	go func() {
		if e := http.Serve(listener, nil); e != nil {
			log.Warn("failed to serve pprof", zap.String("addr", startedPProf), zap.Error(e))
			mu.Lock()
			startedPProf = ""
			mu.Unlock()
			return
		}
	}()
	return nil
}

// StartDynamicPProfListener starts the listener that will enable pprof when received `startPProfSignal`.
func StartDynamicPProfListener() {
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, startPProfSignal)
	go func() {
		for sig := range signalChan {
			if sig == startPProfSignal {
				log.Info("signal received, starting pprof...", zap.Stringer("signal", sig))
				if err := StartPProfListener("0.0.0.0:0"); err != nil {
					log.Warn("failed to start pprof", zap.Error(err))
					return
				}
			}
		}
	}()
}
