// Copyright 2016 PingCAP, Inc.
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

package main

import (
	"bytes"
	"os"
	"sync"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/siddontang/go/ioutil2"
)

var (
	maxSaveTime = 30 * time.Second
)

// Meta is savepoint meta interface
type Meta interface {
	// Load loads meta information.
	Load() error

	// Save saves meta information.
	Save(int64) error

	// Check checks whether we should save meta.
	Check() bool

	// Pos gets position information.
	Pos() int64
}

// LocalMeta is local meta struct.
type localMeta struct {
	sync.RWMutex

	name     string
	saveTime time.Time

	CommitTS int64 `toml:"commitTS" json:"commitTS"`
	// drainer only stores the binlog file suffix
	//Suffixs map[string]uint64 `toml:"suffixs" json:"suffixs"`
}

// NewLocalMeta creates a new LocalMeta.
func NewLocalMeta(name string) Meta {
	return &localMeta{name: name}
}

// Load implements Meta.Load interface.
func (lm *localMeta) Load() error {
	file, err := os.Open(lm.name)
	if err != nil && !os.IsNotExist(errors.Cause(err)) {
		return errors.Trace(err)
	}
	if os.IsNotExist(errors.Cause(err)) {
		return nil
	}
	defer file.Close()

	_, err = toml.DecodeReader(file, lm)
	return errors.Trace(err)
}

// Save implements Meta.Save interface.
func (lm *localMeta) Save(ts int64) error {
	log.Infof("local meta save")
	lm.Lock()
	defer lm.Unlock()

	lm.CommitTS = ts

	var buf bytes.Buffer
	e := toml.NewEncoder(&buf)
	err := e.Encode(lm)
	if err != nil {
		log.Errorf("syncer save meta info to file %s err %v", lm.name, errors.ErrorStack(err))
		return errors.Trace(err)
	}

	err = ioutil2.WriteFileAtomic(lm.name, buf.Bytes(), 0644)
	if err != nil {
		log.Errorf("syncer save meta info to file %s err %v", lm.name, errors.ErrorStack(err))
		return errors.Trace(err)
	}

	lm.saveTime = time.Now()
	log.Infof("ts: %d", ts)
	return nil
}

// Check implements Meta.Check interface.
func (lm *localMeta) Check() bool {
	lm.RLock()
	defer lm.RUnlock()

	if time.Since(lm.saveTime) >= maxSaveTime {
		return true
	}

	return false
}

// Pos implements Meta.Pos interface.
func (lm *localMeta) Pos() int64 {
	lm.RLock()
	defer lm.RUnlock()

	return lm.CommitTS
}
