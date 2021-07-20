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

package main

import (
	"github.com/pingcap/log"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

// WorkerPool contains a pool of workers.
type WorkerPool struct {
	limit   uint
	workers chan *Worker
	name    string
}

// Worker identified by ID.
type Worker struct {
	ID uint64
}

type taskFunc func()
type identifiedTaskFunc func(uint64)

// NewWorkerPool returns a WorkPool.
func NewWorkerPool(limit uint, name string) *WorkerPool {
	workers := make(chan *Worker, limit)
	for i := uint(0); i < limit; i++ {
		workers <- &Worker{ID: uint64(i + 1)}
	}
	return &WorkerPool{
		limit:   limit,
		workers: workers,
		name:    name,
	}
}

// Apply executes a task.
func (pool *WorkerPool) Apply(fn taskFunc) {
	worker := pool.apply()
	go func() {
		defer pool.recycle(worker)
		fn()
	}()
}

// ApplyWithID execute a task and provides it with the worker ID.
func (pool *WorkerPool) ApplyWithID(fn identifiedTaskFunc) {
	worker := pool.apply()
	go func() {
		defer pool.recycle(worker)
		fn(worker.ID)
	}()
}

// ApplyOnErrorGroup executes a task in an errorgroup.
func (pool *WorkerPool) ApplyOnErrorGroup(eg *errgroup.Group, fn func() error) {
	worker := pool.apply()
	eg.Go(func() error {
		defer pool.recycle(worker)
		return fn()
	})
}

// ApplyWithIDInErrorGroup executes a task in an errorgroup and provides it with the worker ID.
func (pool *WorkerPool) ApplyWithIDInErrorGroup(eg *errgroup.Group, fn func(id uint64) error) {
	worker := pool.apply()
	eg.Go(func() error {
		defer pool.recycle(worker)
		return fn(worker.ID)
	})
}

func (pool *WorkerPool) apply() *Worker {
	var worker *Worker
	select {
	case worker = <-pool.workers:
	default:
		log.Debug("wait for workers", zap.String("pool", pool.name))
		worker = <-pool.workers
	}
	return worker
}

func (pool *WorkerPool) recycle(worker *Worker) {
	if worker == nil {
		panic("invalid restore worker")
	}
	pool.workers <- worker
}

// HasWorker checks if the pool has unallocated workers.
func (pool *WorkerPool) HasWorker() bool {
	return len(pool.workers) > 0
}
