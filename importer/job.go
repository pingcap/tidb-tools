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
	"database/sql"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/log"
)

var wirter io.Writer

func addJobs(jobCount int, jobChan chan struct{}) {
	for i := 0; i < jobCount; i++ {
		jobChan <- struct{}{}
	}

	close(jobChan)
}

func doInsert(table *table, writer io.Writer, count int) {
	sql, err := genRowDatas(table, count)
	if err != nil {
		log.Fatalf(errors.ErrorStack(err))
	}

	_, err = writer.Write([]byte(sql))
	if err != nil {
		log.Errorf("fail to write %v", err)
	}
}

func doJob(id int, table *table, db *sql.DB, batch int, jobChan chan struct{}, doneChan chan struct{}) {
	writer, err := os.OpenFile(fmt.Sprintf("test.sbtest.%04d", id), os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		log.Fatal(err)
	}

	count := 0
	for range jobChan {
		count++
		if count == batch {
			doInsert(table, writer, count)
			count = 0
		}
	}

	if count > 0 {
		doInsert(table, writer, count)
		count = 0
	}

	doneChan <- struct{}{}
}

func doWait(doneChan chan struct{}, start time.Time, jobCount int, workerCount int) {
	for i := 0; i < workerCount; i++ {
		<-doneChan
	}

	close(doneChan)

	now := time.Now()
	seconds := now.Unix() - start.Unix()

	tps := int64(-1)
	if seconds > 0 {
		tps = int64(jobCount) / seconds
	}

	fmt.Printf("[importer]total %d cases, cost %d seconds, tps %d, start %s, now %s\n", jobCount, seconds, tps, start, now)
}

func doProcess(table *table, dbs []*sql.DB, jobCount int, workerCount int, batch int) {
	jobChan := make(chan struct{}, 16*workerCount)
	doneChan := make(chan struct{}, workerCount)

	start := time.Now()
	go addJobs(jobCount, jobChan)

	for i := 0; i < workerCount; i++ {
		go doJob(i, table, dbs[i], batch, jobChan, doneChan)
	}

	doWait(doneChan, start, jobCount, workerCount)
}
