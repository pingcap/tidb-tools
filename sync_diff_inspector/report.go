// Copyright 2018 PingCAP, Inc.
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
	"fmt"
	"sync"
)

const (
	// Pass means all data and struct of tables are equal
	Pass = "pass"
	// Fail means not all data or struct of tables are equal
	Fail = "fail"
)

// TableResult saves the check result for every table.
type TableResult struct {
	Table       string
	StructEqual bool
	DataEqual   bool
}

// Report saves the check results.
type Report struct {
	sync.RWMutex

	Schema string
	// Result is pass or fail
	Result       string
	PassNum      int32
	FailedNum    int32
	TableResults map[string]*TableResult
}

// NewReport returns a new Report.
func NewReport(schema string) *Report {
	return &Report{
		Schema:       schema,
		TableResults: make(map[string]*TableResult),
		Result:       Pass,
	}
}

// String returns a string of this Report.
func (r *Report) String() (report string) {
	r.RLock()
	defer r.RUnlock()
	/*
		output example:
		check result of schema test: fail!
		1 tables' check passed, 2 tables' check failed.

		table: test1
		table's struct equal
		table's data not equal

		table: test2
		table's struct equal
		table's data not equal

		table: test3
		table's struct equal
		table's data equal
	*/
	report = fmt.Sprintf("\ncheck result of schema %s: %s!\n", r.Schema, r.Result)
	report += fmt.Sprintf("%d tables' check passed, %d tables' check failed.\n", r.PassNum, r.FailedNum)

	var failTableRsult, passTableResult string
	for table, result := range r.TableResults {
		var structResult, dataResult string
		if !result.StructEqual {
			structResult = "table's struct not equal"
		} else {
			structResult = "table's struct equal"
		}

		if !result.DataEqual {
			dataResult = "table's data not equal"
		} else {
			dataResult = "table's data equal"
		}

		if !result.StructEqual || !result.DataEqual {
			failTableRsult = fmt.Sprintf("%stable: %s\n%s\n%s\n\n", failTableRsult, table, structResult, dataResult)
		} else {
			passTableResult = fmt.Sprintf("%stable: %s\n%s\n%s\n\n", passTableResult, table, structResult, dataResult)
		}
	}

	// first print the check failed table's information
	report += fmt.Sprintf("\n%s%s", failTableRsult, passTableResult)

	return
}

// SetTableStructCheckResult sets the struct check result for table.
func (r *Report) SetTableStructCheckResult(table string, equal bool) {
	r.Lock()
	defer r.Unlock()

	if tableResult, ok := r.TableResults[table]; ok {
		tableResult.StructEqual = equal
	} else {
		r.TableResults[table] = &TableResult{
			StructEqual: equal,
		}
	}

	if !equal {
		r.Result = Fail
	}
}

// SetTableDataCheckResult sets the data check result for table.
func (r *Report) SetTableDataCheckResult(table string, equal bool) {
	r.Lock()
	defer r.Unlock()

	if tableResult, ok := r.TableResults[table]; ok {
		tableResult.DataEqual = equal
	} else {
		r.TableResults[table] = &TableResult{
			DataEqual: equal,
		}
	}

	if !equal {
		r.Result = Fail
	}
}
