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

// TableResult saves the check result for every table.
type TableResult struct {
	Table       string
	StructEqual bool
	DataEqual   bool
}

// Report saves the check results.
type Report struct {
	sync.RWMutex

	Schema         string
	TableNameEqual bool
	SourceTables   []string
	TargetTables   []string
	PassNum        int32
	FailedNum      int32
	TableResults   map[string]*TableResult
	Pass           bool
}

// NewReport returns a new Report.
func NewReport(schema string) *Report {
	return &Report{
		Schema:         schema,
		TableResults:   make(map[string]*TableResult),
		TableNameEqual: true,
		Pass:           true,
	}
}

// String returns a string of this Report.
func (r *Report) String() (report string) {
	if r.Pass {
		report = fmt.Sprintf("check result of schema %s: Success!\n", r.Schema)
	} else {
		report = fmt.Sprintf("check result of schema %s: Failed!\n", r.Schema)
	}

	if !r.TableNameEqual {
		report = fmt.Sprintf("%sget different table! source tables: %v, target tables: %v\n", report, r.SourceTables, r.TargetTables)
	}
	report = fmt.Sprintf("%s%d tables' check passed, %d table's check failed.\n\n", report, r.PassNum, r.FailedNum)

	for table, result := range r.TableResults {
		report = fmt.Sprintf("%stable: %s", report, table)
		if !result.StructEqual {
			report = fmt.Sprintf("%stable's struct not equal\n", report)
		} else {
			report = fmt.Sprintf("%stable's struct equal\n", report)
		}

		if !result.DataEqual {
			report = fmt.Sprintf("%stable's data not equal\n\n", report)
		} else {
			report = fmt.Sprintf("%stable's data equal\n\n", report)
		}
	}

	return
}

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
}

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
}
