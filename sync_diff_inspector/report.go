/*************************************************************************
 *
 * PingCAP CONFIDENTIAL
 * __________________
 *
 *  [2015] - [2018] PingCAP Incorporated
 *  All Rights Reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of PingCAP Incorporated and its suppliers,
 * if any.  The intellectual and technical concepts contained
 * herein are proprietary to PingCAP Incorporated
 * and its suppliers and may be covered by P.R.China and Foreign Patents,
 * patents in process, and are protected by trade secret or copyright law.
 * Dissemination of this information or reproduction of this material
 * is strictly forbidden unless prior written permission is obtained
 * from PingCAP Incorporated.
 */

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
	Schema      string
	Table       string
	StructEqual bool
	DataEqual   bool
}

// Report saves the check results.
type Report struct {
	sync.RWMutex

	// Result is pass or fail
	Result       string
	PassNum      int32
	FailedNum    int32
	TableResults map[string]map[string]*TableResult
}

// NewReport returns a new Report.
func NewReport() *Report {
	return &Report{
		TableResults: make(map[string]map[string]*TableResult),
		Result:       Pass,
	}
}

// String returns a string of this Report.
func (r *Report) String() (report string) {
	r.RLock()
	defer r.RUnlock()
	/*
		output example:
		check result: fail!
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
	report = fmt.Sprintf("\ncheck result: %s!\n", r.Result)
	report += fmt.Sprintf("%d tables' check passed, %d tables' check failed.\n", r.PassNum, r.FailedNum)

	var failTableRsult, passTableResult string
	for schema, tableMap := range r.TableResults {
		for table, result := range tableMap {
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
				failTableRsult = fmt.Sprintf("%stable: %s.%s\n%s\n%s\n\n", failTableRsult, schema, table, structResult, dataResult)
			} else {
				passTableResult = fmt.Sprintf("%stable: %s.%s\n%s\n%s\n\n", passTableResult, schema, table, structResult, dataResult)
			}
		}
	}

	// first print the check failed table's information
	report += fmt.Sprintf("\n%s%s", failTableRsult, passTableResult)

	return
}

// SetTableStructCheckResult sets the struct check result for table.
func (r *Report) SetTableStructCheckResult(schema, table string, equal bool) {
	r.Lock()
	defer r.Unlock()

	if _, ok := r.TableResults[schema]; !ok {
		r.TableResults[schema] = make(map[string]*TableResult)
	}

	if tableResult, ok := r.TableResults[schema][table]; ok {
		tableResult.StructEqual = equal
	} else {
		r.TableResults[schema][table] = &TableResult{
			StructEqual: equal,
		}
	}

	if !equal {
		r.Result = Fail
	}
}

// SetTableDataCheckResult sets the data check result for table.
func (r *Report) SetTableDataCheckResult(schema, table string, equal bool) {
	r.Lock()
	defer r.Unlock()

	if _, ok := r.TableResults[schema]; !ok {
		r.TableResults[schema] = make(map[string]*TableResult)
	}

	if tableResult, ok := r.TableResults[schema][table]; ok {
		tableResult.DataEqual = equal
	} else {
		r.TableResults[schema][table] = &TableResult{
			DataEqual: equal,
		}
	}

	if !equal {
		r.Result = Fail
	}
}
