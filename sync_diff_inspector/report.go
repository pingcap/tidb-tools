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
	"strings"
	"sync"

	"github.com/pingcap/log"
	"github.com/pingcap/tidb-tools/pkg/dbutil"
	"github.com/pingcap/tidb-tools/sync_diff_inspector/utils"
	"go.uber.org/zap"
)

const (
	// Pass means all data and struct of tables are equal
	Pass = "pass"
	// Fail means not all data or struct of tables are equal
	Fail  = "fail"
	Error = "error"
)

// TableResult saves the check result for every table.
type TableResult struct {
	Schema      string `json:"schma"`
	Table       string `json:"table"`
	StructEqual bool   `json:"struct-equal"`
	DataEqual   bool   `json:"data-equal"`
	MeetError   error  `json:"meet-error"`
}

// Report saves the check results.
type Report struct {
	sync.RWMutex

	// Result is pass or fail
	Result       string                             `json:"result"`
	PassNum      int32                              `json:"pass-num"`
	FailedNum    int32                              `json:"failed-num"`
	TableResults map[string]map[string]*TableResult `json:"table-results"`
}

// CommitSummary commit summary info
func (r *Report) CommitSummary(fileName string) error {
	var summary strings.Builder
	if r.Result == Pass {
		summary.WriteString(fmt.Sprintf("A total of %d table have been compared and all are equal.\n", r.FailedNum+r.PassNum))
		summary.WriteString(fmt.Sprintf("You can view the comparision details through './output_dir/%s'\n", fileName))
	} else if r.Result == Fail {
		for schema, tableMap := range r.TableResults {
			for table, result := range tableMap {
				if !result.StructEqual {
					summary.WriteString(fmt.Sprintf("The structure of %s is not equal\n", dbutil.TableName(schema, table)))
				}
				if !result.DataEqual {
					summary.WriteString(fmt.Sprintf("The data of %s is not equal\n", dbutil.TableName(schema, table)))
				}
			}
		}
		summary.WriteString("\n")
		summary.WriteString("The rest of tables are all equal.\n")
		summary.WriteString("The patch file has been generated to './output_dir/patch.sql'\n")
		summary.WriteString(fmt.Sprintf("You can view the comparision details through './output_dir/%s'\n", fileName))
	} else {
		summary.WriteString("Error in comparison process:\n")
		for schema, tableMap := range r.TableResults {
			for table, result := range tableMap {
				summary.WriteString(fmt.Sprintf("%s error occured in %s\n", result.MeetError.Error(), dbutil.TableName(schema, table)))
			}
		}
		summary.WriteString(fmt.Sprintf("You can view the comparision details through './output_dir/%s'\n", fileName))
	}
	summary.WriteString("Press any key to exist.\n")
	log.Info(summary.String())
	utils.GetChar()
	return nil
}

// NewReport returns a new Report.
func NewReport() *Report {
	return &Report{
		TableResults: make(map[string]map[string]*TableResult),
		Result:       Pass,
	}
}

// Print prints the check report.
func (r *Report) Print() {
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

	log.Info("check result summary", zap.Int32("check passed num", r.PassNum), zap.Int32("check failed num", r.FailedNum))

	for schema, tableMap := range r.TableResults {
		for table, result := range tableMap {
			if result.MeetError != nil {
				log.Error("table check result", zap.String("schema", schema), zap.String("table", table), zap.String("meet error", result.MeetError.Error()))
			} else {
				log.Info("table check result", zap.String("schema", schema), zap.String("table", table), zap.Bool("struct equal", result.StructEqual), zap.Bool("data equal", result.DataEqual))
			}
		}
	}

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

	if !equal && r.Result != Error {
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

	if !equal && r.Result != Error {
		r.Result = Fail
	}
}

// SetTableMeetError sets meet error when check the table.
func (r *Report) SetTableMeetError(schema, table string, err error) {
	if _, ok := r.TableResults[schema]; !ok {
		r.TableResults[schema] = make(map[string]*TableResult)
	}

	if tableResult, ok := r.TableResults[schema][table]; ok {
		tableResult.MeetError = err
	} else {
		r.TableResults[schema][table] = &TableResult{
			MeetError: err,
		}
	}

	r.Result = Error
}
