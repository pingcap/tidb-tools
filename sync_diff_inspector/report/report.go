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

package report

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/olekukonko/tablewriter"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb-tools/pkg/dbutil"
	"github.com/pingcap/tidb-tools/sync_diff_inspector/config"
	"github.com/pingcap/tidb-tools/sync_diff_inspector/source/common"
	"github.com/pingcap/tidb-tools/sync_diff_inspector/utils"
)

const (
	// Pass means all data and struct of tables are equal
	Pass = "pass"
	// Fail means not all data or struct of tables are equal
	Fail  = "fail"
	Error = "error"
)

type ReportConfig struct {
	Host     string `toml:"host"`
	Port     int    `toml:"port"`
	User     string `toml:"user"`
	Snapshot string `toml:"snapshot,omitempty"`
	SqlMode  string `toml:"sql-mode,omitempty"`
}

// TableResult saves the check result for every table.
type TableResult struct {
	Schema      string               `json:"schma"`
	Table       string               `json:"table"`
	StructEqual bool                 `json:"struct-equal"`
	DataEqual   bool                 `json:"data-equal"`
	MeetError   error                `json:"meet-error"`
	ChunkMap    map[int]*ChunkResult `json:"chunk-result"`
}

type ChunkResult struct {
	RowsAdd    int   `json:"rows-add"`
	RowsDelete int   `json:"rows-delete"`
	RowsCnt    int64 `json:"rows-count"`
}

// Report saves the check results.
type Report struct {
	sync.RWMutex
	// Result is pass or fail
	Result       string
	PassNum      int32
	FailedNum    int32
	TableResults map[string]map[string]*TableResult `json:"table-results"`
	StartTime    time.Time                          `json:"start-time"`
	Duration     time.Duration                      `json:"time-duration"`
	TotalSize    int64
	SourceConfig [][]byte
	TargetConfig []byte
}

func (r *Report) LoadReport(reportInfo *Report) {
	r.StartTime = time.Now()
	r.Duration = reportInfo.Duration
	r.TotalSize = reportInfo.TotalSize
	for schema, tableMap := range reportInfo.TableResults {
		if _, ok := r.TableResults[schema]; !ok {
			r.TableResults[schema] = make(map[string]*TableResult)
		}
		for table, result := range tableMap {
			r.TableResults[schema][table] = result
		}
	}
}

func (r *Report) getSortedTables() []string {
	equalTables := make([]string, 0)
	for schema, tableMap := range r.TableResults {
		for table, result := range tableMap {
			if result.StructEqual && result.DataEqual {
				equalTables = append(equalTables, dbutil.TableName(schema, table))
			}
		}
	}
	sort.Slice(equalTables, func(i, j int) bool { return equalTables[i] < equalTables[j] })
	return equalTables
}

func (r *Report) getDiffRows() [][]string {
	diffRows := make([][]string, 0)
	for schema, tableMap := range r.TableResults {
		for table, result := range tableMap {
			if result.StructEqual && result.DataEqual {
				continue
			}
			diffRow := make([]string, 0)
			diffRow = append(diffRow, dbutil.TableName(schema, table))
			if !result.StructEqual {
				diffRow = append(diffRow, "false")
			} else {
				diffRow = append(diffRow, "true")
			}
			rowAdd, rowDelete := 0, 0
			for _, chunkResult := range result.ChunkMap {
				rowAdd += chunkResult.RowsAdd
				rowDelete += chunkResult.RowsDelete
			}
			diffRow = append(diffRow, fmt.Sprintf("+%d/-%d", rowAdd, rowDelete))
			diffRows = append(diffRows, diffRow)
		}
	}
	return diffRows
}

func (r *Report) CalculateTotalSize(ctx context.Context, db *sql.DB) error {
	for schema, tableMap := range r.TableResults {
		for table := range tableMap {
			size, err := utils.GetTableSize(ctx, db, schema, table)
			if err != nil {
				return errors.Trace(err)
			}
			r.TotalSize += size
		}
	}
	return nil
}

// CommitSummary commit summary info
func (r *Report) CommitSummary(taskConfig *config.TaskConfig) error {
	passNum, failedNum := int32(0), int32(0)
	for _, tableMap := range r.TableResults {
		for _, result := range tableMap {
			if result.StructEqual && result.DataEqual {
				passNum++
			} else {
				failedNum++
			}
		}
	}
	r.PassNum = passNum
	r.FailedNum = failedNum
	summaryPath := filepath.Join(taskConfig.OutputDir, "summary.txt")
	summaryFile, err := os.Create(summaryPath)
	if err != nil {
		return errors.Trace(err)
	}
	summaryFile.WriteString("Summary\n\n\n\n")
	summaryFile.WriteString("Source Database\n\n\n\n")
	for i := 0; i < len(r.SourceConfig); i++ {
		summaryFile.Write(r.SourceConfig[i])
		summaryFile.WriteString("\n")
	}
	summaryFile.WriteString("Target Databases\n\n\n\n")
	summaryFile.Write(r.TargetConfig)
	summaryFile.WriteString("\n")

	summaryFile.WriteString("Comparison Result\n\n\n\n")
	summaryFile.WriteString("The table structure and data in following tables are equivalent\n\n")
	equalTables := r.getSortedTables()
	for _, table := range equalTables {
		summaryFile.WriteString(table + "\n")
	}
	if r.Result == Fail {
		summaryFile.WriteString("\nThe following tables contains inconsistent data\n\n")
		tableString := &strings.Builder{}
		table := tablewriter.NewWriter(tableString)
		table.SetHeader([]string{"Table", "Structure equality", "Data diff rows"})
		diffRows := r.getDiffRows()
		for _, v := range diffRows {
			table.Append(v)
		}
		table.Render()
		summaryFile.WriteString(tableString.String())
	}
	duration := r.Duration + time.Since(r.StartTime)
	summaryFile.WriteString(fmt.Sprintf("Time Cost: %s\n", duration))
	summaryFile.WriteString(fmt.Sprintf("Average Speed: %fMB/s\n", float64(r.TotalSize)/(1024.0*1024.0*duration.Seconds())))
	return nil
}

func (r *Report) Print(fileName string, w io.Writer) error {
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
	fmt.Fprint(w, summary.String())
	return nil
}

// NewReport returns a new Report.
func NewReport() *Report {
	return &Report{
		TableResults: make(map[string]map[string]*TableResult),
		Result:       Pass,
	}
}

func (r *Report) Init(tableDiffs []*common.TableDiff, sourceConfig [][]byte, targetConfig []byte) {
	r.StartTime = time.Now()
	r.SourceConfig = sourceConfig
	r.TargetConfig = targetConfig
	for _, tableDiff := range tableDiffs {
		schema, table := tableDiff.Schema, tableDiff.Table
		if _, ok := r.TableResults[schema]; !ok {
			r.TableResults[schema] = make(map[string]*TableResult)
		}
		r.TableResults[schema][table] = &TableResult{
			Schema:      schema,
			Table:       table,
			StructEqual: true,
			DataEqual:   true,
			MeetError:   nil,
			ChunkMap:    make(map[int]*ChunkResult),
		}
	}
}

// SetTableStructCheckResult sets the struct check result for table.
func (r *Report) SetTableStructCheckResult(schema, table string, equal bool) {
	r.Lock()
	defer r.Unlock()
	r.TableResults[schema][table].StructEqual = equal
	if !equal && r.Result != Error {
		r.Result = Fail
	}
}

// SetTableDataCheckResult sets the data check result for table.
func (r *Report) SetTableDataCheckResult(schema, table string, equal bool, rowsAdd, rowsDelete int, id int) {
	r.Lock()
	defer r.Unlock()
	if !equal {
		result := r.TableResults[schema][table]
		result.DataEqual = equal
		if _, ok := result.ChunkMap[id]; !ok {
			result.ChunkMap[id] = &ChunkResult{
				RowsAdd:    0,
				RowsDelete: 0,
				RowsCnt:    0,
			}
		}
		result.ChunkMap[id].RowsAdd += rowsAdd
		result.ChunkMap[id].RowsDelete += rowsDelete
		if r.Result != Error {
			r.Result = Fail
		}
	}
	if !equal && r.Result != Error {
		r.Result = Fail
	}
}

// SetTableMeetError sets meet error when check the table.
func (r *Report) SetTableMeetError(schema, table string, err error) {
	r.Lock()
	defer r.Unlock()
	if _, ok := r.TableResults[schema]; !ok {
		r.TableResults[schema] = make(map[string]*TableResult)
	}

	r.TableResults[schema][table].MeetError = err
	r.Result = Error
}

func (r *Report) SetRowsCnt(schema, table string, cnt int64, id int) {
	r.Lock()
	defer r.Unlock()
	result := r.TableResults[schema][table]
	if _, ok := result.ChunkMap[id]; !ok {
		result.ChunkMap[id] = &ChunkResult{
			RowsAdd:    0,
			RowsDelete: 0,
			RowsCnt:    0,
		}
	}
	result.ChunkMap[id].RowsCnt += cnt
}
