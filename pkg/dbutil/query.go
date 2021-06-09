package dbutil

import (
	"database/sql"
	"strings"

	"github.com/pingcap/errors"
)

// ScanRowsToInterfaces scans rows to interface arrary.
func ScanRowsToInterfaces(rows *sql.Rows) ([][]interface{}, error) {
	var rowsData [][]interface{}
	cols, err := rows.Columns()
	if err != nil {
		return nil, errors.Trace(err)
	}

	for rows.Next() {
		colVals := make([]interface{}, len(cols))

		err = rows.Scan(colVals...)
		if err != nil {
			return nil, errors.Trace(err)
		}
		rowsData = append(rowsData, colVals)
	}

	return rowsData, nil
}

// ColumnData saves column's data.
type ColumnData struct {
	Data   []byte
	IsNull bool
}

// ScanRow scans rows into a map.
func ScanRow(rows *sql.Rows) (map[string]*ColumnData, error) {
	cols, err := rows.Columns()
	if err != nil {
		return nil, errors.Trace(err)
	}

	colVals := make([][]byte, len(cols))
	colValsI := make([]interface{}, len(colVals))
	for i := range colValsI {
		colValsI[i] = &colVals[i]
	}

	err = rows.Scan(colValsI...)
	if err != nil {
		return nil, errors.Trace(err)
	}

	result := make(map[string]*ColumnData)
	for i := range colVals {
		data := &ColumnData{
			Data:   colVals[i],
			IsNull: colVals[i] == nil,
		}
		result[cols[i]] = data
	}

	return result, nil
}

// UpperCaseKeyScanRow scan rows into a map, key is upper case.
func UpperCaseKeyScanRow(rows *sql.Rows) (map[string]*ColumnData, error) {
	cols, err := rows.Columns()
	if err != nil {
		return nil, errors.Trace(err)
	}

	colVals := make([][]byte, len(cols))
	colValsI := make([]interface{}, len(colVals))
	for i := range colValsI {
		colValsI[i] = &colVals[i]
	}

	err = rows.Scan(colValsI...)
	if err != nil {
		return nil, errors.Trace(err)
	}

	result := make(map[string]*ColumnData)
	for i := range colVals {
		data := &ColumnData{
			Data:   colVals[i],
			IsNull: colVals[i] == nil,
		}
		//oracle return rows by upper case column names, but tidb/mysql is lower case.
		// so we need unified format
		result[strings.ToUpper(cols[i])] = data
	}

	return result, nil
}
