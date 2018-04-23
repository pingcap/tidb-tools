package util

import (
	"bytes"
	"fmt"
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb-binlog/pkg/dml"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/codec"
)

func GenInsertSQLs(schema string, table *model.TableInfo, rows [][]byte) ([]string, [][]string, error) {
	columns := table.Columns
	sqls := make([]string, 0, len(rows))
	values := make([][]string, 0, len(rows))

	colsTypeMap := toColumnTypeMap(columns)
	columnList := genColumnList(columns)
	columnPlaceholders := dml.GenColumnPlaceholders((len(columns)))
	sql := fmt.Sprintf("replace into `%s`.`%s` (%s) values (%s);", schema, table.Name, columnList, columnPlaceholders)

	for _, row := range rows {
		//decode the pk value
		remain, pk, err := codec.DecodeOne(row)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}

		columnValues, err := tablecodec.DecodeRow(remain, colsTypeMap, time.Local)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}

		if columnValues == nil {
			columnValues = make(map[int64]types.Datum)
		}

		var vals []string
		for _, col := range columns {
			if isPKHandleColumn(table, col) {
				columnValues[col.ID] = pk
				vals = append(vals, fmt.Sprintf("%s", pk.GetValue()))
				continue
			}

			val, ok := columnValues[col.ID]
			if !ok {
				vals = append(vals, fmt.Sprintf("%s", col.DefaultValue))
			} else {
				value, err := formatData(val, col.FieldType)
				if err != nil {
					return nil, nil, errors.Trace(err)
				}

				vals = append(vals, value)
			}
		}

		if columnValues == nil {
			log.Warn("columnValues is nil")
			continue
		}

		sqls = append(sqls, sql)
		values = append(values, vals)
	}

	return sqls, values, nil
}

func GenUpdateSQLs(schema string, table *model.TableInfo, rows [][]byte) ([]string, [][]string, error) {
	columns := table.Columns
	sqls := make([]string, 0, len(rows))
	values := make([][]string, 0, len(rows))
	colsTypeMap := toColumnTypeMap(columns)

	for _, row := range rows {
		var updateColumns []*model.ColumnInfo
		var oldValues []string
		var newValues []string

		oldColumnValues, newColumnValues, err := decodeOldAndNewRow(row, colsTypeMap, time.Local)
		if err != nil {
			return nil, nil, errors.Annotatef(err, "table `%s`.`%s`", schema, table.Name)
		}

		if len(newColumnValues) == 0 {
			continue
		}

		updateColumns, oldValues, err = generateColumnAndValue(columns, oldColumnValues)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		whereColumns := updateColumns

		updateColumns, newValues, err = generateColumnAndValue(columns, newColumnValues)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}

		var value []string
		kvs := genKVs(updateColumns)
		value = append(value, newValues...)

		var where string
		where, oldValues, err = genWhere(table, whereColumns, oldValues)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		value = append(value, oldValues...)
		sql := fmt.Sprintf("update `%s`.`%s` set %s where %s limit 1;", schema, table.Name, kvs, where)
		sqls = append(sqls, sql)
		values = append(values, value)
	}

	return sqls, values, nil
}

func GenDeleteSQLs(schema string, table *model.TableInfo, rows [][]byte) ([]string, [][]string, error) {
	columns := table.Columns
	sqls := make([]string, 0, len(rows))
	values := make([][]string, 0, len(rows))
	colsTypeMap := toColumnTypeMap(columns)

	for _, row := range rows {
		columnValues, err := tablecodec.DecodeRow(row, colsTypeMap, time.Local)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		if columnValues == nil {
			continue
		}

		sql, value, err := genDeleteSQL(schema, table, columnValues)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		values = append(values, value)
		sqls = append(sqls, sql)
	}

	return sqls, values, nil
}

func genDeleteSQL(schema string, table *model.TableInfo, columnValues map[int64]types.Datum) (string, []string, error) {
	columns := table.Columns

	whereColumns, value, err := generateColumnAndValue(columns, columnValues)
	if err != nil {
		return "", nil, errors.Trace(err)
	}

	where, value, err := genWhere(table, whereColumns, value)
	if err != nil {
		return "", nil, errors.Trace(err)
	}

	sql := fmt.Sprintf("delete from `%s`.`%s` where %s limit 1;", schema, table.Name, where)

	return sql, value, nil
}

func GenDDLSQL(sql string, schema string) (string, error) {
	stmts, err := parser.New().Parse(sql, "", "")
	if err != nil {
		return "", errors.Trace(err)
	}

	stmt := stmts[0]
	_, isCreateDatabase := stmt.(*ast.CreateDatabaseStmt)
	if isCreateDatabase {
		return fmt.Sprintf("%s;", sql), nil
	}

	return fmt.Sprintf("use `%s`; %s;", schema, sql), nil
}

func genWhere(table *model.TableInfo, columns []*model.ColumnInfo, data []string) (string, []string, error) {
	var kvs bytes.Buffer
	// if has primary key, use it to construct where condition
	pcs, err := pkIndexColumns(table)
	if err != nil {
		return "", nil, errors.Trace(err)
	}

	hasPK := (len(pcs) != 0)
	pcsMap := make(map[int64]*model.ColumnInfo)
	for _, col := range pcs {
		pcsMap[col.ID] = col
	}

	var conditionValues []string
	first := true
	for i, col := range columns {
		_, ok := pcsMap[col.ID]
		if !ok && hasPK {
			// if table has primary key, just ignore the non primary key column
			continue
		}

		valueClause := "= ?"
		if data[i] == "" {
			valueClause = "is NULL"
		} else {
			conditionValues = append(conditionValues, fmt.Sprintf("%s", data[i]))
		}

		if first {
			first = false
			fmt.Fprintf(&kvs, "`%s` %s", columns[i].Name, valueClause)
		} else {
			fmt.Fprintf(&kvs, " and `%s` %s", columns[i].Name, valueClause)
		}
	}

	return kvs.String(), conditionValues, nil
}

func genColumnList(columns []*model.ColumnInfo) string {
	var columnList []byte
	for i, column := range columns {
		name := fmt.Sprintf("`%s`", column.Name)
		columnList = append(columnList, []byte(name)...)

		if i != len(columns)-1 {
			columnList = append(columnList, ',')
		}
	}

	return string(columnList)
}

func genKVs(columns []*model.ColumnInfo) string {
	var kvs bytes.Buffer
	for i := range columns {
		if i == len(columns)-1 {
			fmt.Fprintf(&kvs, "`%s` = ?", columns[i].Name)
		} else {
			fmt.Fprintf(&kvs, "`%s` = ?, ", columns[i].Name)
		}
	}

	return kvs.String()
}

func pkHandleColumn(table *model.TableInfo) *model.ColumnInfo {
	for _, col := range table.Columns {
		if isPKHandleColumn(table, col) {
			return col
		}
	}

	return nil
}

func pkIndexColumns(table *model.TableInfo) ([]*model.ColumnInfo, error) {
	col := pkHandleColumn(table)
	if col != nil {
		return []*model.ColumnInfo{col}, nil
	}

	var cols []*model.ColumnInfo
	for _, idx := range table.Indices {
		if idx.Primary {
			columns := make(map[string]*model.ColumnInfo)

			for _, col := range table.Columns {
				columns[col.Name.O] = col
			}

			for _, col := range idx.Columns {
				if column, ok := columns[col.Name.O]; ok {
					cols = append(cols, column)
				}
			}

			if len(cols) == 0 {
				return nil, errors.New("primay index is empty, but should not be empty")
			}

			return cols, nil
		}
	}

	return cols, nil
}

func isPKHandleColumn(table *model.TableInfo, column *model.ColumnInfo) bool {
	return (mysql.HasPriKeyFlag(column.Flag) && table.PKIsHandle) || column.ID == implicitColID
}

func generateColumnAndValue(columns []*model.ColumnInfo, columnValues map[int64]types.Datum) ([]*model.ColumnInfo, []string, error) {
	var newColumn []*model.ColumnInfo
	var newColumnsValues []string

	for _, col := range columns {
		val, ok := columnValues[col.ID]
		if ok {
			newColumn = append(newColumn, col)
			value, err := formatData(val, col.FieldType)
			if err != nil {
				return nil, nil, errors.Trace(err)
			}

			newColumnsValues = append(newColumnsValues, value)
		}
	}

	return newColumn, newColumnsValues, nil
}

func formatData(data types.Datum, ft types.FieldType) (string, error) {
	if data.GetValue() == nil {
		return "", nil
	}

	switch ft.Tp {
	case mysql.TypeDate, mysql.TypeDatetime, mysql.TypeNewDate, mysql.TypeTimestamp, mysql.TypeDuration, mysql.TypeDecimal, mysql.TypeNewDecimal, mysql.TypeJSON:
		data = types.NewDatum(fmt.Sprintf("%v", data.GetValue()))
	case mysql.TypeEnum:
		data = types.NewDatum(data.GetMysqlEnum().Value)
	case mysql.TypeSet:
		data = types.NewDatum(data.GetMysqlSet().Value)
	case mysql.TypeBit:
		data = types.NewDatum(data.GetMysqlBit())
	}

	return fmt.Sprintf("%s", data.GetValue()), nil
}

func toColumnTypeMap(columns []*model.ColumnInfo) map[int64]*types.FieldType {
	colTypeMap := make(map[int64]*types.FieldType)
	for _, col := range columns {
		colTypeMap[col.ID] = &col.FieldType
	}

	return colTypeMap
}

// DecodeRowWithMap decodes a byte slice into datums with a existing row map.
// Row layout: colID1, value1, colID2, value2, .....
func decodeOldAndNewRow(b []byte, cols map[int64]*types.FieldType, loc *time.Location) (map[int64]types.Datum, map[int64]types.Datum, error) {
	if b == nil {
		return nil, nil, nil
	}
	if b[0] == codec.NilFlag {
		return nil, nil, nil
	}

	cnt := 0
	var (
		data   []byte
		err    error
		oldRow = make(map[int64]types.Datum, len(cols))
		newRow = make(map[int64]types.Datum, len(cols))
	)
	for len(b) > 0 {
		// Get col id.
		data, b, err = codec.CutOne(b)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		_, cid, err := codec.DecodeOne(data)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		// Get col value.
		data, b, err = codec.CutOne(b)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		id := cid.GetInt64()
		ft, ok := cols[id]
		if ok {
			v, err := tablecodec.DecodeColumnValue(data, ft, loc)
			if err != nil {
				return nil, nil, errors.Trace(err)
			}

			if _, ok := oldRow[id]; ok {
				newRow[id] = v
			} else {
				oldRow[id] = v
			}

			cnt++
			if cnt == len(cols)*2 {
				// Get enough data.
				break
			}
		}
	}

	if cnt != len(cols)*2 || len(newRow) != len(oldRow) {
		return nil, nil, errors.Errorf(" row data is corruption %v", b)
	}

	return oldRow, newRow, nil
}
