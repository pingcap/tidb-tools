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

package diff

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"math/rand"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb-tools/pkg/dbutil"
	"github.com/pingcap/tidb-tools/pkg/utils"
	"github.com/pingcap/tidb/types"
	"go.uber.org/zap"
)

var (
	successState = "success"

	failedState = "failed"

	errorState = "error"

	notCheckedState = "not_checked"

	checkingState = "checking"

	ignoreState = "ignore"
)

func equalStrings(str1, str2 []string) bool {
	if len(str1) != len(str2) {
		return false
	}
	for i := 0; i < len(str1); i++ {
		if str1[i] != str2[i] {
			return false
		}
	}
	return true
}

func removeColumns(tableInfo *model.TableInfo, columns []string) *model.TableInfo {
	if len(columns) == 0 {
		return tableInfo
	}

	removeColMap := utils.SliceToMap(columns)
	for i := 0; i < len(tableInfo.Indices); i++ {
		index := tableInfo.Indices[i]
		for j := 0; j < len(index.Columns); j++ {
			col := index.Columns[j]
			if _, ok := removeColMap[col.Name.O]; ok {
				index.Columns = append(index.Columns[:j], index.Columns[j+1:]...)
				j--
				if len(index.Columns) == 0 {
					tableInfo.Indices = append(tableInfo.Indices[:i], tableInfo.Indices[i+1:]...)
					i--
				}
			}
		}
	}

	for j := 0; j < len(tableInfo.Columns); j++ {
		col := tableInfo.Columns[j]
		if _, ok := removeColMap[col.Name.O]; ok {
			tableInfo.Columns = append(tableInfo.Columns[:j], tableInfo.Columns[j+1:]...)
			j--
		}
	}

	return tableInfo
}

func getColumnsFromIndex(index *model.IndexInfo, tableInfo *model.TableInfo) []*model.ColumnInfo {
	indexColumns := make([]*model.ColumnInfo, 0, len(index.Columns))
	for _, indexColumn := range index.Columns {
		for _, column := range tableInfo.Columns {
			if column.Name.O == indexColumn.Name.O {
				indexColumns = append(indexColumns, column)
			}
		}
	}

	return indexColumns
}

func getRandomN(total, num int) []int {
	if num > total {
		log.Warn("the num is greater than total", zap.Int("num", num), zap.Int("total", total))
		num = total
	}

	totalArray := make([]int, 0, total)
	for i := 0; i < total; i++ {
		totalArray = append(totalArray, i)
	}

	for j := 0; j < num; j++ {
		r := j + rand.Intn(total-j)
		totalArray[j], totalArray[r] = totalArray[r], totalArray[j]
	}

	return totalArray[:num]
}

func needQuotes(ft types.FieldType) bool {
	return !(dbutil.IsNumberType(ft.Tp) || dbutil.IsFloatType(ft.Tp))
}

func saveChunkInfo(ctx context.Context, db *sql.DB, chunkID int, instanceID, schema, table, checksum string, chunk *ChunkRange) error {
	chunkBytes, err := json.Marshal(chunk)
	if err != nil {
		return err
	}

	sql := "REPLACE INTO `sync_diff_inspector`.`chunk` VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?);"
	err = dbutil.ExecSQLWithRetry(ctx, db, sql, chunkID, instanceID, schema, table, chunk.Where, checksum, string(chunkBytes), chunk.State, time.Now())
	if err != nil {
		log.Error("save chunk info failed", zap.Error(err))
		return err
	}
	return nil
}

func updateChunkInfo(ctx context.Context, db *sql.DB, chunkID int, instanceID, schema, table, column string, value string) error {
	sql := fmt.Sprintf("UPDATE `sync_diff_inspector`.`chunk` SET `%s` = ?, `update_time` = ? WHERE `chunk_id` = ? AND `instance_id` = ? AND `schema` = ? AND `table` = ?;", column)
	err := dbutil.ExecSQLWithRetry(ctx, db, sql, value, time.Now(), chunkID, instanceID, schema, table)
	if err != nil {
		log.Error("save chunk info failed", zap.Error(err), zap.String("sql", sql), zap.Int("chunkID", chunkID), zap.String("instanceID", instanceID), zap.String("schema", schema),
			zap.String("table", table), zap.String("value", value))
		return err
	}
	return nil
}

func loadFromCheckPoint(ctx context.Context, db *sql.DB, schema, table string) (bool, error) {
	query := "SELECT `state` FROM `sync_diff_inspector`.`table_summary` WHERE `schema` = ? AND `table` = ? limit 1"
	rows, err := db.QueryContext(ctx, query, schema, table)
	if err != nil {
		return false, errors.Trace(err)
	}
	defer rows.Close()

	var state sql.NullString

	for rows.Next() {
		err1 := rows.Scan(&state)
		if err1 != nil {
			return false, errors.Trace(err1)
		}

		if state.Valid {
			if state.String == successState || state.String == notCheckedState {
				return false, nil
			}
		}

		return true, nil
	}

	return false, nil
}

func initSummaryInfo(ctx context.Context, db *sql.DB, schema, table string, configHash string) error {
	sql := "REPLACE INTO `sync_diff_inspector`.`table_summary`(`schema`, `table`, `state`, `config_hash`, `update_time`) VALUES(?, ?, ?, ?, ?)"
	err := dbutil.ExecSQLWithRetry(ctx, db, sql, schema, table, notCheckedState, configHash, time.Now())
	if err != nil {
		log.Error("save summary info failed", zap.Error(err))
		return err
	}

	return nil
}

func loadChunksInfo(ctx context.Context, db *sql.DB, instanceID, schema, table string) ([]*ChunkRange, error) {
	chunks := make([]*ChunkRange, 0, 100)

	ctx, cancel := context.WithTimeout(context.Background(), dbutil.DefaultTimeout)
	defer cancel()
	sql := "SELECT * FROM `sync_diff_inspector`.`chunk` WHERE `instance_id` = ? AND `schema` = ? AND `table` = ?"
	rows, err := db.QueryContext(ctx, sql, instanceID, schema, table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		fields, err1 := dbutil.ScanRow(rows)
		if err1 != nil {
			return nil, errors.Trace(err1)
		}

		chunkStr := fields["chunk_str"].Data
		chunk := new(ChunkRange)
		err := json.Unmarshal(chunkStr, &chunk)
		if err != nil {
			return nil, err
		}
		chunks = append(chunks, chunk)
	}

	return chunks, nil
}

func updateSummaryInfo(ctx context.Context, db *sql.DB, instanceID, schema, table string) error {
	ctx, cancel := context.WithTimeout(ctx, dbutil.DefaultTimeout)
	defer cancel()

	query := "SELECT `state`, COUNT(*) FROM `sync_diff_inspector`.`chunk` WHERE `instance_id` = ? AND `schema` = ? AND `table` = ? GROUP BY `state` ;"
	rows, err := db.QueryContext(ctx, query, instanceID, schema, table)
	if err != nil {
		return errors.Trace(err)
	}
	defer rows.Close()

	var total, successNum, failedNum, ignoreNum int64
	var chunkState sql.NullString
	var num sql.NullInt64
	for rows.Next() {
		err1 := rows.Scan(&chunkState, &num)
		if err1 != nil {
			return errors.Trace(err1)
		}

		if !chunkState.Valid || !num.Valid {
			continue
		}

		total += num.Int64
		switch chunkState.String {
		case successState:
			successNum = num.Int64
		case failedState, errorState:
			failedNum += num.Int64
		case ignoreState:
			ignoreNum += num.Int64
		case notCheckedState, checkingState:
		}
	}

	log.Info("summary info", zap.String("instance_id", instanceID), zap.String("schema", schema), zap.String("table", table), zap.Int64("chunk num", total), zap.Int64("success num", successNum), zap.Int64("failed num", failedNum), zap.Int64("ignore num", ignoreNum))

	state := checkingState
	if total == successNum+failedNum+ignoreNum {
		if total == successNum + ignoreNum {
			state = successState
		} else {
			state = failedState
		}
	}

	updateSQL := "UPDATE `sync_diff_inspector`.`table_summary` SET `chunk_num` = ?, `check_success_num` = ?, `check_failed_num` = ?, `check_ignore_num` = ?, `state` = ?, `update_time` = ? WHERE `schema` = ? AND `table` = ?"
	err = dbutil.ExecSQLWithRetry(ctx, db, updateSQL, total, successNum, failedNum, ignoreNum, state, time.Now(), schema, table)
	if err != nil {
		return err
	}

	return nil
}

func createCheckpointTable(ctx context.Context, db *sql.DB) error {
	createSchemaSQL := "CREATE DATABASE IF NOT EXISTS `sync_diff_inspector`;"
	_, err := db.ExecContext(ctx, createSchemaSQL)
	if err != nil {
		log.Info("create schema", zap.Error(err))
		return errors.Trace(err)
	}

	createSummaryTableSQL := "CREATE TABLE IF NOT EXISTS `sync_diff_inspector`.`table_summary`(`schema` varchar(30), `table` varchar(30), `chunk_num` int, `check_success_num` int, `check_failed_num` int, `check_ignore_num` int, `state` enum('not_checked', 'checking', 'success', 'failed') DEFAULT 'not_checked', `config_hash` varchar(50), `update_time` datetime, PRIMARY KEY(`schema`, `table`));"
	_, err = db.ExecContext(ctx, createSummaryTableSQL)
	if err != nil {
		log.Info("create chunk table", zap.Error(err))
		return errors.Trace(err)
	}

	createChunkTableSQL := "CREATE TABLE IF NOT EXISTS `sync_diff_inspector`.`chunk`(`chunk_id` int, `instance_id` varchar(30), `schema` varchar(30), `table` varchar(30), `range` varchar(100), `checksum` varchar(20), `chunk_str` text, `state` enum('not_checked','checking','success', 'failed', 'ignore', 'error') DEFAULT 'not_checked', update_time datetime, PRIMARY KEY(`chunk_id`, `instance_id`, `schema`, `table`));"
	_, err = db.ExecContext(ctx, createChunkTableSQL)
	if err != nil {
		log.Info("create chunk table", zap.Error(err))
		return errors.Trace(err)
	}

	return nil
}

func cleanCheckpointInfo(ctx context.Context, db *sql.DB, schema, table string) error {
	deleteTableSummarySQL := "DELETE FROM `sync_diff_inspector`.`table_summary` WHERE `schema` = ? AND `table` = ?;"
	err := dbutil.ExecSQLWithRetry(ctx, db, deleteTableSummarySQL, schema, table)
	if err != nil {
		return errors.Trace(err)
	}

	deleteChunkSQL := "DELETE FROM `sync_diff_inspector`.`chunk` WHERE `schema` = ? AND `table` = ?;"
	err = dbutil.ExecSQLWithRetry(ctx, db, deleteChunkSQL, schema, table)
	if err != nil {
		return errors.Trace(err)
	}

	return nil
}
