// Copyright 2019 PingCAP, Inc.
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
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb-tools/pkg/dbutil"
	"go.uber.org/zap"
)

var (
	// for chunk: means this chunk's data is equal
	// for table: means this all chunk in this table is equal(except ignore chunk)
	successState = "success"

	// for chunk: means this chunk's data is not equal
	// for table: means some chunks' data is not equal or some chunk check failed in this table
	failedState = "failed"

	// for chunk: means meet error when check, don't know the chunk's data is equal or not equal
	// for table: don't have this state
	errorState = "error"

	// for chunk: means this chunk is not in check
	// for table: all the chunk in this table is not in check
	notCheckedState = "not_checked"

	// for chunk: means this chunk is checking
	// for table: some chunks in this table is checking
	checkingState = "checking"

	// for chunk: this chunk is ignored. if sample is not 100%, will ignore some chunk
	// for table: don't have this state
	ignoreState = "ignore"

	checkpointSchemaName = "sync_diff_inspector"

	summaryTableName = "summary"

	chunkTableName = "chunk"
)

func saveChunk(ctx context.Context, db *sql.DB, chunkID int, instanceID, schema, table, checksum string, chunk *ChunkRange) error {
	chunkBytes, err := json.Marshal(chunk)
	if err != nil {
		return err
	}

	sql := fmt.Sprintf("REPLACE INTO `%s`.`%s` VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?);", checkpointSchemaName, chunkTableName)
	err = dbutil.ExecSQLWithRetry(ctx, db, sql, chunkID, instanceID, schema, table, chunk.Where, checksum, string(chunkBytes), chunk.State, time.Now())
	if err != nil {
		log.Error("save chunk info failed", zap.Error(err))
		return err
	}
	return nil
}

func loadFromCheckPoint(ctx context.Context, db *sql.DB, schema, table, configHash string) (bool, error) {
	query := fmt.Sprintf("SELECT `state`, `config_hash` FROM `%s`.`%s` WHERE `schema` = ? AND `table` = ? limit 1;", checkpointSchemaName, summaryTableName)
	rows, err := db.QueryContext(ctx, query, schema, table)
	if err != nil {
		return false, errors.Trace(err)
	}
	defer rows.Close()

	var state, cfgHash sql.NullString

	for rows.Next() {
		err1 := rows.Scan(&state, &cfgHash)
		if err1 != nil {
			return false, errors.Trace(err1)
		}

		if cfgHash.Valid {
			if configHash != cfgHash.String {
				return false, nil
			}
		}

		if state.Valid {
			// is state is success, will begin a new check for this table
			// if state is not checked, the chunk info maybe not exists, so just return false
			if state.String == successState || state.String == notCheckedState {
				return false, nil
			}
		}

		return true, nil
	}

	return false, errors.Trace(rows.Err())
}

func initTableSummary(ctx context.Context, db *sql.DB, schema, table string, configHash string) error {
	sql := fmt.Sprintf("REPLACE INTO `%s`.`%s`(`schema`, `table`, `state`, `config_hash`) VALUES(?, ?, ?, ?, ?)", checkpointSchemaName, summaryTableName)
	err := dbutil.ExecSQLWithRetry(ctx, db, sql, schema, table, notCheckedState, configHash)
	if err != nil {
		log.Error("save summary info failed", zap.Error(err))
		return err
	}

	return nil
}

func loadChunks(ctx context.Context, db *sql.DB, instanceID, schema, table string) ([]*ChunkRange, error) {
	chunks := make([]*ChunkRange, 0, 100)

	ctx, cancel := context.WithTimeout(context.Background(), dbutil.DefaultTimeout)
	defer cancel()
	query := fmt.Sprintf("SELECT `chunk_str` FROM `%s`.`%s` WHERE `instance_id` = ? AND `schema` = ? AND `table` = ?", checkpointSchemaName, chunkTableName)
	rows, err := db.QueryContext(ctx, query, instanceID, schema, table)
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

	return chunks, errors.Trace(rows.Err())
}

func updateSummaryInfo(ctx context.Context, db *sql.DB, instanceID, schema, table string) error {
	ctx, cancel := context.WithTimeout(ctx, dbutil.DefaultTimeout)
	defer cancel()

	query := fmt.Sprintf("SELECT `state`, COUNT(*) FROM `%s`.`%s` WHERE `instance_id` = ? AND `schema` = ? AND `table` = ? GROUP BY `state` ;", checkpointSchemaName, chunkTableName)
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
	if rows.Err() != nil {
		return errors.Trace(rows.Err())
	}

	log.Info("summary info", zap.String("instance_id", instanceID), zap.String("schema", schema), zap.String("table", table), zap.Int64("chunk num", total), zap.Int64("success num", successNum), zap.Int64("failed num", failedNum), zap.Int64("ignore num", ignoreNum))

	state := checkingState
	if total == successNum+failedNum+ignoreNum {
		if total == successNum+ignoreNum {
			state = successState
		} else {
			state = failedState
		}
	}

	updateSQL := fmt.Sprintf("UPDATE `%s`.`%s` SET `chunk_num` = ?, `check_success_num` = ?, `check_failed_num` = ?, `check_ignore_num` = ?, `state` = ? WHERE `schema` = ? AND `table` = ?", checkpointSchemaName, summaryTableName)
	err = dbutil.ExecSQLWithRetry(ctx, db, updateSQL, total, successNum, failedNum, ignoreNum, state, schema, table)
	if err != nil {
		return err
	}

	return nil
}

func createCheckpointTable(ctx context.Context, db *sql.DB) error {
	createSchemaSQL := fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s`;", checkpointSchemaName)
	_, err := db.ExecContext(ctx, createSchemaSQL)
	if err != nil {
		log.Info("create schema", zap.Error(err))
		return errors.Trace(err)
	}

	/* example
	mysql> select * from sync_diff_inspector.summary;
	+--------+-------+-----------+-------------------+------------------+------------------+---------+----------------------------------+---------------------+
	| schema | table | chunk_num | check_success_num | check_failed_num | check_ignore_num | state   | config_hash                      | update_time         |
	+--------+-------+-----------+-------------------+------------------+------------------+---------+----------------------------------+---------------------+
	| diff   | test  |       112 |               104 |                0 |                8 | success | 91f302052783672b01af3e2b0e7d66ff | 2019-03-26 12:42:11 |
	+--------+-------+-----------+-------------------+------------------+------------------+---------+----------------------------------+---------------------+
	
	note: config_hash is the hash value for the config, if config is changed, will clear the history checkpoint.
	*/
	createSummaryTableSQL :=
		"CREATE TABLE IF NOT EXISTS `sync_diff_inspector`.`summary`(" +
			"`schema` varchar(30), `table` varchar(30)," +
			"`chunk_num` int," +
			"`check_success_num` int," +
			"`check_failed_num` int," +
			"`check_ignore_num` int," +
			"`state` enum('not_checked', 'checking', 'success', 'failed') DEFAULT 'not_checked'," +
			"`config_hash` varchar(50)," +
			"`update_time` datetime ON UPDATE CURRENT_TIMESTAMP," +
			"PRIMARY KEY(`schema`, `table`));"

	_, err = db.ExecContext(ctx, createSummaryTableSQL)
	if err != nil {
		log.Info("create chunk table", zap.Error(err))
		return errors.Trace(err)
	}

	/* example
	mysql> select * from sync_diff_inspector.chunk where chunk_id = 2;;
	+----------+-------------+--------+-------+---------------------------------+-------------+-----------+---------+---------------------+
	| chunk_id | instance_id | schema | table | range                           |  checksum   | chunk_str | state   | update_time         |
	+----------+-------------+--------+-------+---------------------------------+-------------+-----------+---------+---------------------+
	|        2 | target-1    | diff   | test1 | (`a` >= ? AND `a` < ? AND TRUE) |  91f3020527 |  .....    | success | 2019-03-26 12:41:42 |
	+----------+-------------+--------+-------+---------------------------------+-------------+-----------+---------+---------------------+
	*/
	createChunkTableSQL :=
		"CREATE TABLE IF NOT EXISTS `sync_diff_inspector`.`chunk`(" +
			"`chunk_id` int," +
			"`instance_id` varchar(30)," +
			"`schema` varchar(30)," +
			"`table` varchar(30)," +
			"`range` varchar(100)," +
			"`checksum` varchar(20)," +
			"`chunk_str` text," +
			"`state` enum('not_checked', 'checking', 'success', 'failed', 'ignore', 'error') DEFAULT 'not_checked'," +
			"`update_time` datetime ON UPDATE CURRENT_TIMESTAMP," +
			"PRIMARY KEY(`schema`, `table`, `instance_id`, `chunk_id`));"
	_, err = db.ExecContext(ctx, createChunkTableSQL)
	if err != nil {
		log.Info("create chunk table", zap.Error(err))
		return errors.Trace(err)
	}

	return nil
}

func cleanCheckpointInfo(ctx context.Context, db *sql.DB, schema, table string) error {
	deleteTableSummarySQL := fmt.Sprintf("DELETE FROM `%s`.`%s` WHERE `schema` = ? AND `table` = ?;", checkpointSchemaName, summaryTableName)
	err := dbutil.ExecSQLWithRetry(ctx, db, deleteTableSummarySQL, schema, table)
	if err != nil {
		return errors.Trace(err)
	}

	deleteChunkSQL := fmt.Sprintf("DELETE FROM `%s`.`%s` WHERE `schema` = ? AND `table` = ?;", checkpointSchemaName, chunkTableName)
	err = dbutil.ExecSQLWithRetry(ctx, db, deleteChunkSQL, schema, table)
	if err != nil {
		return errors.Trace(err)
	}

	return nil
}
