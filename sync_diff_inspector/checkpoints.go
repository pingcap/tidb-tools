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

package main

import (
	"context"
	"encoding/json"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb-tools/sync_diff_inspector/chunk"
)

var (

	// checkpointFile represents the checkpoints' file name which used for save and loads chunks
	checkpointFile = "sync_diff_checkpoints.pb"

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
	// for table: this table is checking but not finished
	notCheckedState = "not_checked"

	// for chunk: means this chunk is checking
	// for table: don't have this state
	checkingState = "checking"

	// for chunk: this chunk is ignored. if sample is not 100%, will ignore some chunk
	// for table: don't have this state
	ignoreState = "ignore"
)

// saveChunk saves the chunk to file.
func saveChunk(ctx context.Context, chunk *chunk.Range) error {
	_, err := json.Marshal(chunk)
	if err != nil {
		return errors.Trace(err)
	}

	// TODO save Chunk to file
	return nil
}

// loadChunks loads chunk info from file `chunk`
func loadChunks(ctx context.Context, instanceID, schema, table string) ([]*chunk.Range, error) {
	chunks := make([]*chunk.Range, 0, 100)
	// TODO load chunks from files
	return chunks, nil
}
