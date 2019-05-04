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

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	. "github.com/pingcap/check"
)

var _ = Suite(&testCheckpointSuite{})

type testCheckpointSuite struct{}

func (s *testCheckpointSuite) TestCheckpoint(c *C) {
	conns, err := createConns()
	c.Assert(err, IsNil)
	defer dropCheckpoint(context.Background(), conns.cpConn)

	s.testInitAndGetSummary(c, conns.cpConn)
	s.testSaveAndLoadChunk(c, conns.cpConn)
	s.testUpdateSummary(c, conns.cpConn)
}

func (s *testCheckpointSuite) testInitAndGetSummary(c *C, conn *sql.Conn) {
	err := createCheckpointTable(context.Background(), conn)
	c.Assert(err, IsNil)

	_, _, _, _, _, err = getTableSummary(context.Background(), conn, "test", "checkpoint")
	c.Log(err)
	c.Assert(err, ErrorMatches, "*not found*")

	err = initTableSummary(context.Background(), conn, "test", "checkpoint", "123")
	c.Assert(err, IsNil)

	total, successNum, failedNum, ignoreNum, state, err := getTableSummary(context.Background(), conn, "test", "checkpoint")
	c.Assert(err, IsNil)
	c.Assert(total, Equals, int64(0))
	c.Assert(successNum, Equals, int64(0))
	c.Assert(failedNum, Equals, int64(0))
	c.Assert(ignoreNum, Equals, int64(0))
	c.Assert(state, Equals, notCheckedState)
}

func (s *testCheckpointSuite) testSaveAndLoadChunk(c *C, conn *sql.Conn) {
	chunk := &ChunkRange{
		ID:     1,
		Bounds: []*Bound{{Column: "a", Lower: "1", LowerSymbol: ">"}},
		Mode:   normalMode,
		State:  successState,
	}

	err := saveChunk(context.Background(), conn, chunk.ID, "target", "test", "checkpoint", "", chunk)
	c.Assert(err, IsNil)

	newChunk, err := getChunk(context.Background(), conn, "target", "test", "checkpoint", chunk.ID)
	c.Assert(err, IsNil)
	c.Assert(newChunk, DeepEquals, chunk)

	chunks, err := loadChunks(context.Background(), conn, "target", "test", "checkpoint")
	c.Assert(err, IsNil)
	c.Assert(chunks, HasLen, 1)
	c.Assert(chunks[0], DeepEquals, chunk)
}

func (s *testCheckpointSuite) testUpdateSummary(c *C, conn *sql.Conn) {
	failedChunk := &ChunkRange{
		ID:    2,
		State: failedState,
	}
	err := saveChunk(context.Background(), conn, failedChunk.ID, "target", "test", "checkpoint", "", failedChunk)
	c.Assert(err, IsNil)

	ignoreChunk := &ChunkRange{
		ID:    3,
		State: ignoreState,
	}
	err = saveChunk(context.Background(), conn, ignoreChunk.ID, "target", "test", "checkpoint", "", ignoreChunk)
	c.Assert(err, IsNil)

	err = updateTableSummary(context.Background(), conn, "target", "test", "checkpoint")
	c.Assert(err, IsNil)

	total, successNum, failedNum, ignoreNum, state, err := getTableSummary(context.Background(), conn, "test", "checkpoint")
	c.Assert(err, IsNil)
	c.Assert(total, Equals, int64(3))
	c.Assert(successNum, Equals, int64(1))
	c.Assert(failedNum, Equals, int64(1))
	c.Assert(ignoreNum, Equals, int64(1))
	c.Assert(state, Equals, failedState)
}

func (s *testUtilSuite) TestloadFromCheckPoint(c *C) {
	db, mock, err := sqlmock.New()
	c.Assert(err, IsNil)

	conn, err := db.Conn(context.Background())
	c.Assert(err, IsNil)

	rows := sqlmock.NewRows([]string{"state", "config_hash"}).AddRow("success", "123")
	mock.ExpectQuery("SELECT").WillReturnRows(rows)
	useCheckpoint, err := loadFromCheckPoint(context.Background(), conn, "test", "test", "123")
	c.Assert(useCheckpoint, Equals, false)

	rows = sqlmock.NewRows([]string{"state", "config_hash"}).AddRow("success", "123")
	mock.ExpectQuery("SELECT").WillReturnRows(rows)
	useCheckpoint, err = loadFromCheckPoint(context.Background(), conn, "test", "test", "456")
	c.Assert(useCheckpoint, Equals, false)

	rows = sqlmock.NewRows([]string{"state", "config_hash"}).AddRow("failed", "123")
	mock.ExpectQuery("SELECT").WillReturnRows(rows)
	useCheckpoint, err = loadFromCheckPoint(context.Background(), conn, "test", "test", "123")
	c.Assert(useCheckpoint, Equals, true)
}
