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
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb-tools/pkg/dbutil"
	"go.uber.org/zap"
)

// Conns keeps some connections
type Conns struct {
	db    *sql.DB
	conns []*sql.Conn

	// cann't write data when set snapshot, so need create another connection for write checkpoint
	cpDB   *sql.DB
	cpConn *sql.Conn
}

// NewConns returns a new Conns
func NewConns(dbConfig dbutil.DBConfig, num int, snapshot string) (*Conns, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(num)*time.Second)
	defer cancel()

	conns := make([]*sql.Conn, 0, num)

	db, err := dbutil.OpenDB(dbConfig)
	if err != nil {
		return nil, errors.Errorf("create db connections %+v error %v", dbConfig, err)
	}
	db.SetMaxOpenConns(num)
	db.SetMaxIdleConns(num)

	for i := 0; i < num; i++ {
		conn, err := db.Conn(context.Background())
		if err != nil {
			return nil, errors.Errorf("create connection %+v error %v", dbConfig, err)
		}
		conns = append(conns, conn)

		if snapshot == "" {
			continue
		}
		err = dbutil.SetSnapshot(ctx, conn, snapshot)
		if err != nil {
			return nil, errors.Errorf("set history snapshot %s for source db %+v error %v", snapshot, dbConfig, err)
		}
	}

	cpDB, err := dbutil.OpenDB(dbConfig)
	if err != nil {
		return nil, errors.Errorf("create db connections %+v error %v", dbConfig, err)
	}
	cpDB.SetMaxOpenConns(1)
	cpDB.SetMaxIdleConns(1)
	cpConn, err := cpDB.Conn(context.Background())
	if err != nil {
		return nil, errors.Errorf("create connection %+v error %v", dbConfig, err)
	}

	return &Conns{
		db:     db,
		conns:  conns,
		cpDB:   cpDB,
		cpConn: cpConn,
	}, nil
}

// GetConn returns the first connection
func (c *Conns) GetConn() *sql.Conn {
	if c == nil || len(c.conns) == 0 {
		log.Warn("empty conns", zap.Reflect("conns", c))
		return nil
	}

	return c.conns[0]
}

// Close closes all the connections
func (c *Conns) Close() {
	for _, conn := range c.conns {
		if err := conn.Close(); err != nil {
			log.Warn("close connection failed", zap.Error(err))
		}
	}

	if err := c.db.Close(); err != nil {
		log.Warn("close db connection failed", zap.Error(err))
	}

	if err := c.cpConn.Close(); err != nil {
		log.Warn("close connection failed", zap.Error(err))
	}

	if err := c.cpDB.Close(); err != nil {
		log.Warn("close db connection failed", zap.Error(err))
	}
}
