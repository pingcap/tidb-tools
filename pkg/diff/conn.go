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

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb-tools/pkg/dbutil"
	"go.uber.org/zap"
)

// Conns keeps some connections
type Conns struct {
	DB *sql.DB

	// cann't write data when set snapshot, so need create another connection for write checkpoint
	CpDB *sql.DB
}

// NewConns returns a new Conns
func NewConns(ctx context.Context, dbConfig dbutil.DBConfig, num int, snapshot string) (conns *Conns, err error) {
	var db, cpDB *sql.DB
	defer func() {
		if err == nil {
			return
		}

		if db != nil {
			if err1 := db.Close(); err != nil {
				log.Warn("close db connection failed", zap.Error(err1))
			}
		}

		if cpDB != nil {
			if err1 := cpDB.Close(); err != nil {
				log.Warn("close db connection failed", zap.Error(err1))
			}
		}
	}()

	if snapshot != "" {
		db, err = dbutil.OpenDBWithSnapshot(dbConfig, snapshot)
	} else {
		db, err = dbutil.OpenDB(dbConfig)
	}
	if err != nil {
		return nil, errors.Errorf("create db connections %+v error %v", dbConfig, err)
	}

	// SetMaxOpenConns and SetMaxIdleConns for connection to avoid error like
	// `dial tcp 10.26.2.1:3306: connect: cannot assign requested address`
	db.SetMaxOpenConns(num)
	db.SetMaxIdleConns(num)

	cpDB, err = dbutil.OpenDB(dbConfig)
	if err != nil {
		return nil, errors.Errorf("create db connections %+v error %v", dbConfig, err)
	}
	cpDB.SetMaxOpenConns(1)
	cpDB.SetMaxIdleConns(1)

	return &Conns{
		DB:   db,
		CpDB: cpDB,
	}, nil
}

// Close closes all the connections
func (c *Conns) Close() {
	if c.DB != nil {
		if err := c.DB.Close(); err != nil {
			log.Warn("close db connection failed", zap.Error(err))
		}
	}

	if c.CpDB != nil {
		if err := c.CpDB.Close(); err != nil {
			log.Warn("close db connection failed", zap.Error(err))
		}
	}
}
