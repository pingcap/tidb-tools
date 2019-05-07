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
	//conns []*sql.Conn

	// cann't write data when set snapshot, so need create another connection for write checkpoint
	CpDB *sql.DB
	//cpConn *sql.Conn
}

// NewConns returns a new Conns
func NewConns(ctx context.Context, dbConfig dbutil.DBConfig, num int, snapshot string) (conns *Conns, err error) {
	var db *sql.DB
	//conns := make([]*sql.Conn, 0, num)

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

	/*
		for i := 0; i < num; i++ {
			conn, err := db.Conn(ctx)
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
	*/

	cpDB, err := dbutil.OpenDB(dbConfig)
	if err != nil {
		return nil, errors.Errorf("create db connections %+v error %v", dbConfig, err)
	}
	cpDB.SetMaxOpenConns(1)
	cpDB.SetMaxIdleConns(1)
	/*
		cpConn, err := cpDB.Conn(ctx)
		if err != nil {
			return nil, errors.Errorf("create connection %+v error %v", dbConfig, err)
		}
	*/

	return &Conns{
		DB: db,
		//conns:  conns,
		CpDB: cpDB,
		//cpConn: cpConn,
	}, nil
}

/*
// GetConn returns the first connection
func (c *Conns) GetConn() *sql.Conn {
	if c == nil || len(c.conns) == 0 {
		log.Warn("empty conns", zap.Reflect("conns", c))
		return nil
	}

	return c.conns[0]
}
*/

// Close closes all the connections
func (c *Conns) Close() {
	/*
		for _, conn := range c.conns {
			if err := conn.Close(); err != nil {
				log.Warn("close connection failed", zap.Error(err))
			}
		}
	*/

	if err := c.DB.Close(); err != nil {
		log.Warn("close db connection failed", zap.Error(err))
	}

	/*
		if err := c.cpConn.Close(); err != nil {
			log.Warn("close connection failed", zap.Error(err))
		}
	*/

	if err := c.CpDB.Close(); err != nil {
		log.Warn("close db connection failed", zap.Error(err))
	}
}
