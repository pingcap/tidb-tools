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

package common

import (
	"database/sql"
	"regexp"

	"github.com/pingcap/errors"
)

// CreateDB creates sql.DB used for select data
func CreateDB(dsn string, num int) (db *sql.DB, err error) {
	db, err = sql.Open("mysql", dsn)
	if err != nil {
		return nil, errors.Trace(err)
	}

	err = db.Ping()
	if err != nil {
		reg, err := regexp.Compile(":.*@tcp")
		if reg == nil || err != nil {
			return nil, errors.Errorf("create db connections (failed to replace password for dsn) error %v", err)
		}
		return nil, errors.Errorf("create db connections %s error %v", reg.ReplaceAllString(dsn, ":?@tcp"), err)
	}

	// SetMaxOpenConns and SetMaxIdleConns for connection to avoid error like
	// `dial tcp 10.26.2.1:3306: connect: cannot assign requested address`
	db.SetMaxOpenConns(num)
	db.SetMaxIdleConns(num)

	return db, nil
}
