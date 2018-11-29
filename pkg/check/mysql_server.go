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

package check

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/pingcap/tidb-tools/pkg/dbutil"
	"github.com/pingcap/tidb-tools/pkg/utils"
)

// MySQLVersionChecker checks mysql/mariadb/rds,... version.
type MySQLVersionChecker struct {
	db     *sql.DB
	dbinfo *dbutil.DBConfig
}

// NewMySQLVersionChecker returns a Checker
func NewMySQLVersionChecker(db *sql.DB, dbinfo *dbutil.DBConfig) Checker {
	return &MySQLVersionChecker{db: db, dbinfo: dbinfo}
}

// MinVersion is mysql minimal version required
var MinVersion = [3]uint{5, 5, 0}

// Check implements the Checker interface.
// we only support version >= 5.5
func (pc *MySQLVersionChecker) Check(ctx context.Context) *Result {
	result := &Result{
		Name:  pc.Name(),
		Desc:  "check whether mysql version is satisfied",
		State: StateFailure,
		Extra: fmt.Sprintf("address of db instance - %s:%d", pc.dbinfo.Host, pc.dbinfo.Port),
	}

	value, err := dbutil.ShowVersion(ctx, pc.db)
	if err != nil {
		markCheckError(result, err)
		return result
	}

	version, err := toMySQLVersion(value)
	if err != nil {
		markCheckError(result, err)
		return result
	}

	if !version.IsAtLeast(MinVersion) {
		result.ErrorMsg = fmt.Sprintf("version required at least %v but got %v", MinVersion, version)
		result.Instruction = "Please upgrade your database system"
		return result
	}

	result.State = StateSuccess
	return result
}

// Name implements the Checker interface.
func (pc *MySQLVersionChecker) Name() string {
	return "mysql_version"
}

/*****************************************************/

// MySQLServerIDChecker checks mysql/mariadb server ID.
type MySQLServerIDChecker struct {
	db     *sql.DB
	dbinfo *dbutil.DBConfig
}

// NewMySQLServerIDChecker returns a Checker
func NewMySQLServerIDChecker(db *sql.DB, dbinfo *dbutil.DBConfig) Checker {
	return &MySQLServerIDChecker{db: db, dbinfo: dbinfo}
}

// Check implements the Checker interface.
func (pc *MySQLServerIDChecker) Check(ctx context.Context) *Result {
	result := &Result{
		Name:  pc.Name(),
		Desc:  "check whether mysql server_id has been set > 1",
		State: StateFailure,
		Extra: fmt.Sprintf("address of db instance - %s:%d", pc.dbinfo.Host, pc.dbinfo.Port),
	}

	serverID, err := dbutil.ShowServerID(ctx, pc.db)
	if err != nil {
		if utils.OriginError(err) == sql.ErrNoRows {
			result.ErrorMsg = "server_id not set"
			result.Instruction = "please set server_id in your database"
		} else {
			markCheckError(result, err)
		}

		return result
	}

	if serverID == 0 {
		result.ErrorMsg = "server_id is 0"
		result.Instruction = "please set server_id greater than 0"
		return result
	}
	result.State = StateSuccess
	return result
}

// Name implements the Checker interface.
func (pc *MySQLServerIDChecker) Name() string {
	return "mysql_server_id"
}

// MySQLServerTimezoneChecker checks two instances whether in same timezone
type MySQLServerTimezoneChecker struct {
	source *sql.DB
	target *sql.DB
}

// NewMySQLServerTimezoneChecker returns a Checker
func NewMySQLServerTimezoneChecker(source, target *sql.DB) Checker {
	return &MySQLServerTimezoneChecker{source: source, target: target}
}

// Check implements the Checker interface.
func (tc *MySQLServerTimezoneChecker) Check(ctx context.Context) *Result {
	setErrorMsg := func(r *Result, dbOrigin, tzName string) {
		r.ErrorMsg = fmt.Sprintf("%s server %s not found", dbOrigin, tzName)
		r.Instruction = fmt.Sprintf("please set %s in your database", tzName)
	}

	result := &Result{
		Name:  tc.Name(),
		Desc:  "check whether two mysql instances in same timezone",
		State: StateWarning,
		Extra: fmt.Sprintf(""),
	}

	var sourceTZ, targetTZ string
	var err error
	sourceTZ, err = dbutil.GetDBTimezone(ctx, tc.source)
	if err != nil {
		setErrorMsg(result, "source", "global.time_zone")
		return result
	}
	if strings.ToLower(sourceTZ) == "system" {
		sourceTZ, err = dbutil.GetSystemTimezone(ctx, tc.source)
		if err != nil {
			setErrorMsg(result, "source", "system_time_zone")
			return result
		}
	}

	targetTZ, err = dbutil.GetDBTimezone(ctx, tc.target)
	if err != nil {
		setErrorMsg(result, "target", "global.time_zone")
		return result
	}
	if strings.ToLower(targetTZ) == "system" {
		targetTZ, err = dbutil.GetSystemTimezone(ctx, tc.target)
		if err != nil {
			setErrorMsg(result, "target", "system_time_zone")
			return result
		}
	}

	result.Extra = fmt.Sprintf("source db time_zone: %s, target db time_zone: %s", sourceTZ, targetTZ)
	if sourceTZ == targetTZ {
		result.State = StateSuccess
	}

	return result
}

// Name implements the Checker interface.
func (tc *MySQLServerTimezoneChecker) Name() string {
	return "mysql_server_timezone"
}
