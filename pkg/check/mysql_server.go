package check

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/ngaut/log"
	"github.com/pingcap/tidb-tools/pkg/dbutil"
	"github.com/pingcap/tidb-tools/pkg/utils"
)

// MySQLVersionPreChecker checks mysql/mariadb/rds,... version.
type MySQLVersionPreChecker struct {
	db     *sql.DB
	dbinfo *dbutil.DBConfig
}

// NewMySQLVersionPreChecker returns a PreChecker
func NewMySQLVersionPreChecker(db *sql.DB, dbinfo *dbutil.DBConfig) Checker {
	return &MySQLVersionPreChecker{db: db, dbinfo: dbinfo}
}

// MinVersion is mysql minimal version required
var MinVersion = [3]uint{5, 5, 0}

// Check implements the PreChecker interface.
// we only support version >= 5.5
func (pc *MySQLVersionPreChecker) Check(ctx context.Context) *Result {
	result := &Result{
		Name:  pc.Name(),
		Desc:  "checks whether mysql version is satisfied",
		State: StateFailure,
		Extra: fmt.Sprintf("%s:%d", pc.dbinfo.Host, pc.dbinfo.Port),
	}
	defer log.Infof("[precheck] check mysql version, result %+v", result)

	value, err := dbutil.ShowVersion(ctx, pc.db)
	if err != nil {
		markCheckError(result, err)
		return result
	}

	version := toMySQLVersion(value)
	if !version.IsAtLeast(MinVersion) {
		result.ErrorMsg = fmt.Sprintf("version required at least %v but got %v", MinVersion, version)
		result.Instruction = "Please upgrade your database system"
		return result
	}

	result.State = StateSuccess
	return result
}

// Name implements the PreChecker interface.
func (pc *MySQLVersionPreChecker) Name() string {
	return "mysql_version"
}

/*****************************************************/

// MySQLServerIDPreChecker checks mysql/mariadb version.
type MySQLServerIDPreChecker struct {
	db     *sql.DB
	dbinfo *dbutil.DBConfig
}

// NewMySQLServerIDPreChecker returns a PreChecker
func NewMySQLServerIDPreChecker(db *sql.DB, dbinfo *dbutil.DBConfig) Checker {
	return &MySQLServerIDPreChecker{db: db, dbinfo: dbinfo}
}

// Check implements the PreChecker interface.
func (pc *MySQLServerIDPreChecker) Check(ctx context.Context) *Result {
	result := &Result{
		Name:  pc.Name(),
		Desc:  "checks whether mysql server_id has been set > 1",
		State: StateFailure,
		Extra: fmt.Sprintf("%s:%d", pc.dbinfo.Host, pc.dbinfo.Port),
	}
	defer log.Infof("[precheck] check mysql version, result %+v", result)

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

// Name implements the PreChecker interface.
func (pc *MySQLServerIDPreChecker) Name() string {
	return "mysql_server_id"
}
