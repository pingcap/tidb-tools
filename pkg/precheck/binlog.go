package precheck

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb-tools/pkg/dbutil"
)

// MySQLBinlogEnablePreChecker checks whether `log_bin` variable is enabled in MySQL.
type MySQLBinlogEnablePreChecker struct {
	db     *sql.DB
	dbinfo *dbutil.DBConfig
}

// NewMySQLBinlogEnablePreChecker returns a PreChecker.
func NewMySQLBinlogEnablePreChecker(db *sql.DB, dbinfo *dbutil.DBConfig) PreChecker {
	return &MySQLBinlogEnablePreChecker{db: db, dbinfo: dbinfo}
}

// PreCheck implements the PreChecker interface.
func (pc *MySQLBinlogEnablePreChecker) PreCheck() *PreCheckResult {
	result := &PreCheckResult{
		Name:  pc.Name(),
		Desc:  "checks whether mysql binlog is enable",
		State: PreCheckState_Failure,
		Extra: fmt.Sprintf("%s:%d", pc.dbinfo.Host, pc.dbinfo.Port),
	}
	defer log.Infof("[precheck] check binlog enable, result %+v", result)

	value, err := dbutil.ShowLogBin(pc.db)
	if err != nil {
		result.ErrorMsg = errors.ErrorStack(err)
		return result
	}
	if strings.ToUpper(value) != "ON" {
		result.ErrorMsg = fmt.Sprintf("log_bin is %s, and should be ON", value)
		result.Instruction = "ref: https://dev.mysql.com/doc/refman/5.7/en/replication-howto-masterbaseconfig.html"
		return result
	}
	result.State = PreCheckState_Success
	return result
}

// Name implements the PreChecker interface.
func (pc *MySQLBinlogEnablePreChecker) Name() string {
	return "mysql_binlog_enable"
}

/*****************************************************/

// MySQLBinlogFormatPreChecker checks mysql binlog_format.
type MySQLBinlogFormatPreChecker struct {
	db     *sql.DB
	dbinfo *dbutil.DBConfig
}

// NewMySQLBinlogFormatPreChecker returns a PreChecker.
func NewMySQLBinlogFormatPreChecker(db *sql.DB, dbinfo *dbutil.DBConfig) PreChecker {
	return &MySQLBinlogFormatPreChecker{db: db, dbinfo: dbinfo}
}

// PreCheck implements the PreChecker interface.
func (pc *MySQLBinlogFormatPreChecker) PreCheck() *PreCheckResult {
	result := &PreCheckResult{
		Name:  pc.Name(),
		Desc:  "checks whether mysql binlog_format is ROW",
		State: PreCheckState_Failure,
		Extra: fmt.Sprintf("%s:%d", pc.dbinfo.Host, pc.dbinfo.Port),
	}
	defer log.Infof("[precheck] check binlog_format, result %+v", result)

	value, err := dbutil.ShowBinlogFormat(pc.db)
	if err != nil {
		result.ErrorMsg = errors.ErrorStack(err)
		return result
	}
	if strings.ToUpper(value) != "ROW" {
		result.ErrorMsg = fmt.Sprintf("binlog_format is %s, and should be ROW", value)
		result.Instruction = "set global binlog_format=ROW;"
		return result
	}
	result.State = PreCheckState_Success

	return result
}

// Name implements the PreChecker interface.
func (pc *MySQLBinlogFormatPreChecker) Name() string {
	return "mysql_binlog_format"
}

/*****************************************************/

var (
	mysqlBinlogRowImageRequired   MySQLVersion = [3]uint{5, 6, 2}
	mariadbBinlogRowImageRequired MySQLVersion = [3]uint{10, 1, 6}
)

// MySQLBinlogRowImagePreChecker checks mysql binlog_row_image
type MySQLBinlogRowImagePreChecker struct {
	db     *sql.DB
	dbinfo *dbutil.DBConfig
}

// NewMySQLBinlogRowImagePreChecker returns a PreChecker
func NewMySQLBinlogRowImagePreChecker(db *sql.DB, dbinfo *dbutil.DBConfig) PreChecker {
	return &MySQLBinlogRowImagePreChecker{db: db, dbinfo: dbinfo}
}

// PreCheck implements the PreChecker interface.
// 'binlog_row_image' is introduced since mysql 5.6.2, and mariadb 10.1.6.
// > In MySQL 5.5 and earlier, full row images are always used for both before images and after images.
// So we need check 'binlog_row_image' after mysql 5.6.2 version and mariadb 10.1.6.
// ref:
// - https://dev.mysql.com/doc/refman/5.6/en/replication-options-binary-log.html#sysvar_binlog_row_image
// - https://mariadb.com/kb/en/library/replication-and-binary-log-server-system-variables/#binlog_row_image
func (pc *MySQLBinlogRowImagePreChecker) PreCheck() *PreCheckResult {
	result := &PreCheckResult{
		Name:  pc.Name(),
		Desc:  "checks whether mysql binlog_row_image is FULL",
		State: PreCheckState_Failure,
		Extra: fmt.Sprintf("%s:%d", pc.dbinfo.Host, pc.dbinfo.Port),
	}
	defer log.Infof("[precheck] check binlog_row_image, result %+v", result)

	// check version firstly
	value, err := dbutil.ShowVersion(pc.db)
	if err != nil {
		result.ErrorMsg = errors.ErrorStack(err)
		return result
	}
	version := toMySQLVersion(value)

	// for mysql.version < 5.6.2 || mariadb.version < 10.1.6,  we don't need to check binlog_row_image.
	if (!IsMariaDB(value) && !version.IsAtLeast(mysqlBinlogRowImageRequired)) || (IsMariaDB(value) && !version.IsAtLeast(mariadbBinlogRowImageRequired)) {
		result.State = PreCheckState_Success
		return result
	}

	value, err = dbutil.ShowBinlogRowImage(pc.db)
	if err != nil {
		result.ErrorMsg = errors.ErrorStack(err)
		return result
	}
	if strings.ToUpper(value) != "FULL" {
		result.ErrorMsg = fmt.Sprintf("binlog_row_image is %s, and should be FULL", value)
		result.Instruction = "set global binlog_row_image = FULL;"
		return result
	}
	result.State = PreCheckState_Success
	return result
}

// Name implements the PreChecker interface.
func (pc *MySQLBinlogRowImagePreChecker) Name() string {
	return "mysql_binlog_row_image"
}
