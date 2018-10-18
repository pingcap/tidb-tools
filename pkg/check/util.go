/*************************************************************************
 *
 * PingCAP CONFIDENTIAL
 * __________________
 *
 *  [2015] - [2018] PingCAP Incorporated
 *  All Rights Reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of PingCAP Incorporated and its suppliers,
 * if any.  The intellectual and technical concepts contained
 * herein are proprietary to PingCAP Incorporated
 * and its suppliers and may be covered by P.R.China and Foreign Patents,
 * patents in process, and are protected by trade secret or copyright law.
 * Dissemination of this information or reproduction of this material
 * is strictly forbidden unless prior written permission is obtained
 * from PingCAP Incorporated.
 */

package check

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/juju/errors"
	"github.com/pingcap/tidb-tools/pkg/utils"
)

// MySQLVersion represents MySQL version number.
type MySQLVersion [3]uint

// version format:
// mysql        5.7.18-log
// mariadb      5.5.50-MariaDB-1~wheezy
// percona      5.7.19-17-log
// aliyun rds   5.7.18-log
// aws rds      5.7.16-log
// ref: https://dev.mysql.com/doc/refman/5.7/en/which-version.html

// v is mysql version in string format.
func toMySQLVersion(v string) (MySQLVersion, error) {
	version := MySQLVersion{0, 0, 0}
	tmp := strings.Split(v, "-")
	if len(tmp) == 0 {
		return version, errors.NotValidf("version %s", v)
	}

	tmp = strings.Split(tmp[0], ".")
	if len(tmp) != 3 {
		return version, errors.NotValidf("version %s", v)
	}

	for i := range tmp {
		val, err := strconv.ParseUint(tmp[i], 10, 64)
		if err != nil {
			return version, errors.NotValidf("version %s", v)
		}
		version[i] = uint(val)
	}
	return version, nil
}

// IsAtLeast means v >= min
func (v MySQLVersion) IsAtLeast(min MySQLVersion) bool {
	for i := range v {
		if v[i] > min[i] {
			return true
		} else if v[i] < min[i] {
			return false
		}
	}
	return true
}

// String implements the Stringer interface.
func (v MySQLVersion) String() string {
	return fmt.Sprintf("%d.%d.%d", v[0], v[1], v[2])
}

// IsMariaDB tells whether the version is from mariadb.
func IsMariaDB(version string) bool {
	return strings.Contains(strings.ToUpper(version), "MARIADB")
}

func markCheckError(result *Result, err error) {
	if err != nil {
		if utils.OriginError(err) == context.Canceled {
			result.State = StateWarning
		} else {
			result.State = StateFailure
		}
		result.ErrorMsg = fmt.Sprintf("%v\n%s", err, result.ErrorMsg)
	}
}
