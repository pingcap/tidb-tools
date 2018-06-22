package precheck

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/ngaut/log"
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
func toMySQLVersion(v string) MySQLVersion {
	tmp := strings.Split(v, "-")
	tmp = strings.Split(tmp[0], ".")
	if len(tmp) != 3 {
		log.Warnf("[precheck] invalid version %s", v)
		return [3]uint{0, 0, 0}
	}
	version := [3]uint{}
	for i := range tmp {
		v, err := strconv.ParseUint(tmp[i], 10, 64)
		if err != nil {
			log.Warnf("[precheck] invalid version %s", v)
			return [3]uint{0, 0, 0}
		}
		version[i] = uint(v)
	}
	return version
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
