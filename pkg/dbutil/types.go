package dbutil

import (
	"github.com/pingcap/tidb/pkg/parser/mysql"
)

// IsNumberType returns true if tp is number type
func IsNumberType(tp byte) bool {
	switch tp {
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeLong, mysql.TypeLonglong, mysql.TypeInt24, mysql.TypeYear:
		return true
	}

	return false
}

// IsFloatType returns true if tp is float type
func IsFloatType(tp byte) bool {
	switch tp {
	case mysql.TypeFloat, mysql.TypeDouble, mysql.TypeNewDecimal:
		return true
	}

	return false
}

// IsTimeTypeAndNeedDecode returns true if tp is time type and encoded in tidb buckets.
func IsTimeTypeAndNeedDecode(tp byte) bool {
	if tp == mysql.TypeDatetime || tp == mysql.TypeTimestamp || tp == mysql.TypeDate {
		return true
	}
	return false
}

// IsBlobType returns true if tp is Blob type
func IsBlobType(tp byte) bool {
	switch tp {
	case mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeBlob, mysql.TypeLongBlob:
		return true
	}

	return false
}
