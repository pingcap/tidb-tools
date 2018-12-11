package dbutil

import (
	"strconv"

	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/types"
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

// IsTimeType returns true if tp is time type
func IsTimeType(tp byte) bool {
	if tp == mysql.TypeDatetime || tp == mysql.TypeTimestamp || tp == mysql.TypeDate {
		return true
	}
	return false
}

// FromPackedUint decodes Time from a packed uint64 value.
func FromPackedUint(packedStr string, tp byte) (string, error) {
	var t types.Time

	packed, err := strconv.ParseUint(packedStr, 10, 64)
	if err != nil {
		return "", err
	}

	if packed == 0 {
		t.Time = types.ZeroTime
		return "", nil
	}
	ymdhms := packed >> 24
	ymd := ymdhms >> 17
	day := int(ymd & (1<<5 - 1))
	ym := ymd >> 5
	month := int(ym % 13)
	year := int(ym / 13)

	hms := ymdhms & (1<<17 - 1)
	second := int(hms & (1<<6 - 1))
	minute := int((hms >> 6) & (1<<6 - 1))
	hour := int(hms >> 12)
	microsec := int(packed % (1 << 24))

	t.Time = types.FromDate(year, month, day, hour, minute, second, microsec)
	return t.String(), nil
}
