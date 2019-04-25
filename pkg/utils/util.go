package utils

import (
	"fmt"
	"os"

	"github.com/pingcap/log"
)

// SliceToMap converts slice to map
func SliceToMap(slice []string) map[string]interface{} {
	sMap := make(map[string]interface{})
	for _, str := range slice {
		sMap[str] = struct{}{}
	}
	return sMap
}

// StringsToInterfaces converts string slice to interface slice
func StringsToInterfaces(strs []string) []interface{} {
	is := make([]interface{}, 0, len(strs))
	for _, str := range strs {
		is = append(is, str)
	}

	return is
}

func SyncLog() {
	syncErr := log.Sync()
	if syncErr != nil {
		fmt.Fprintln(os.Stderr, "sync log failed", syncErr)
	}
}
