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

package utils

import (
	"net"
	"strings"

	"github.com/juju/errors"
)

// ParseHostPortAddr returns a host:port list
func ParseHostPortAddr(s string) ([]string, error) {
	strs := strings.Split(s, ",")
	addrs := make([]string, 0, len(strs))
	for _, str := range strs {
		str = strings.TrimSpace(str)
		_, _, err := net.SplitHostPort(str)
		if err != nil {
			return nil, errors.Trace(err)
		}
		addrs = append(addrs, str)
	}
	return addrs, nil
}
