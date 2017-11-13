// Copyright 2016 PingCAP, Inc.
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

package main

import (
	"net"
	"net/url"
	"sort"
	"strings"

	"github.com/juju/errors"
)

// URLs defines a slice of URLs as a type
type URLs []url.URL

// NewURLs return a URLs from a slice of formatted URL strings
func NewURLs(strs []string) (URLs, error) {
	all := make([]url.URL, len(strs))
	if len(all) == 0 {
		return nil, errors.New("no valid URLs given")
	}
	for i, in := range strs {
		in = strings.TrimSpace(in)
		u, err := url.Parse(in)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if u.Scheme != "http" && u.Scheme != "https" && u.Scheme != "unix" && u.Scheme != "unixs" {
			return nil, errors.Errorf("URL scheme must be http, https, unix, or unixs: %s", in)
		}
		if _, _, err := net.SplitHostPort(u.Host); err != nil {
			return nil, errors.Errorf(`URL address does not have the form "host:port": %s`, in)
		}
		if u.Path != "" {
			return nil, errors.Errorf("URL must not contain a path: %s", in)
		}
		all[i] = *u
	}
	us := URLs(all)
	us.Sort()

	return us, nil
}

// String return a string of list of URLs witch separated by comma
func (us URLs) String() string {
	return strings.Join(us.StringSlice(), ",")
}

// Sort sorts the URLs
func (us *URLs) Sort() {
	sort.Sort(us)
}

// Len return the lenght of URL slice
func (us URLs) Len() int { return len(us) }

// Less compares two URL and return the less one
func (us URLs) Less(i, j int) bool { return us[i].String() < us[j].String() }

// Swap swaps two URLs in the slice
func (us URLs) Swap(i, j int) { us[i], us[j] = us[j], us[i] }

// StringSlice return a slice of formatted string of URL
func (us URLs) StringSlice() []string {
	out := make([]string, len(us))
	for i := range us {
		out[i] = us[i].String()
	}

	return out
}

// NewURLsValue return a URLsValue from a string of URLs list
func NewURLsValue(init string) (*URLs, error) {
	v := &URLs{}
	err := v.Set(init)
	return v, err
}

// Set set URLs use string
func (us *URLs) Set(s string) error {
	strs := strings.Split(s, ",")
	nus, err := NewURLs(strs)
	if err != nil {
		return errors.Trace(err)
	}

	*us = URLs(nus)
	return nil
}

// HostString return a string of host:port format list separated by comma
func (us *URLs) HostString() string {
	all := make([]string, len(*us))
	for i, u := range *us {
		all[i] = u.Host
	}
	return strings.Join(all, ",")
}
