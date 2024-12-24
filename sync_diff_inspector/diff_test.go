// Copyright 2024 PingCAP, Inc.
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
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"
)

func TestGetSnapshot(t *testing.T) {
	cases := []struct {
		latestSnapshot     []string
		snapshot           string
		expected           string
		latestSnapshotRows string
		snapshotRows       string
	}{
		{
			latestSnapshot: []string{},
			snapshot:       "1",
			expected:       "1",
		},
		{
			latestSnapshot: []string{"2"},
			snapshot:       "",
			expected:       "2",
		},
		{
			latestSnapshot: []string{"0"},
			snapshot:       "3",
			expected:       "3",
		},
		{
			latestSnapshot: []string{"4"},
			snapshot:       "0",
			expected:       "0",
		},
		{
			latestSnapshot: []string{"5"},
			snapshot:       "6",
			expected:       "5",
		},
		{
			latestSnapshot: []string{"7"},
			snapshot:       "6",
			expected:       "6",
		},
		{
			latestSnapshot:     []string{"2016-10-08 16:45:26"},
			snapshot:           "2017-10-08 16:45:26",
			expected:           "2016-10-08 16:45:26",
			latestSnapshotRows: "1475916326",
			snapshotRows:       "1507452326",
		},
		{
			latestSnapshot:     []string{"2017-10-08 16:45:26"},
			snapshot:           "2016-10-08 16:45:26",
			expected:           "2016-10-08 16:45:26",
			latestSnapshotRows: "1507452326",
			snapshotRows:       "1475916326",
		},
		{
			latestSnapshot: []string{"1"},
			snapshot:       "2016-10-08 16:45:26",
			expected:       "1",
			snapshotRows:   "1475916326",
		},
		{
			latestSnapshot:     []string{"2017-10-08 16:45:26"},
			snapshot:           "1",
			expected:           "1",
			latestSnapshotRows: "1507452326",
		},
		{
			latestSnapshot:     []string{"3814697265"},
			snapshot:           "2090-11-18 22:07:45.62",
			expected:           "3814697265",
			latestSnapshotRows: "3814697266.625",
		},
	}

	conn, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer conn.Close()

	for _, cs := range cases {
		if len(cs.latestSnapshotRows) > 0 {
			dataRows := sqlmock.NewRows([]string{""}).AddRow(cs.latestSnapshotRows)
			mock.ExpectQuery("SELECT unix_timestamp(?)").WillReturnRows(dataRows)
			dataRows = sqlmock.NewRows([]string{""}).AddRow(cs.snapshotRows)
			mock.ExpectQuery("SELECT unix_timestamp(?)").WillReturnRows(dataRows)
		}
		val := GetSnapshot(cs.latestSnapshot, cs.snapshot, conn)
		require.Equal(t, cs.expected, val)
	}

}
