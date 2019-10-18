// Copyright 2019 Liquidata, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package logictest

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestParseResultFile(t *testing.T) {
	entries, err := ParseResultFile("testdata/resultlog.txt")
	assert.NoError(t, err)

	expectedResults := []*ResultLogEntry{
		{
			EntryTime: mustParseTime("2019-10-16T16:02:18.3408696-07:00"),
			TestFile:  "evidence/in1.test",
			LineNum:   25,
			Query:     "SELECT 1 IN ()",
			Result:    Skipped,
		},
		{
			EntryTime: mustParseTime("2019-10-16T16:02:18.3418683-07:00"),
			TestFile:  "evidence/in1.test",
			LineNum:   30,
			Query:     "SELECT 1 IN (2)",
			Result:    Ok,
		},
		{
			EntryTime: mustParseTime("2019-10-16T16:02:18.3418683-07:00"),
			TestFile:  "evidence/in1.test",
			LineNum:   35,
			Query:     "SELECT 1 IN (2,3,4,5,6,7,8,9)",
			Result:    Ok,
		},
		{
			EntryTime: mustParseTime("2019-10-16T16:02:18.3418683-07:00"),
			TestFile:  "evidence/in1.test",
			LineNum:   41,
			Query:     "SELECT 1 NOT IN ()",
			Result:    Skipped,
		},
		{
			EntryTime: mustParseTime("2019-10-16T16:02:18.3418683-07:00"),
			TestFile:  "evidence/in1.test",
			LineNum:   46,
			Query:     "SELECT 1 NOT IN (2)",
			Result:    Ok,
		},
		{
			EntryTime: mustParseTime("2019-10-16T16:02:18.3418683-07:00"),
			TestFile:  "evidence/in1.test",
			LineNum:   51,
			Query:     "SELECT 1 NOT IN (2,3,4,5,6,7,8,9)",
			Result:    Ok,
		},
		{
			EntryTime: mustParseTime("2019-10-16T16:02:18.3418683-07:00"),
			TestFile:  "evidence/in1.test",
			LineNum:   57,
			Query:     "SELECT null IN ()",
			Result:    Skipped,
		},
		{
			EntryTime: mustParseTime("2019-10-16T16:02:18.3418683-07:00"),
			TestFile:  "evidence/in1.test",
			LineNum:   63,
			Query:     "SELECT null NOT IN ()",
			Result:    Skipped,
		},
		{
			EntryTime:    mustParseTime("2019-10-16T16:02:18.3428692-07:00"),
			TestFile:     "evidence/in1.test",
			LineNum:      68,
			Query:        "CREATE TABLE t1(x INTEGER)",
			Result:       NotOk,
			ErrorMessage: "Unexpected error no primary key columns",
		},
		{
			EntryTime: mustParseTime("2019-10-16T16:02:18.3428692-07:00"),
			TestFile:  "evidence/in1.test",
			LineNum:   72,
			Query:     "SELECT 1 IN t1",
			Result:    Skipped,
		},
	}

	assert.Equal(t, expectedResults, entries)
}

func mustParseTime(t string) time.Time {
	parsed, err := time.Parse(time.RFC3339Nano, t)
	if err != nil {
		panic(err)
	}
	return parsed
}
