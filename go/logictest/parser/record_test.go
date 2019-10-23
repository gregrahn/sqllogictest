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

package parser

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestRecordMethods(t *testing.T) {
	record := Record {
		recordType: Query,
		label:      "join-4-1",
		sortMode:   ValueSort,
		query: removeNewlines(`SELECT x29,x31,x51,x55
  FROM t51,t29,t31,t55
  WHERE a51=b31
    AND a29=6
    AND a29=b51
    AND b55=a31`),
		lineNum: 90,
		schema:  "TTTT",
		result:  []string{"table t29 row 6", "table t31 row 9", "table t51 row 5", "table t55 row 4"},
	}

	assert.Equal(t, Query, record.Type())
	assert.Equal(t, 4, record.NumCols())
	assert.Equal(t, 4, record.NumResults())
	assert.Equal(t, []string{"table t29 row 6", "table t31 row 9", "table t51 row 5", "table t55 row 4"}, record.Result())
	assert.Equal(t, 90, record.LineNum())
	assert.False(t, record.ExpectError())
	assert.False(t, record.IsHashResult())
	assert.True(t, record.ShouldExecuteForEngine("mysql"))
	assert.True(t, record.ShouldExecuteForEngine("postgresql"))
	assert.Equal(t, []string { "a", "b", "c", "d"}, record.SortResults([]string {"c", "a", "d", "b"}))

	record = Record {
		recordType: Query,
		schema:     "II",
		sortMode:   "nosort",
		query: removeNewlines(`SELECT a-b,
       CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END
  FROM t1
 WHERE c>d
   AND b>c
 ORDER BY 2,1`),
		conditions: []*Condition{
			{
				isSkip: true,
				engine: "mssql",
			},
		},
		result:  []string{"-3", "222", "-3", "222", "-1", "222", "-1", "222"},
		lineNum: 62,
	}

	assert.Equal(t, Query, record.Type())
	assert.Equal(t, 2, record.NumCols())
	assert.Equal(t, 8, record.NumResults())
	assert.Equal(t, []string{"-3", "222", "-3", "222", "-1", "222", "-1", "222"}, record.Result())
	assert.Equal(t, 62, record.LineNum())
	assert.False(t, record.ExpectError())
	assert.False(t, record.IsHashResult())
	assert.True(t, record.ShouldExecuteForEngine("mysql"))
	assert.False(t, record.ShouldExecuteForEngine("mssql"))
	assert.Equal(t, []string { "c", "b", "a"}, record.SortResults([]string {"c", "b", "a"}))

	record = Record{
		recordType: Query,
		schema:     "IIIII",
		sortMode:   "rowsort",
		query: removeNewlines(`SELECT a+b*2+c*3+d*4+e*5,
       CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
        WHEN a<b+3 THEN 333 ELSE 444 END,
       abs(b-c),
       (a+b+c+d+e)/5,
       a+b*2+c*3
  FROM t1
 WHERE (e>c OR e<d)
   AND d>e
   AND EXISTS(SELECT 1 FROM t1 AS x WHERE x.b<t1.b)
 ORDER BY 4,2,1,3,5`),
		conditions: []*Condition{
			{
				isOnly: true,
				engine: "mysql",
			},
		},
		result:  []string{"1", "2", "3", "4", "5"},
		lineNum: 41,
	}

	assert.Equal(t, Query, record.Type())
	assert.Equal(t, 5, record.NumCols())
	assert.Equal(t, 5, record.NumResults())
	assert.Equal(t, []string{"1", "2", "3", "4", "5"}, record.Result())
	assert.Equal(t, 41, record.LineNum())
	assert.False(t, record.ExpectError())
	assert.False(t, record.IsHashResult())
	assert.True(t, record.ShouldExecuteForEngine("mysql"))
	assert.False(t, record.ShouldExecuteForEngine("mssql"))
	assert.Equal(t, []string {
		"a", "j", "k", "e", "3",
		"b", "l", "2", "foo", "m",
		"c", "a", "z", "e", "f",
		"c", "a", "z", "e", "f",
		"c", "a", "z", "e", "g",
		"d", "b", "w", "q", "g",
	}, record.SortResults([]string {
		"c", "a", "z", "e", "g",
		"a", "j", "k", "e", "3",
		"d", "b", "w", "q", "g",
		"c", "a", "z", "e", "f",
		"b", "l", "2", "foo", "m",
		"c", "a", "z", "e", "f",
	}))

	record = Record {
	recordType: Query,
			schema:     "II",
			sortMode:   "nosort",
			label:      "label-1",
			query: removeNewlines(`SELECT a+b*2+c*3+d*4+e*5,
       (a+b+c+d+e)/5
  FROM t1
 ORDER BY 1,2`),
			result:  []string{"60 values hashing to 808146289313018fce25f1a280bd8c30"},
			lineNum: 29,
	}

	assert.Equal(t, Query, record.Type())
	assert.Equal(t, "II", record.schema)
	assert.Equal(t, 2, record.NumCols())
	assert.Equal(t, 60, record.NumResults())
	assert.Equal(t, 29, record.LineNum())
	assert.False(t, record.ExpectError())
	assert.True(t, record.IsHashResult())
	assert.Equal(t, "808146289313018fce25f1a280bd8c30", record.HashResult())
	assert.True(t, record.ShouldExecuteForEngine("mysql"))
	assert.Equal(t, []string { "c", "b", "a"}, record.SortResults([]string {"c", "b", "a"}))

	record = Record {
	 recordType:  Statement,
			expectError: false,
			query:       "INSERT INTO t1(e,c,b,d,a) VALUES(103,102,100,101,104)",
			lineNum:     5,
	}

	assert.Equal(t, Statement, record.Type())
	assert.Panics(t, func() {
		record.NumCols()
	})
	assert.Panics(t, func() {
		record.NumResults()
	})
	assert.Equal(t, 5, record.LineNum())
	assert.False(t, record.ExpectError())
	assert.True(t, record.ShouldExecuteForEngine("mysql"))

	record = Record {
		recordType:  Statement,
			expectError: true,
			query:       "INSERT INTO t1(a,c,d,e,b) VALUES(107,106,108,109,105)",
			lineNum:     8,
	}

	assert.Equal(t, Statement, record.Type())
	assert.Panics(t, func() {
		record.NumCols()
	})
	assert.Panics(t, func() {
		record.NumResults()
	})
	assert.Equal(t, 8, record.LineNum())
	assert.True(t, record.ExpectError())
	assert.True(t, record.ShouldExecuteForEngine("mysql"))

	record = Record{
		recordType: Query,
		sortMode:   NoSort,
		query:      removeNewlines(`SELECT 1 FROM t1 WHERE 1.0 IN ()`),
		lineNum:    106,
		schema:     "I",
		conditions: []*Condition{
			{
				isSkip: true,
				engine: "mysql",
			},
			{
				isSkip: true,
				engine: "mssql",
			},
			{
				isSkip: true,
				engine: "oracle",
			},
		},
	}

	assert.Equal(t, Query, record.Type())
	assert.Equal(t, 106, record.LineNum())
	assert.False(t, record.ExpectError())
	assert.False(t, record.IsHashResult())
	assert.False(t, record.ShouldExecuteForEngine("mysql"))
	assert.False(t, record.ShouldExecuteForEngine("mssql"))
	assert.False(t, record.ShouldExecuteForEngine("mysql"))
	assert.True(t, record.ShouldExecuteForEngine("postgresql"))
}
