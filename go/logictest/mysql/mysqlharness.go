
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

package main

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"reflect"
	"strings"
)

func main() {
	db, err := sql.Open("mysql", "sqllogictest:password@tcp(127.0.0.1:3306)/sqllogictest")
	if err != nil {
		panic(err)
	}

	rows, err := db.Query("show tables")
	if err != nil {
		panic(err)
	}

	columns, err := columns(rows)
	if err != nil {
		panic(err)
	}

	for rows.Next() {
		err = rows.Scan(columns...)
		if err != nil {
			panic(err)
		}

		for _, column := range columns {
			strPtr := column.(*string)
			fmt.Printf("%v\n", *strPtr)
		}
	}
}

// Returns a slice of columns suitable for scanning values into.
func columns(rows *sql.Rows) ([]interface{}, error) {
	types, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}

	sb := strings.Builder{}
	var columns []interface{}
	for _, columnType := range types {
		scanType := columnType.ScanType()
		switch scanType.Kind() {
		case reflect.Bool:
			colVal := false
			columns = append(columns, &colVal)
			sb.WriteString("I")
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			colVal := int64(0)
			columns = append(columns, &colVal)
			sb.WriteString("I")
		case reflect.Float32, reflect.Float64:
			colVal := float64(0)
			columns = append(columns, &colVal)
			sb.WriteString("R")
		case reflect.String, reflect.Slice: // the mysql driver returns a ScanType of slice for string results
			colVal := ""
			columns = append(columns, &colVal)
			sb.WriteString("T")
		default:
			return nil, fmt.Errorf("Unhandled type %d", scanType.Kind())
		}
	}

	return columns, nil
}
