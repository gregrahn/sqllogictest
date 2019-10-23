
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
	"github.com/liquidata-inc/sqllogictest/go/logictest"
	"os"
	"reflect"
	"strings"
)

func main() {
	args := os.Args[1:]

	harness := NewMysqlHarness("sqllogictest:password@tcp(127.0.0.1:3306)/sqllogictest")
	logictest.RunTestFiles(harness, args...)
}

type MysqlHarness struct {
	db *sql.DB
}

func NewMysqlHarness(dsn string) *MysqlHarness {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		panic(err)
	}
	return &MysqlHarness{db:db}
}

func (h *MysqlHarness) EngineStr() string {
	return "mysql"
}

func (h *MysqlHarness) Init() error {
	return h.dropAllTables()
}

func (h *MysqlHarness) dropAllTables() error {
	rows, err := h.db.Query("show tables")
	if err != nil {
		return err
	}

	_, columns, err := columns(rows)
	if err != nil {
		return err
	}

	var tableNames []string
	for rows.Next() {
		err := rows.Scan(columns)
		if err != nil {
			return err
		}

		tableName := columns[0].(*string)
		tableNames = append(tableNames, *tableName)
	}

	if len(tableNames) > 0 {
		dropTables := "drop table if exists " + strings.Join(tableNames, ",")
		_, err = h.db.Exec(dropTables)
		if err != nil {
			return err
		}
	}

	return nil
}

func (h *MysqlHarness) ExecuteStatement(statement string) error {
	_, err := h.db.Exec(statement)
	return err
}

func (h *MysqlHarness) ExecuteQuery(statement string) (schema string, results []string, err error) {
	rows, err := h.db.Query(statement)
	if err != nil {
		return "", nil, err
	}

	schema, columns, err := columns(rows)
	if err != nil {
		return "", nil, err
	}

	for rows.Next() {
		err := rows.Scan(columns)
		if err != nil {
			return "", nil, err
		}

		for _, col := range columns {
			results = append(results, stringVal(col))
		}
	}

	if rows.Err() != nil {
		return "", nil, rows.Err()
	}

	return schema, results, nil
}

// Returns the string representation of the column value given
func stringVal(col interface{}) string {
	switch v := col.(type) {
	case bool:
		if v {
			return "1"
		} else {
			return "0"
		}
	case int64:
		return fmt.Sprintf("%d", v)
	case float64:
		return fmt.Sprintf("%.3f", v)
	case string:
		return v
	default:
		panic(fmt.Sprintf("unhandled type %T for value %v", v, v))
	}
}

// Returns the schema for the rows given, as well as a slice of columns suitable for scanning values into.
func columns(rows *sql.Rows) (string, []interface{}, error) {
	types, err := rows.ColumnTypes()
	if err != nil {
		return "", nil, err
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
			return "", nil, fmt.Errorf("Unhandled type %d", scanType.Kind())
		}
	}

	return "", columns, nil
}
