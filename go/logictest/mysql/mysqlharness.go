
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

package mysql

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/liquidata-inc/sqllogictest/go/logictest"
	"strings"
)

// sqllogictest harness for MySQL databases.
type MysqlHarness struct {
	db *sql.DB
}

// compile check for interface compliance
var _ logictest.Harness = &MysqlHarness{}

// NewMysqlHarness returns a new MySQL test harness for the data source name given. Panics if it cannot open a
// connection using the DSN.
func NewMysqlHarness(dsn string) *MysqlHarness {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		panic(err)
	}
	return &MysqlHarness{db:db}
}

// See Harness.EngineStr
func (h *MysqlHarness) EngineStr() string {
	return "mysql"
}

// See Harness.Init
func (h *MysqlHarness) Init() error {
	return h.dropAllTables()
}

// See Harness.ExecuteStatement
func (h *MysqlHarness) ExecuteStatement(statement string) error {
	_, err := h.db.Exec(statement)
	return err
}

// See Harness.ExecuteQuery
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
		err := rows.Scan(columns...)
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
		err := rows.Scan(columns...)
		if err != nil {
			return err
		}

		tableName := columns[0].(*sql.NullString)
		tableNames = append(tableNames, tableName.String)
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

// Returns the string representation of the column value given
func stringVal(col interface{}) string {
	switch v := col.(type) {
	case *sql.NullBool:
		if !v.Valid {
			return "NULL"
		}
		if v.Bool {
			return "1"
		} else {
			return "0"
		}
	case *sql.NullInt64:
		if !v.Valid {
			return "NULL"
		}
		return fmt.Sprintf("%d", v.Int64)
	case *sql.NullFloat64:
		if !v.Valid {
			return "NULL"
		}
		return fmt.Sprintf("%.3f", v.Float64)
	case *sql.NullString:
		if !v.Valid {
			return "NULL"
		}
		return v.String
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
		switch columnType.DatabaseTypeName() {
		case "BIT":
			colVal := sql.NullBool{}
			columns = append(columns, &colVal)
			sb.WriteString("I")
		case "TEXT", "VARCHAR", "MEDIUMTEXT", "CHAR", "TINYTEXT":
			colVal := sql.NullString{}
			columns = append(columns, &colVal)
			sb.WriteString("T")
		case "DECIMAL", "DOUBLE", "FLOAT":
			colVal := sql.NullFloat64{}
			columns = append(columns, &colVal)
			sb.WriteString("R")
		case "MEDIUMINT", "INT", "BIGINT", "TINYINT", "SMALLINT":
			colVal := sql.NullInt64{}
			columns = append(columns, &colVal)
			sb.WriteString("I")
		default:
			return "", nil, fmt.Errorf("Unhandled type %s", columnType.DatabaseTypeName())
		}
	}

	return sb.String(), columns, nil
}
