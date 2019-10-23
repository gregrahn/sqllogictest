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
	"github.com/liquidata-inc/sqllogictest/go/logictest"
	"github.com/liquidata-inc/sqllogictest/go/logictest/mysql"
	"os"
)

// MySQL test runner. Assumes a local MySQL with user sqllogictest, password "password". Adjust as necessary. Uses the
// database "sqllogictest" for all operations, and will drop all tables in this database routinely.
func main() {
	args := os.Args[1:]

	harness := mysql.NewMysqlHarness("sqllogictest:password@tcp(127.0.0.1:3306)/sqllogictest")
	logictest.RunTestFiles(harness, args...)
}