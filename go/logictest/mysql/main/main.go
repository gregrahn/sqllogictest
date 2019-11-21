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
	"fmt"
	"github.com/liquidata-inc/sqllogictest/go/logictest"
	"github.com/liquidata-inc/sqllogictest/go/logictest/mysql"
	"os"
)

// MySQL test runner. Assumes a local MySQL with user sqllogictest, password "password". Adjust as necessary. Uses the
// database "sqllogictest" for all operations, and will drop all tables in this database routinely.
//
// Two modes, controlled by the first argument:
// verify: Runs the test files given, outputting a pass / fail line to STDOUT for each test record. All arguments after
//  the first are interpreted as test files or directories, which contain tests to be run. For directory arguments,
//  directories are descended recursively, and all files with the .test extension will be added to the list of tests.
// generate: Runs tests as verify does, but also produces a new version of each test file, named $testfile.generated,
//  with the results of this test run.
// Usage: go run main.go (verify|generate) testfile1 [testfile2 ...]
func main() {
	args := os.Args[1:]

	harness := mysql.NewMysqlHarness("sqllogictest:password@tcp(127.0.0.1:3306)/sqllogictest")

	mode := args[0]
	switch mode {
	case "verify":
		logictest.RunTestFiles(harness, args[1:]...)
	case "generate":
		logictest.GenerateTestFiles(harness, args[1:]...)
	default:
		fmt.Println("Usage: main (verify|generate) testfile1 [testfiles2 ...] ")
		os.Exit(1)
	}
}