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
	"bufio"
	"context"
	"crypto/md5"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/liquidata-inc/sqllogictest/go/logictest/parser"
)

var currTestFile string
var currRecord *parser.Record
var _, TruncateQueriesInLog = os.LookupEnv("SQLLOGICTEST_TRUNCATE_QUERIES")
var startTime time.Time
var recordTimeout = time.Hour * 1

// Runs the test files found under any of the paths given. Can specify individual test files, or directories that
// contain test files somewhere underneath. All files named *.test encountered under a directory will be attempted to be
// parsed as a test file, and will panic for malformed test files or paths that don't exist.
func RunTestFiles(harness Harness, paths ...string) {
	testFiles := collectTestFiles(paths)

	for _, file := range testFiles {
		runTestFile(harness, file)
	}
}

// Returns all the test files residing at the paths given.
func collectTestFiles(paths []string) []string {
	var testFiles []string
	for _, arg := range paths {
		abs, err := filepath.Abs(arg)
		if err != nil {
			panic(err)
		}

		stat, err := os.Stat(abs)
		if err != nil {
			panic(err)
		}

		if stat.IsDir() {
			filepath.Walk(arg, func(path string, info os.FileInfo, err error) error {
				if err != nil {
					return err
				}
				if info.IsDir() {
					return nil
				}

				if strings.HasSuffix(path, ".test") {
					testFiles = append(testFiles, path)
				}
				return nil
			})
		} else {
			testFiles = append(testFiles, abs)
		}
	}
	return testFiles
}

// Generates the test files given by executing the query and replacing expected results with the ones obtained by the
// test run. Files written will have the .generated suffix.
func GenerateTestFiles(harness Harness, paths ...string) {
	testFiles := collectTestFiles(paths)

	for _, file := range testFiles {
		generateTestFile(harness, file)
	}
}

func generateTestFile(harness Harness, f string) {
	currTestFile = f

	err := harness.Init()
	if err != nil {
		panic(err)
	}

	file, err := os.Open(f)
	if err != nil {
		panic(err)
	}

	testRecords, err := parser.ParseTestFile(f)
	if err != nil {
		panic(err)
	}

	generatedFile, err := os.Create(f + ".generated")
	if err != nil {
		panic(err)
	}

	scanner := &parser.LineScanner{
		bufio.NewScanner(file), 0,
	}
	wr := bufio.NewWriter(generatedFile)

	defer func() {
		err  = wr.Flush()
		if err != nil {
			panic(err)
		}

		err = generatedFile.Close()
		if err != nil {
			panic(err)
		}
	}()

	for _, record := range testRecords {
		schema, records, _, err := executeRecord(harness, record)

		// If there was an error or we skipped this test, then just copy output until the next record.
		if err != nil || !record.ShouldExecuteForEngine(harness.EngineStr()) {
			copyUntilEndOfRecord(scanner, wr) // advance until the next record
			continue
		} else if record.Type() == parser.Halt {
			copyRestOfFile(scanner, wr)
			return
		}

		// Copy until we get to the line before the query we executed (e.g. "query IIRT no-sort")
		for scanner.Scan() && scanner.LineNum < record.LineNum() - 1 {
			line := scanner.Text()
			writeLine(wr, line)
		}

		// Copy statements directly
		if record.Type() == parser.Statement {
			writeLine(wr, scanner.Text())
		// Fill in the actual query result schema
		} else if record.Type() == parser.Query {
			var label string
			if record.Label() != "" {
				label = " " + record.Label()
			}

			writeLine(wr, fmt.Sprintf("query %s %s%s", schema, record.SortString(), label))
			copyUntilSeparator(scanner, wr)   // copy the original query and separator
			writeResults(record, records, wr) // write the query result
			skipUntilEndOfRecord(scanner, wr) // advance until the next record
		}
	}

	copyRestOfFile(scanner, wr)
}

func writeLine(wr *bufio.Writer, s string) {
	_, err := wr.WriteString(s + "\n")
	if err != nil {
		panic(err)
	}
}

func copyRestOfFile(scanner *parser.LineScanner, wr *bufio.Writer) {
	for scanner.Scan() {
		writeLine(wr, scanner.Text())
	}
}

func writeResults(record *parser.Record, results []string, wr *bufio.Writer) {
	results = record.SortResults(results)

	if len(results) > record.HashThreshold() {
		hash, err := hashResults(results)
		if err != nil {
			panic(err)
		}
		writeLine(wr, fmt.Sprintf("%d values hashing to %s", len(results), hash))
	} else {
		for _, result := range results {
			writeLine(wr, fmt.Sprintf("%s", result))
		}
	}
}

func copyUntilSeparator(scanner *parser.LineScanner, wr *bufio.Writer) {
	for scanner.Scan() {
		line := scanner.Text()
		writeLine(wr, line)

		if strings.TrimSpace(line) == parser.Separator {
			break
		}
	}
}

func copyUntilEndOfRecord(scanner *parser.LineScanner, wr *bufio.Writer) {
	for scanner.Scan() {
		line := scanner.Text()
		writeLine(wr, line)
		if strings.TrimSpace(line) == "" {
			break
		}
	}
}

func skipUntilEndOfRecord(scanner *parser.LineScanner, wr *bufio.Writer) {
	for scanner.Scan() {
		line := scanner.Text()
		if strings.TrimSpace(line) == "" {
			writeLine(wr, "")
			break
		}
	}
}

func runTestFile(harness Harness, file string) {
	currTestFile = file

	err := harness.Init()
	if err != nil {
		panic(err)
	}

	testRecords, err := parser.ParseTestFile(file)
	if err != nil {
		panic(err)
	}

	for _, record := range testRecords {
		_, _, cont, _ := executeRecord(harness, record)
		if !cont {
			break
		}
	}
}

type R struct {
	schema string
	results []string
	cont bool
	err error
}

// Executes a single record and returns whether execution of records should continue
func executeRecord(harness Harness, record *parser.Record) (schema string, results []string, cont bool, err error) {
	currRecord = record
	startTime = time.Now()

	ctx, cancel := context.WithTimeout(context.Background(), recordTimeout)
	defer cancel()

	rc := make(chan *R, 1)
	go func() {
		schema, results, cont, err := execute(ctx, harness, record)
		rc <- &R{
			schema: schema,
			results: results,
			cont: cont,
			err: err,
		}
	}()

	select {
	case res := <-rc:
		return res.schema, res.results, res.cont, res.err
	case <-ctx.Done():
		logTimeout()

		// set cont to false so that no subsequent records in this file
		// we assume they will likely also timeout
		return "", []string{}, false, nil
	}
}

func execute(ctx context.Context, harness Harness, record *parser.Record) (schema string, results []string, cont bool, err error) {
	defer func() {
		if r := recover(); r != nil {
			toLog := r
			if str, ok := r.(string); ok {
				// attempt to keep entries on one line
				toLog = strings.ReplaceAll(str, "\n", " ")
			} else if err, ok := r.(error); ok {
				// attempt to keep entries on one line
				toLog = strings.ReplaceAll(err.Error(), "\n", " ")
			}
			logFailure(ctx, "Caught panic: %v", toLog)
			cont = true
		}
	}()

	if !record.ShouldExecuteForEngine(harness.EngineStr()) {
		// Log a skip for queries and statements only, not other control records
		if record.Type() == parser.Query || record.Type() == parser.Statement {
			logSkip(ctx)
		}
		return "", nil, true, nil
	}

	switch record.Type() {
	case parser.Statement:
		err := harness.ExecuteStatement(record.Query())

		if record.ExpectError() {
			if err == nil {
				logFailure(ctx, "Expected error but didn't get one")
				return "", nil, true, nil
			}
		} else if err != nil {
			logFailure(ctx, "Unexpected error %v", err)
			return "", nil, true, err
		}

		logSuccess(ctx)
		return "", nil, true, nil
	case parser.Query:
		schemaStr, results, err := harness.ExecuteQuery(record.Query())
		if err != nil {
			logFailure(ctx, "Unexpected error %v", err)
			return "", nil, true, err
		}

		// Only log one error per record, so if schema comparison fails don't bother with result comparison
		if verifySchema(ctx, record, schemaStr) {
			verifyResults(ctx, record, schemaStr, results)
		}
		return schemaStr, results, true, nil
	case parser.Halt:
		return "", nil, false, nil
	default:
		panic(fmt.Sprintf("Uncrecognized record type %v", record.Type()))
	}
}

func verifyResults(ctx context.Context, record *parser.Record, schema string, results []string) {
	if len(results) != record.NumResults() {
		logFailure(ctx, fmt.Sprintf("Incorrect number of results. Expected %v, got %v", record.NumResults(), len(results)))
		return
	}

	results = normalizeResults(results, schema)
	results = record.SortResults(results)

	if record.IsHashResult() {
		verifyHash(ctx, record, results)
	} else {
		verifyRows(ctx, record, results)
	}
}

// Normalizes the results according to the schema given.
// Test files have type rules that conform to MySQL's actual behavior, which is pretty odd in some cases. For example,
// the type of the expression `- - - 8` is decimal (float) as of MySQL 8.0. Rather than expect all databases to
// duplicate these semantics, we allow integer types to be freely converted to floats. This means we need to format
// integer results as float results, with three trailing zeros, where necessary.
func normalizeResults(results []string, schema string) []string {
	newResults := make([]string, len(results))
	for i := range results {
		typ := schema[i%len(schema)]
		if typ == 'R' && !strings.Contains(results[i], ".") {
			newResults[i] = results[i] + ".000"
		} else {
			newResults[i] = results[i]
		}
	}
	return newResults
}

// Verifies that the rows given exactly match the expected rows of the record, in the order given. Rows must have been
// previously sorted according to the semantics of the record.
func verifyRows(ctx context.Context, record *parser.Record, results []string) {
	for i := range record.Result() {
		if record.Result()[i] != results[i] {
			logFailure(ctx, "Incorrect result at position %d. Expected %v, got %v", i, record.Result()[i], results[i])
			return
		}
	}

	logSuccess(ctx)
}

// Verifies that the hash of the rows given exactly match the expected hash of the record given. Rows must have been
// previously sorted according to the semantics of the record.
func verifyHash(ctx context.Context, record *parser.Record, results []string) {
	results = record.SortResults(results)

	computedHash, err := hashResults(results)
	if err != nil {
		logFailure(ctx, "Error hashing results: %v", err)
		return
	}

	if record.HashResult() != computedHash {
		logFailure(ctx, "Hash of results differ. Expected %v, got %v", record.HashResult(), computedHash)
	} else {
		logSuccess(ctx)
	}
}

// Computes the md5 hash of the results given, using the same algorithm as the original sqllogictest C code.
func hashResults(results []string) (string, error) {
	h := md5.New()
	for _, r := range results {
		if _, err := h.Write(append([]byte(r), byte('\n'))); err != nil {
			return "", err
		}
	}
	return fmt.Sprintf("%x", h.Sum(nil)), nil
}

// Returns whether the schema given matches the record's expected schema, and logging an error if not.
func verifySchema(ctx context.Context, record *parser.Record, schemaStr string) bool {
	if schemaStr == record.Schema() {
		return true
	}

	if len(schemaStr) != len(record.Schema()) {
		logFailure(ctx, "Schemas differ. Expected %s, got %s", record.Schema(), schemaStr)
		return false
	}

	// MySQL has odd rules for when a result is a float v. an integer. Rather than try to replicate MySQL's type logic
	// exactly, we allow integer results in place of floats. See normalizeResults for details.
	for i, c := range record.Schema() {
		if !compatibleSchemaTypes(c, rune(schemaStr[i])) {
			logFailure(ctx, "Schemas differ. Expected %s, got %s", record.Schema(), schemaStr)
			return false
		}
	}
	return true
}

func compatibleSchemaTypes(expected, actual rune) bool {
	if expected != actual {
		if expected == 'R' && actual == 'I'{
			return true
		} else {
			return false
		}
	}
	return true
}

func logFailure(ctx context.Context, message string, args ...interface{}) {
	if ctx.Err() != nil {
		return
	}
	newMsg := logMessagePrefix() + " not ok: " + message
	failureMessage := fmt.Sprintf(newMsg, args...)
	failureMessage = strings.ReplaceAll(failureMessage, "\n", " ")
	fmt.Println(failureMessage)
}

func logSkip(ctx context.Context) {
	if ctx.Err() != nil {
		return
	}
	fmt.Println(logMessagePrefix(), "skipped")
}

func logSuccess(ctx context.Context) {
	if ctx.Err() != nil {
		return
	}
	fmt.Println(logMessagePrefix(), "ok")
}

func logTimeout() {
	fmt.Println(logMessagePrefix(), "timeout")
}

func logMessagePrefix() string {
	return fmt.Sprintf("%s %d %s:%d: %s",
		time.Now().Format(time.RFC3339Nano),
		time.Now().Sub(startTime).Milliseconds(),
		testFilePath(currTestFile),
		currRecord.LineNum(),
		truncateQuery(currRecord.Query()))
}

func testFilePath(f string) string {
	var pathElements []string
	filename := f

	for len(pathElements) < 4 && len(filename) > 0 {
		dir, file := filepath.Split(filename)
		// Stop recursing at the leading "test/" directory (root directory for the sqllogictest files)
		if file == "test" {
			break
		}
		pathElements = append([]string{file}, pathElements...)
		filename = filepath.Clean(dir)
	}

	return strings.ReplaceAll(filepath.Join(pathElements...), "\\", "/")
}

func truncateQuery(query string) string {
	if TruncateQueriesInLog && len(query) > 50 {
		return query[:47] + "..."
	}
	return query
}
