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
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/liquidata-inc/sqllogictest/go/logictest/parser"
)

type ResultType int

const (
	Ok ResultType = iota
	NotOk
	Skipped
	Timeout
	DidNotRun
)

// ResultLogEntry is a single line in a sqllogictest result log file.
type ResultLogEntry struct {
	EntryTime    time.Time
	TestFile     string
	LineNum      int
	Query        string
	Duration	 time.Duration
	Result       ResultType
	ErrorMessage string
}

// ParseResultFile parses a result log file produced by the test runner and returns a slice of results, in the order
// that they occurred.
func ParseResultFile(f string) ([]*ResultLogEntry, error) {
	file, err := os.Open(f)
	if err != nil {
		panic(err)
	}

	var entries []*ResultLogEntry

	scanner := parser.LineScanner{Scanner: bufio.NewScanner(file)}

	for {
		entry, err := parseLogEntry(&scanner)
		if err == io.EOF {
			return entries, nil
		} else if err != nil {
			return nil, err
		}
		entries = append(entries, entry)
	}
}

func parseLogEntry(scanner *parser.LineScanner) (*ResultLogEntry, error) {
	entry := &ResultLogEntry{}

	var err error
	linesScanned := 0
	for scanner.Scan() {
		line := scanner.Text()
		linesScanned++

		// Sample line:
		// 2019-10-16T12:20:29.0594292-07:00 123456 index/random/10/slt_good_0.test:535: SELECT * FROM tab0 AS cor0 WHERE NULL <> 29 + col0 not ok: Schemas differ. Expected IIIIIII, got IIRTIRT
		firstSpace := strings.Index(line, " ")
		if firstSpace == -1 {
			// unrecognized log line, ignore and continue
			continue
		}

		entry.EntryTime, err = time.Parse(time.RFC3339Nano, line[:firstSpace])
		if err != nil {
			// unrecognized log line, ignore and continue
			continue
		}

		secondSpace := strings.Index(line[firstSpace+1:], " ")
		if secondSpace == -1 {
			// unrecognized log line, ignore and continue
			continue
		} else {
			secondSpace = secondSpace + firstSpace
		}

		entry.Duration, err = time.ParseDuration(fmt.Sprintf("%sms", line[firstSpace+1:secondSpace+1]))
		if err != nil {
			// unrecognized log line, ignore and continue
			continue
		}

		if strings.HasSuffix(line, "ok") {
			entry.Result = Ok
		} else if strings.Contains(line, "not ok:") {
			entry.Result = NotOk
		} else if strings.HasSuffix(line, "timeout") {
			entry.Result = Timeout
		} else if strings.HasSuffix(line, "skipped") {
			entry.Result = Skipped
		} else if strings.HasSuffix(line, "did not run") {
			entry.Result = DidNotRun
		} else {
			panic("Couldn't determine result of log line " + line)
		}

		colonIdx := strings.Index(line[secondSpace+1:], ":")
		if colonIdx == -1 {
			panic(fmt.Sprintf("Malformed line %v on line %d", line, scanner.LineNum))
		} else {
			colonIdx = colonIdx + secondSpace + 1
		}

		entry.TestFile = line[secondSpace+2 : colonIdx]
		colonIdx2 := strings.Index(line[colonIdx+1:], ":")
		if colonIdx2 == -1 {
			panic(fmt.Sprintf("Malformed line %v on line %d", line, scanner.LineNum))
		} else {
			colonIdx2 = colonIdx + 1 + colonIdx2
		}

		entry.LineNum, err = strconv.Atoi(line[colonIdx+1 : colonIdx2])
		if err != nil {
			panic(fmt.Sprintf("Failed to parse line number on line %v", scanner.LineNum))
		}

		switch entry.Result {
		case NotOk:
			eoq := strings.Index(line[colonIdx2+1:], "not ok: ") + colonIdx2 + 1
			entry.Query = line[colonIdx2+2 : eoq-1]
			entry.ErrorMessage = line[eoq+len("not ok: "):]
		case Timeout:
			eoq := strings.Index(line[colonIdx2+1:], "timeout") + colonIdx2 + 1
			entry.Query = line[colonIdx2+2 : eoq-1]
		case Ok:
			eoq := strings.Index(line[colonIdx2+1:], "ok") + colonIdx2 + 1
			entry.Query = line[colonIdx2+2 : eoq-1]
		case Skipped:
			eoq := strings.Index(line[colonIdx2+1:], "skipped") + colonIdx2 + 1
			entry.Query = line[colonIdx2+2 : eoq-1]
		case DidNotRun:
			eoq := strings.Index(line[colonIdx2+1:], "did not run") + colonIdx2 + 1
			entry.Query = line[colonIdx2+2 : eoq-1]
		}

		return entry, nil
	}

	if scanner.Err() != nil {
		return nil, scanner.Err()
	}

	if scanner.Err() == nil && linesScanned == 0 {
		return nil, io.EOF
	}

	return entry, nil
}
