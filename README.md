# sqllogictest

This is an unofficial mirror of the sqllogictests provided by SQLite, available at https://www.sqlite.org/sqllogictest/doc/trunk/about.wiki. In addition to the tests themselves, this repository also provides a go parser and runner for executing the tests against a database of your choice. Simply provide a test harness, then point the runner at a file or directory containing the subset of the sqllogictests you want to run.

# Parser

The parser for sqllogictest files can be found in [parser.go](go/logictest/parser/parser.go). The parser supports reading all records for test files in this unofficial mirror, with the exception of hash-threshold control records, which are ignored.

## Records

Each test script contains many [records](go/logictest/parser/record.go), to be executed in order by a runner. In addition to standard getter methods the query string, results, etc., the Record object also contains several useful helper methods:

* `NumResults` returns the number of expected results for a record, correctly handling both native and hashed results
* `HashResult` returns the md5 string for expected results of the query, for records that expect a hash result
* `ShouldExecuteForEngine` considers the `onlyif` and `skipif` conditions for a record and returns whether a record should be run for a given engine.
* `SortResults` sorts a set of result strings according to the rules of the record

# Harness

The harness for running a test against a database engine of your choice is defined in [harness.go](go/logictest/harness.go).

For an example of how to define a harness against a real database, see the [harness defined by dolt](https://github.com/liquidata-inc/dolt/blob/master/go/libraries/doltcore/sqle/logictest/dolt/doltharness.go). Additional examples for other databases will be added to this mirror over time.

# Runner

The [runner](go/logictest/runner.go) parses one or more test files, runs them against a harness you provide, and emits pass or failure results in a log format. For those interested in analyzing test results automatically, logs emitted by the runner can be parsed back into structs with the [result_parser](go/logictest/resultparser.go) methods.

Example output of the runner looks like:

```
2019-10-16T16:02:18.3418683-07:00 evidence/in1.test:51: SELECT 1 NOT IN (2,3,4,5,6,7,8,9) ok
2019-10-16T16:02:18.3418683-07:00 evidence/in1.test:57: SELECT null IN () skipped
2019-10-16T16:02:18.3418683-07:00 evidence/in1.test:63: SELECT null NOT IN () skipped
2019-10-16T16:02:18.3428692-07:00 evidence/in1.test:68: CREATE TABLE t1(x INTEGER) not ok: Unexpected error no primary key columns
```

# Dolt database results

For [dolt](https://github.com/liquidata-inc/dolt), we periodically publish dolt's performance against these tests to a [dolt repository](https://www.dolthub.com/repositories/Liquidata/dolt-sqllogictest-results/data/master/dolt_results). You can clone the repository and inspect the results using the `dolt` tool:

```
% dolt clone Liquidata/dolt-sqllogictest-results
% cd dolt-sqllogictest-results
% dolt sql -q "select result, count(*) from dolt_results group by 1"
+---------+----------+
| result  | COUNT(*) |
+---------+----------+
| skipped | 1315601  |
| ok      | 1335695  |
| not ok  | 4233009  |
+---------+----------+
```
