#!/usr/bin/tclsh
#
# Run this script in the "src" subdirectory of sqllogictest, after first
# compiling the ./sqllogictest binary, in order to verify correct output
# of all historical test cases.
#

set starttime [clock seconds]

if {$tcl_platform(platform)=="unix"} {
  set BIN ./sqllogictest
} else {
  set BIN ./sqllogictest.exe
}
if {![file exec $BIN]} {
  error "$BIN does not exist or is not executable.  Run make."
}

# add all test case file in the $subdir subdirectory to the
# set of all test case files in the global tcase() array.
#
proc search_for_test_cases {subdir} {
  foreach nx [glob -nocomplain $subdir/*] {
    if {[file isdir $nx]} {
      search_for_test_cases $nx
    } elseif {[string match *.test $nx]} {
      set ::tcase($nx) 1
    }
  }
}
search_for_test_cases ../test

# Run the tests
#
set totalerr 0
set totaltest 0
set totalrun 0
foreach tx [lsort [array names tcase]] {
  foreach opt {0 0xfff} {
    set opt "integrity_check;optimizer=[expr {$opt+0}]"
    catch {
      exec $BIN -verify -parameter $opt $tx
    } res
    puts $res
    if {[regexp {(\d+) errors out of (\d+) tests} $res all nerr ntst]} {
      incr totalerr $nerr
      incr totaltest $ntst
    } else {
      error "test did not complete: $BIN -verify -parameter optimizer=$opt $tx"
    }
    incr totalrun
  }
}

set endtime [clock seconds]
set totaltime [expr {$endtime - $starttime}]
puts "$totalerr errors out of $totaltest tests and $totalrun invocations\
      in $totaltime seconds"
