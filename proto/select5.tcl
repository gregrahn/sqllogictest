#!/usr/bin/tclsh
#
# Run this script to generate a larger prototype test script for
# sqllogictest.
#
# Thirty separate tables, T1 through T30, each with an integer primary
# key, integer data, and a text identifier.  The primary keys are
# numbered from 1 to 30.  Data is the same 30 integers scrambled.
# Used to test deep joins.
#
expr {srand(0)}

# Scramble the $inlist list into a random order.
#
proc scramble {inlist} {
  set y {}
  foreach x $inlist {
    lappend y [list [expr {rand()}] $x]
  }
  set y [lsort $y]
  set outlist {}
  foreach x $y {
    lappend outlist [lindex $x 1]
  }
  return $outlist
}

set N 64
set M 10
set sequence {}
for {set i 1} {$i<=$M} {incr i} {
  lappend sequence $i
}
set tableset {}
set tablenums {}

# Create $N tables each with $M entries
#
for {set tn 1} {$tn<=$N} {incr tn} {
  lappend tableset t$tn
  lappend tablenums $tn
  set sql "CREATE TABLE t${tn}(\n"
  append sql "  a$tn INTEGER PRIMARY KEY,\n"
  append sql "  b$tn INTEGER,\n"
  append sql "  x$tn VARCHAR(40)\n"
  append sql ")"
  puts "statement ok\n$sql\n"

  foreach a $sequence b [scramble $sequence] {
    set sql "INSERT INTO t$tn VALUES($a,$b,'table t$tn row $a')"
    puts "statement ok\n$sql\n"
  }
}

# Generate a deep join
#
proc generate_deep_join {depth i} {
  global sequence tablenums M

  set tset1 [lrange [scramble $tablenums] 0 [expr {$depth-1}]]
  set tset2 [scramble $tset1]
  set typestr [string range \
    TTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTT \
    1 $depth]

  set head "SELECT x[join $tset1 ",x"]\n"
  unset -nocomplain seen
  set initw a[lindex $tset2 end]=[expr {int(rand()*$M)+1}]
  for {set j 0} {$j<3} {incr j} {
    set from "  FROM t[join [scramble $tset2] ",t"]\n"
    set w $initw
    foreach a [lrange $tset2 1 end] b [lrange $tset2 0 end-1] {
      if {rand()<0.5} {
        lappend w a$a=b$b
      } else {
        lappend w b$b=a$a
      }
    }
    set where " WHERE [join [scramble $w] "\n   AND "]"
    set sql $head$from$where
    if {[info exists seen($sql)]} {
      incr j -1
      continue
    }
    set seen($sql) 1
    puts "query $typestr valuesort join-$depth-$i\n$head$from$where\n"
  }
}

# Generate deep joins
#
for {set depth 4} {$depth<=$N} {incr depth} {
  for {set i 1} {$i<=4} {incr i} {
    generate_deep_join $depth $i
  }
}
