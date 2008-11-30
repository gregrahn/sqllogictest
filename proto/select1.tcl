#!/usr/bin/tclsh
#
# Run this script to generate a larger prototype test script for
# sqllogictest.
#
expr {srand(0)}

# Scramble the $inlist into a random order.
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

puts {statement ok}
puts {CREATE TABLE t1(a INTEGER, b INTEGER, c INTEGER, d INTEGER, e INTEGER)}
puts {}

for {set i 0} {$i<30} {incr i} {
  set base [expr {$i*5+100}]
  set values {}
  for {set j 0} {$j<5} {incr j} {
    if {rand()<0.1} {
      lappend values NULL
    } else {
      lappend values [expr {$j+$base}]
    }
  }
  set values [scramble $values]
  set cols [scramble {a b c d e}]
  set sql "INSERT INTO t1([join $cols ,]) VALUES([join $values ,])"
  puts "statement ok"
  puts $sql
  puts ""
}

set rexpr {a b c d e a-b a-c a-d a-e b-c b-d b-e c-d c-e d-e
           abs(a) abs(a+b) coalesce(a,b,c,d,e)
           {(SELECT max(a) FROM t1)}
           {(SELECT min(b) FROM t1)}
           {(SELECT max(c)-max(d) FROM t1)}
}
set nrexpr [llength $rexpr]
set sequence {}
set type {}
for {set i 1} {$i<=$nrexpr} {incr i} {
  lappend sequence $i
  append type I
}
set wexpr {
  a>b
  b>c
  c>d
  d>e
  {c BETWEEN b AND d}
  {d BETWEEN 110 AND 120}
  {e+d BETWEEN a+b+10 AND c+130}
}
set nwexpr [llength $wexpr]

for {set i 0} {$i<100} {incr i} {
  set n [expr {int(rand()*($nrexpr-1))+1}]
  set r [lrange [scramble $rexpr] 1 $n]
  set sql "SELECT [join $r ",\n       "]\n  FROM t1"
  set m [expr {int(rand()*$nwexpr)}]
  if {$m>0} {
    set op [expr {rand()>0.5 ? "\n    OR " : "\n   AND "}]
    set w [lrange [scramble $wexpr] 1 $m]
    append sql "\n WHERE [join $w $op]"
  }
  incr n -1
  append sql "\n ORDER BY [join [scramble [lrange $sequence 0 $n]] ,]"
  append sql "\n LIMIT 4"
  puts "query [string range $type 0 $n] nosort"
  puts "$sql"
  puts ""
}
