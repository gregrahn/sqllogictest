#!/usr/bin/tclsh
#
# Run this script to generate a larger prototype test script for
# sqllogictest.
#
# Nine separate tables, T1 through T9, each with integer data.
# Data for table T1 is values in the range 1000..1999.  Data for
# table T2 is values in the range 2000.2999.  And so forth.  The
# data is random.
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

# Construct the schema.  9 tables, each with 5 integer columns and one
# text column.
#
for {set tn 1} {$tn<=9} {incr tn} {
  puts {statement ok}
  puts [subst {CREATE TABLE t${tn}(
  a$tn INTEGER,
  b$tn INTEGER,
  c$tn INTEGER,
  d$tn INTEGER,
  e$tn INTEGER,
  x$tn VARCHAR(30)
)}]
  puts {}
}

# Populate the tables with data
#
for {set tn 1} {$tn<=9} {incr tn} {set nrow($tn) 0}
for {set i 0} {$i<1000} {incr i} {
  set tn [expr {int(rand()*9)+1}]
  set base 0 ;# [expr {$tn*1000}]
  incr nrow($tn)
  set x "table tn$tn row $nrow($tn)"
  foreach column {a b c d e} {
    set v [expr {$base+int(rand()*999)}]
    lappend cdata($column$tn) $v
    set $column $v
  }
  lappend tdata($tn) [list $a $b $c $d $e $x]
  set sql "INSERT INTO t$tn VALUES($a,$b,$c,$d,$e,'$x')"
  puts "statement ok\n$sql\n"
}

# Queries to make sure all the data got in correctly.
#
for {set tn 1} {$tn<=9} {incr tn} {
  puts "query IIIIIT rowsort all$tn\nSELECT * FROM t$tn\n"
}

# Create indices
#
# t1 gets prefix indices...
#
for {set i 0} {$i<5} {incr i} {
  puts "statement ok"
  puts "CREATE INDEX t1i$i ON t1([join [lrange {a1 b1 c1 d1 e1 x1} $i end] ,])"
  puts ""
}
# t2 gets a separate index on each column
#
foreach c {a2 b2 c2 d2 e2} {
  puts "statement ok"
  puts "CREATE INDEX t2$c ON t2($c)"
  puts ""
}
# t3 through t7 get a single index on one column
#
for {set i 0} {$i<5} {incr i} {
  set tn [expr {$i+3}]
  set cn [string index {abcde} $i]$tn
  puts "statement ok"
  puts "CREATE INDEX t$tn$cn ON t${tn}($cn)"
  puts ""
}
# t8 gets a single reverse-order index.
#
puts "statement ok"
puts "CREATE INDEX t8all ON t8(e8 DESC, d8 ASC, c8 DESC, b8 ASC, a8 DESC)"
puts ""
# t9 does not get an index.
#

# Repeat the data dumps.  The table contents should not have changed.
#
for {set tn 1} {$tn<=9} {incr tn} {
  puts "query IIIIIT rowsort all$tn\nSELECT * FROM t$tn\n"
}

# Return a list of one or more WHERE clause terms that will restrict
# the number of rows obtained from table $tn to a handful.
#
proc few_row_where {tn} {
  global cdata tdata
  set w {}
  set n [expr {int(rand()*3)+1}]
  for {set i 0} {$i<$n} {incr i} {
    set p [expr {rand()}]
    if {$p<0.3333} {
      set cn [string index abcde [expr {int(rand()*5)}]]$tn
      set m [expr {int(rand()*12)+2}]
      set term "$cn in ([join [lrange [scramble $::cdata($cn)] 0 $m] ,])"
    } elseif {$p<0.6666} {
      set nrow [llength $tdata($tn)]
      set idx [expr {int(rand()*$nrow)}]
      set row [lindex $tdata($tn) $idx]
      set m [expr {int(rand()*5)}]
      set r [lrange [scramble {0 1 2 3 4}] 0 $m]
      set term {}
      set conn (
      foreach k $r {
        set cn [string index abcde $k]$tn
        set vx [lindex $row $k]
        if {rand()<0.5} {
          append term $conn$vx=$cn
        } else {
          append term $conn$cn=$vx
        }
        set conn " AND "
      }
      append term )
    } else {
      set m [expr {int(rand()*3)+1}]
      set term {}
      set conn (
      for {set k 0} {$k<$m} {incr k} {
        set cn [string index abcde [expr {int(rand()*5)}]]$tn
        set nv [llength $cdata($cn)]
        set idx [expr {int(rand()*$nv)}]
        set vx [lindex $cdata($cn) $idx]
        if {rand()<0.5} {
          append term $conn$vx=$cn
        } else {
          append term $conn$cn=$vx
        }
        set conn " OR "
      }
      append term )
    }
    lappend w $term
  }
  return $w
}

# Do lots of compound queries across multiple tables.
#
for {set i 0} {$i<1000} {incr i} {
  set n [expr {int(rand()*8)+1}]
  set tnset [lrange [scramble {1 2 3 4 5 6 7 8 9}] 0 $n]
  set sql {}
  set j 0
  foreach tn $tnset {
    incr j
    set want_many 0
    if {[string length $sql]>0} {
      set p [expr {rand()}]
      if {$p<0.25} {
        append sql "UNION\n"
      } elseif {$p<0.5} {
        append sql "UNION ALL\n"
      } elseif {$p<0.75 || $j>2} {
        append sql "EXCEPT\n"
        set want_many 1
      } else {
        append sql "INTERSECT\n"
        incr j -1
        set want_many 1
      }
    }
    set cn [string index abcde [expr {int(rand()*5)}]]$tn
    append sql "  SELECT $cn FROM t$tn\n"
    if {$want_many} {
      set w [few_row_where $tn]
      set op     "\n           OR "
      append sql "   WHERE NOT ([join $w $op])\n"
    } else {
      set w [few_row_where $tn]
      set op "\n      OR "
      append sql "   WHERE [join $w $op]\n"
    }
  }
  puts "query T valuesort\n$sql" 
}
