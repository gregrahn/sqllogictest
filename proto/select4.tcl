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
package require sqlite3
sqlite3 db :memory:

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
  set sql [subst {CREATE TABLE t${tn}(
  a$tn INTEGER,
  b$tn INTEGER,
  c$tn INTEGER,
  d$tn INTEGER,
  e$tn INTEGER,
  x$tn VARCHAR(30)
)}]
  puts $sql
  db eval $sql
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
  db eval $sql
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
  set sql \
    "CREATE INDEX t1i$i ON t1([join [lrange {a1 b1 c1 d1 e1 x1} $i end] ,])"
  puts "$sql\n"
  db eval $sql
}
set fast(t1) {a1} ;# {a1 b1} {a1 b1 c1} {a1 b1 c1 d1} {a1 b1 c1 d3 e1}}
# t2 gets a separate index on each column
#
foreach c {a2 b2 c2 d2 e2} {
  puts "statement ok"
  set sql "CREATE INDEX t2$c ON t2($c)"
  puts "$sql\n"
  db eval $sql
}
set fast(t2) {a2 b2 c2 d2 e2}
# t3 through t7 get a single index on one column
#
for {set i 0} {$i<5} {incr i} {
  set tn [expr {$i+3}]
  set cn [string index {abcde} $i]$tn
  puts "statement ok"
  set sql "CREATE INDEX t$tn$cn ON t${tn}($cn)"
  puts "$sql\n"
  db eval $sql
  set fast(t$tn) $cn
}
# t8 gets a single reverse-order index.
#
puts "statement ok"
set sql "CREATE INDEX t8all ON t8(e8 DESC, d8 ASC, c8 DESC, b8 ASC, a8 DESC)"
db eval $sql
puts "$sql\n"
set fast(t8) {e8} ;# {e8 d8} {e8 d8 c8} {e8 d8 c8 b8} {e8 d8 c8 b8 a8}}
# t9 does not get an index.
#
set fast(t9) {}

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

# Return a single WHERE clause term that restricts table $tn to just
# a handful or rows.  Try to use an index.
#
# Feel free to use columns from any tables in $tnset as right-hand side.
#
proc oneortwo_where_term {tn tnset} {
  global cdata fast
  set n [llength $fast(t$tn)]
  if {$n==0} {
    set cn [string index abcde [expr {int(rand()*5)}]]$tn
  } else {
    set cn [lindex [scramble $fast(t$tn)] 0]
  }
  set ntnset [llength $tnset]
  set rhs [lindex $tnset [expr {int(rand()*$ntnset)}]]
  if {$rhs==$tn} {set rhs 0}
  set p [expr {rand()}]
  if {$p<0.3333} {
    set m [expr {int(rand()*6)+2}]
    set term "$cn in ([join [lrange [scramble $::cdata($cn)] 0 $m] ,])"
  } elseif {$p<0.6666 && $rhs>0} {
    set rcn [string index abcde [expr {int(rand()*5)}]]$rhs
    set term "$cn=$rcn"
  } else {
    if {$p<0.9} {
      set m 1
      set conn ""
    } else {
      set m [expr {int(rand()*3)+2}]
      set conn (
    }
    set term {}
    for {set k 0} {$k<$m} {incr k} {
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
    if {$m>1} {
      append term )
    }
  }
  return $term
}

# Do some joins
#
for {set i 0} {$i<500} {incr i} {
  while {1} {
    set n [expr {int(rand()*7)+1}]
    set tnset [lrange [scramble {1 2 3 4 5 6 7 8 9}] 0 $n]
    set rset {}
    set typestr {}
    set w {}
    set tnnames {}
    foreach tn $tnset {
      lappend tnnames t$tn
      set cn [string index abcdex [expr {int(rand()*6)}]]
      if {$cn=="x"} {
        append typestr T
        append cn $tn
      } else {
        append typestr I
        append cn $tn
        set p [expr {rand()}]
        if {$p<0.2} {
          append cn "*[expr {int(rand()*1000)+1}]"
        } elseif {$p<0.4} {
          append cn "+[expr {int(rand()*1000)+1}]"
        }
        if {rand()<0.3} {
          set tn2 [lindex $tnset [expr {int(rand()*$n)}]]
          set cn2 [string index abcde [expr {int(rand()*5)}]]
          append cn +$cn2$tn2
        }
      }
      lappend rset $cn
      lappend w [oneortwo_where_term $tn $tnset]
    }
    set sql "SELECT count(*) FROM [join $tnnames ,] WHERE [join $w { AND }]"
    append sql " LIMIT [expr {(100000+$n*2)/$n}]"
    set njoin [db eval $sql]
    if {$njoin<1 || $njoin*$n>100000} {
      puts -nonewline stderr .
      continue
    }
    puts stderr "join-$i is [expr {$n+1}]-way has $njoin rows"
  
    unset -nocomplain seen
    for {set j 0} {$j<4} {incr j} {
      set sql "SELECT [join $rset {, }]\n"
      append sql "  FROM [join [scramble $tnnames] {, }]\n"
      append sql " WHERE [join [scramble $w] "\n   AND "]"
      if {[info exists seen($sql)]} continue
      set seen($sql) 1
      puts "query $typestr rowsort join$i"
      puts $sql
      puts ""
    }
    break
  }
}
