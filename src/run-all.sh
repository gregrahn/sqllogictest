#!/bin/sh
#
# Run this script to run all test cases
#
find ../test -name '*.test' -print |
  while read i
  do
    ./sqllogictest -verify $i
    ./sqllogictest -verify -parameter optimizer=64 $i
    ./sqllogictest -verify -parameter optimizer=255 $i
  done
