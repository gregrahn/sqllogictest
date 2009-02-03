#!/bin/sh
#
# Run this script to run all test cases
#
find ../test -name '*.test' -print |
  while read i
  do
    ./sqllogictest -verify $i
  done
