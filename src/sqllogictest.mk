#!/usr/make
#
# Makefile for SQLITE
#
# This particular makefile is designed to build the amalgamation
# for use with sqllogictest.
#
# To regenerate the sqlite3.c and sqlite3.h files used by sqllogictest,
# edit this file as appropriate for the build system and type:
#
#      make -f sqllogictest.mk sqlite3.c
#
# This makefile is only intended to build the amalgamation.  A separate
# makefile is used to build the sqllogictest binary.
#

#### The toplevel directory of the SQLite source tree.  This is the directory
#    that contains this "Makefile.in" and the "configure.in" script.
#
TOP = ../../sqlite

#### C Compiler and options for use in building executables that
#    will run on the platform that is doing the build.  This is the
#    C compiler used to build lemon and mkkeywordhash.
#
BCC = gcc -g
#BCC = /opt/ancic/bin/c89 -0

#### The OMIT options must be included in the build of the amalgamation.
#    The amalgamation contains generated code, and that code depends on
#    which features have been omitted.  So the omit options here must
#    match the omit options in the main sqllogictest makefile.
#
OPTS += -DSQLITE_THREADSAFE=0

OPTS += -DSQLITE_OMIT_ALTERTABLE
OPTS += -DSQLITE_OMIT_ANALYZE
OPTS += -DSQLITE_OMIT_ATTACH
OPTS += -DSQLITE_OMIT_AUTHORIZATION
OPTS += -DSQLITE_OMIT_AUTOINCREMENT
OPTS += -DSQLITE_OMIT_AUTOVACUUM
#OPTS += -DSQLITE_OMIT_BUILTIN_TEST
OPTS += -DSQLITE_OMIT_COMPLETE
#OPTS += -DSQLITE_OMIT_CONFLICT_CLAUSE
OPTS += -DSQLITE_OMIT_DATETIME_FUNCS
OPTS += -DSQLITE_OMIT_GET_TABLE
OPTS += -DSQLITE_OMIT_INCRBLOB
OPTS += -DSQLITE_OMIT_LOAD_EXTENSION
OPTS += -DSQLITE_OMIT_MEMORYDB
#OPTS += -DSQLITE_OMIT_PRAGMA
#OPTS += -DSQLITE_OMIT_REINDEX
OPTS += -DSQLITE_OMIT_SHARED_CACHE
OPTS += -DSQLITE_OMIT_TCL_VARIABLE
OPTS += -DSQLITE_OMIT_TRACE
OPTS += -DSQLITE_OMIT_TRACE
OPTS += -DSQLITE_OMIT_UTF16
OPTS += -DSQLITE_OMIT_VACUUM
OPTS += -DSQLITE_OMIT_VIRTUALTABLE

#### AWK  (Needed by Solaris systems)
#
NAWK = awk

################################# STOP HERE ################################
#
# The remainder of this file is legacy.  Nothing below this point is needed
# in order to build the amalgamation.  You can safely ignore everything
# below this line.
#

#### The suffix to add to executable files.  ".exe" for windows.
#    Nothing for unix.
#
#EXE = .exe
EXE =

#### C Compile and options for use in building executables that 
#    will run on the target platform.  This is usually the same
#    as BCC, unless you are cross-compiling.
#
#TCC = gcc -O6
TCC = gcc -g -rdynamic -O0 -Wall -fstrict-aliasing
#TCC = gcc -g -O0 -Wall -fprofile-arcs -ftest-coverage

#### Tools used to build a static library.
#
AR = ar cr
RANLIB = ranlib

#### Extra compiler options needed for programs that use the TCL library.
#
TCL_FLAGS =

#### Linker options needed to link against the TCL library.
#
LIBTCL = -ltcl8.5 -lm -ldl

#### Compiler options needed for programs that use the readline() library.
#
READLINE_FLAGS =
#READLINE_FLAGS = -DHAVE_READLINE=1 -I/usr/include/readline

#### Linker options needed by programs using readline() must link against.
#
#LIBREADLINE = -ldl -lpthread
#LIBREADLINE = -static -lreadline -ltermcap

#### Math library
#
MATHLIB = -lm
# MATHLIB =

# You should not have to change anything below this line
###############################################################################
include $(TOP)/main.mk
