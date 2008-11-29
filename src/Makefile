#!/usr/bin/make
#
#### The suffix to add to executable files.  ".exe" for windows.
#    Nothing for unix.
#
E =

#### C Compile and options for use in building executables that 
#    will run on the target platform.
#
CC = gcc -g -Wall

#### Extra arguments for linking the finished binary. 
#
LIB = $(LDFLAGS)

# You should not need to change anything below this line
###############################################################################
#
OBJ = \
  sqlite3.o

sqllogictest$(E):	sqllogictest.c sqllogictest.h $(OBJ)
	$(CC) -o sqllogictest$(E) sqllogictest.c $(OBJ)

sqlite3.o:	sqlite3.c sqlite3.h
	$(CC) -c sqlite3.c -DSQLITE_THREADSAFE=0 -DSQLITE_OMIT_LOAD_EXTENSION

clean:
	rm -f $(OBJ)
