/*
** Copyright (c) 2008 D. Richard Hipp
**
** This program is free software; you can redistribute it and/or
** modify it under the terms of the GNU General Public
** License version 2 as published by the Free Software Foundation.
**
** This program is distributed in the hope that it will be useful,
** but WITHOUT ANY WARRANTY; without even the implied warranty of
** MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
** General Public License for more details.
** 
** You should have received a copy of the GNU General Public
** License along with this library; if not, write to the
** Free Software Foundation, Inc., 59 Temple Place - Suite 330,
** Boston, MA  02111-1307, USA.
**
** Author contact information:
**   drh@hwaci.com
**   http://www.hwaci.com/drh/
**
*******************************************************************************
**
** This main driver for the sqllogictest program.
*/
#include "sqllogictest.h"
#include <stdio.h>
#include <stdlib.h>


/*****************************************************************************
** Here begins the implementation of the SQLite DbEngine object.
**
** Use this interface as a model for other database engine interfaces.
*/
#include "sqlite3.h"

/*
** This routine is called to open a connection to a new, empty database.
** The zConnectStr argument is the value of the -connection command-line
** option.  This is intended to contain information on how to connect to
** the database engine.  The zConnectStr argument will be NULL if there
** is no -connection on the command-line.  In the case of SQLite, the
** zConnectStr is the name of the database file to open.
**
** An object that describes the newly opened and initialized database
** connection is returned by writing into *ppConn.
**
** This routine returns 0 on success and non-zero if there are any errors.
*/
static int sqliteConnect(
  void *NotUsed,              /* Argument from DbEngine object.  Not used */
  const char *zConnectStr,    /* Connection string */
  void **ppConn               /* Write completed connection here */
){
  sqlite3 *db;
  int rc;

  /* If the database filename is defined and the database already exists,
  ** then delete the database before we start, thus resetting it to an
  ** empty database.
  */
  if( zConnectStr ){
    unlink(zConnectStr);
  }

  /* Open a connection to the new database.
  */
  rc = sqlite3_open(zConnectStr, &db);
  if( rc!=SQLITE_OK ){
    return 1;
  }
  sqlite3_exec(db, "PRAGMA synchronous=OFF", 0, 0, 0);
  *ppConn = (void*)db;
  return 0;  
}

/*
** Evaluate the single SQL statement given in zSql.  Return 0 on success.
** return non-zero if any error occurs.
*/
static int sqliteStatement(
  void *pConn,                /* Connection created by xConnect */
  const char *zSql            /* SQL statement to evaluate */
){
  int rc;
  sqlite3 *db;

  db = (sqlite3*)pConn;
  rc = sqlite3_exec(db, zSql, 0, 0, 0)
  return rc!=SQLITE_OK;
}

/*
** Structure used to accumulate a result set.
*/
typedef struct ResAccum ResAccum;
struct ResAccum {
  char **azValue;   /* Array of pointers to values, each malloced separately */
  int nAlloc;       /* Number of slots allocated in azValue */
  int nUsed;        /* Number of slots in azValue used */
};

/*
** Append a value to a result set.  zValue is copied into memory obtained
** from malloc.  If zValue==0 then a "<NULL>" string is appended.  If
** zValue[0]==0 then an "<EMPTY-STRING>" is appended.  Control characters
** and non-printable characters are converted to "@".
*/
static void appendValue(ResAccum *p, const char *zValue){
  char *z;
  if( zValue==0 ){
    zValue = "<NULL>";
  }else if( zValue[0]==0 ){
    zValue = "<EMPTY-STRING>";
  }
  z = sqlite3_mprintf("%s", zValue);
  if( z==0 ){
    fprintf(stderr, "out of memory at %s:%d\n", __FILE__, __LINE__);
    exit(1);
  }
  if( p->nUsed>=p->nAlloc ){
    char **az;
    p->nAlloc += 200;
    az = sqlite3_realloc(p->azValue, p->nAlloc*sizeof(p->azValue[0]));
    if( az==0 ){
      fprintf(stderr, "out of memory at %s:%d\n", __FILE__, __LINE__);
      exit(1);
    }
    p->azValue = az;
  }
  p->azValue[p->nUsed++] = z;
  while( *z ){
    if( *z<' ' || *z>'~' ) *z = '@';
    z++;
  }
}

/*
** This interface runs a query and accumulates the results into an array
** of pointers to strings.  *pazResult is made to point to the resulting
** array and *pnResult is set to the number of elements in the array.
**
** NULL values in the result set should be represented by a string "<NULL>".
** Empty strings should be shown as "<EMPTY-STRING>".  Unprintable and
** control characters should be rendered as "@".
*/
static int sqliteQuery(
  void *pConn,                /* Connection created by xConnect */
  const char *zSql,           /* SQL statement to evaluate */
  const char *zType,          /* One character for each column of result */
  char ***pazResult,          /* RETURN:  Array of result values */
  int *pnResult               /* RETURN:  Number of result values */
){
  sqlite3 *db;
  sqlite3_stmt *pStmt;
  ResAccum res;
  char zBuffer[200];

  memset(&res, 0, sizeof(res));
  db = (sqlite3*)pConn;
  rc = sqlite3_prepare_v2(db, zSql, -1, &pStmt, 0);
  if( rc!=SQLITE_OK ){
    sqlite3_finalize(pStmt);
    return 1;
  }
  while( sqlite3_step(pStmt)==SQLITE_ROW ){
    int i;
    for(i=0; zType[i]; i++){
      switch( zType[i] ){
        case 'S': {
          char *zValue = sqlite3_column_text(pStmt, i);
          appendValue(&res, zValue);
          break;
        }
        case 'I': {
          int ii = sqlite3_column_int(pStmt, i);
          sqlite3_snprintf(sizeof(zBuffer), zBuffer, "%d", ii);
          appendValue(&res, zValue);
          break;
        }
        case 'R': {
          double r = sqlite3_column_double(pStmt, i);
          sqlite3_snprintf(sizeof(zBuffer), zBuffer, "%.3f", r);
          appendValue(&res, zValue);
          break;
        }
        default: {
          sqlite3_finalize(pStmt);
          fprintf(stderr, "Unknown character in type-string: %c\n", zType[i]);
          return 1;
        }
      }
    }
  }
  sqlite3_finalize(pStmt);
  *pazResult = res.azValue;
  *pnResult = res.nUsed;
}

/*
** This interface is called to free the memory that was returned
** by xQuery.
*/
static int sqliteFreeResults(
  void *pConn,                /* Connection created by xConnect */
  char **azResult,            /* The results to be freed */
  int nResult                 /* Number of rows of result */
){
  int i;
  for(i=0; i<nResult; i++){
    sqlite3_free(azResult[i]);
  }
  sqlite3_free(azResult);
}

/*
** This routine is called to close a connection previously opened
** by xConnect.
**
** This routine may or may not delete the database.  Whichever way
** it works, steps should be taken to avoid an accumulation of left-over
** database files.  If the database is deleted here, that is one approach.
** The other approach is to delete left-over databases in the xConnect
** method.  The SQLite interface takes the latter approach.
*/
static int sqliteDisconnect(
  void *pConn                 /* Connection created by xConnect */
){
  sqlite3 *db = (sqlite3*)pConn;
  sqlite3_close(db);
}

/*
** This routine registers the SQLite database engine with the main
** driver.  New database engine interfaces should have a single
** routine similar to this one.  The main() function below should be
** modified to call that routine upon startup.
*/
void registerSqlite(void){
  /*
  ** This is the object that defines the database engine interface.
  */
  static const DbEngine sqliteDbEngine = {
     "SQLite",             /* zName */
     0,                    /* pAuxData */
     sqliteConnect,        /* xConnect */
     sqliteStatement,      /* xStatement */
     sqliteQuery,          /* xQuery */
     sqliteFreeResult,     /* xFreeResult */
     sqliteDisconnect      /* xDisconnect */
  };
  sqllogictestRegisterEngine(&sqliteDbEngine);
}

/*
**************** End of the SQLite database engine interface *****************
*****************************************************************************/


/*
** An array of registered database engines
*/
static int nEngine = 0;
static const DbEngine **apEngine = 0;

/*
** Register a new database engine.
*/
void sqllogictestRegisterEngine(const DbEngine *p){
  nEngine++;
  apEngine = realloc(apEngine, nEngine*sizeof(apEngine[0]));
  if( apEngine==0 ){
    fprintf(stderr, "out or memory - line %d\n", __LINE__);
    exit(1);
  }
  apEngine[nEngine-1] = p;
}

/*
** Print a usage comment and die
*/
static void usage(const char *argv0){
  fprintf(stderr, "Usage: %s [-verify] [-engine ENGINE] [-connect STR] script",
    argv0);
  exit(1);
}


/*
** This is the main routine.  This routine runs first.  It processes
** command-line arguments then runs the test.
*/
int main(int argc, char **argv){
  int verifyMode = 0;                  /* True if in -verify mode */
  const char *zScriptFile = 0;         /* Input script filename */
  const char *zDbEngine = "SQLite";    /* Name of database engine */
  const char *zConnection = 0;         /* Connection string on DB engine */
  const DbEngine *pEngine = 0;         /* Pointer to DbEngine object */
  int i;                               /* Loop counter */
  char *zScript;                       /* Content of the script */
  long nScript;                        /* Size of the script in bytes */
  void *pConn;                         /* Connection to the database engine */
  int rc;                              /* Result code from subroutine call */
  int nErr = 0;                        /* Number of errors */
  int nResult;                         /* Number of query results */
  char **azResult;                     /* Query result vector */
  

  /* Add calls to the registration procedures for new database engine
  ** interfaces here
  */
  registerSqlite();

  /* Scan the command-line and process arguments
  */
  for(i=1; i<argc; i++){
    int n = strlen(argv[i]);
    if( strncmp(argv[i], "-verify",n)==0 ){
      verifyMode = 1;
    }else if( strncmp(argv[i], "-engine",n)==0 ){
      zDbEngine = argv[++i];
    }else if( strncmp(argv[i], "-connection",n)==0 ){
      zConnection = argv[++i];
    }else if( zScriptFile==0 ){
      zScriptFile = argv[i];
    }else{
      fprintf(stderr, "%s: unknown argument: %s\n", argv[0], argv[i]);
      usage(argv[0]);
    }
  }

  /* Check for errors and missing arguments.  Find the database engine
  ** to use for this run.
  */
  if( zScript==0 ){
    fprintf(stderr, "%s: no input script specified\n", argv[0]);
    usage(argv[0]);
  }
  for(i=0; i<nEngine; i++){
    if( strcmp(zDbEngine, apEngine[i]->zName)==0 ){
      pEngine = apEngine[i];
      break;
    }
  }
  if( pEngine==0 ){
    fprintf(stderr, "%s: unknown database engine: %s\n", argv[0], zDbEngine);
    fprintf(stderr, "Choices are:");
    for(i=0; i<nEngine; i++) fprintf(stderr, " %s", apEngine[i]->zName);
    fprintf(stderr, "\n");
    exit(1);
  }

  /*
  ** Read the entire script file content into memory
  */
  in = fopen(zScriptFile, "rb");
  if( in==0 ){
    fprintf(stderr, "%s: cannot open input script %s\n", argv[0], zScriptFile);
    exit(1);
  }
  fseek(in, 0L, SEEK_END);
  nScript = ftell(in);
  zScript = malloc( nScript+1 );
  if( zScript==0 ){
    fprintf(stderr, "%s: out of memory on %s:%d\n", argv[0], __FILE__,__LINE__);
    exit(1);
  }
  fseek(in, 0L, SEEK_SET);
  fread(zScript, 1, nScript, in);
  fclose(in);
  zScript[nScript] = 0;

  /* Open the database engine under test
  */
  rc = pEngine->xConnect(pEngine->pAuxData, zConnectStr, &pConn);
  if( rc ){
    fprintf(stderr, "%s: unable to connect to database\n", argv[0]);
    exit(1);
  }


  /* Finally, shutdown the database connection.  Return the number of errors.
  */
  rc = pEngine->xDisconnect(pConn);
  if( rc ){
    fprintf(stderr, "%s: disconnection from database failed\n", argv[0]);
  }
  return nErr; 
}
