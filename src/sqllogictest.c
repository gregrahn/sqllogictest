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
#include <ctype.h>
#include <unistd.h>
#include <string.h>

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
  rc = sqlite3_exec(db, zSql, 0, 0, 0);
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
** from malloc.  Or if zValue is NULL, then a NULL pointer is appended.
*/
static void appendValue(ResAccum *p, const char *zValue){
  char *z;
  if( zValue ){
    z = sqlite3_mprintf("%s", zValue);
    if( z==0 ){
      fprintf(stderr, "out of memory at %s:%d\n", __FILE__, __LINE__);
      exit(1);
    }
  }else{
    z = 0;
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
}

/*
** This interface runs a query and accumulates the results into an array
** of pointers to strings.  *pazResult is made to point to the resulting
** array and *pnResult is set to the number of elements in the array.
**
** NULL values in the result set should be represented by a string "<NULL>".
** Empty strings should be shown as "<EMPTY-STRING>".  Unprintable and
** control characters should be rendered as "@".
**
** Return 0 on success and 1 if there is an error.  It is not necessary
** to initialize *pazResult or *pnResult if an error occurs.
*/
static int sqliteQuery(
  void *pConn,                /* Connection created by xConnect */
  const char *zSql,           /* SQL statement to evaluate */
  const char *zType,          /* One character for each column of result */
  char ***pazResult,          /* RETURN:  Array of result values */
  int *pnResult               /* RETURN:  Number of result values */
){
  sqlite3 *db;                /* The database connection */
  sqlite3_stmt *pStmt;        /* Prepared statement */
  int rc;                     /* Result code from subroutine calls */
  ResAccum res;               /* query result accumulator */
  char zBuffer[200];          /* Buffer to render numbers */

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
      if( sqlite3_column_type(pStmt, i)==SQLITE_NULL ){
        appendValue(&res, "<NULL>");
      }else{
        switch( zType[i] ){
          case 'T': {
            const char *zValue = (const char*)sqlite3_column_text(pStmt, i);
            char *z;
            if( zValue[0]==0 ) zValue = "<EMPTY-STRING>";
            appendValue(&res, zValue);

            /* Convert non-printing and control characters to '@' */
            z = res.azValue[res.nUsed-1];
            while( *z ){
              if( *z<' ' || *z>'~' ){ *z = '@'; }
              z++;
            }
            break;
          }
          case 'I': {
            int ii = sqlite3_column_int(pStmt, i);
            sqlite3_snprintf(sizeof(zBuffer), zBuffer, "%d", ii);
            appendValue(&res, zBuffer);
            break;
          }
          case 'R': {
            double r = sqlite3_column_double(pStmt, i);
            sqlite3_snprintf(sizeof(zBuffer), zBuffer, "%.3f", r);
            appendValue(&res, zBuffer);
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
  }
  sqlite3_finalize(pStmt);
  *pazResult = res.azValue;
  *pnResult = res.nUsed;
  return 0;
}

/*
** This interface is called to free the memory that was returned
** by xQuery.
**
** It might be the case that nResult==0 or azResult==0.
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
  return 0;
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
  return 0;
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
     sqliteFreeResults,    /* xFreeResults */
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
  fprintf(stderr,
    "Usage: %s [-verify] [-engine DBENGINE] [-connection STR] script\n",
    argv0);
  exit(1);
}

/*
** A structure to keep track of the state of scanning the input script.
*/
typedef struct Script Script;
struct Script {
  char *zScript;       /* Complete text of the input script */
  int iCur;            /* Index in zScript of start of current line */
  char *zLine;         /* Pointer to start of current line */
  int len;             /* Length of current line */
  int iNext;           /* index of start of next line */
  int nLine;           /* line number for the current line */
  int iEnd;            /* Index in zScript of '\000' at end of script */
  int startLine;       /* Line number of start of current record */
  int copyFlag;        /* If true, copy lines to output as they are read */
  char azToken[3][20]; /* tokenization of a line */
};

/*
** Advance the cursor to the start of the next non-comment line of the
** script.  Make p->zLine point to the start of the line.  Make p->len
** be the length of the line.  Zero-terminate the line.  Any \r at the
** end of the line is removed.
**
** Return 1 on success.  Return 0 and no-op at end-of-file.
*/
static int nextLine(Script *p){
  int i;

  /* Loop until a non-comment line is found, or until end-of-file */
  while(1){

    /* When we reach end-of-file, return 0 */
    if( p->iNext>=p->iEnd ){
      p->iCur = p->iEnd;
      p->zLine = &p->zScript[p->iEnd];
      p->len = 0;
      return 0;
    }

    /* Advance the cursor to the next line */
    p->iCur = p->iNext;
    p->nLine++;
    p->zLine = &p->zScript[p->iCur];
    for(i=p->iCur; i<p->iEnd && p->zScript[i]!='\n'; i++){}
    p->zScript[i] = 0;
    p->len = i - p->iCur;
    p->iNext = i+1;

    /* If the current line ends in a \r then remove the \r. */
    if( p->len>0 && p->zScript[i-1]=='\r' ){
      p->len--;
      i--;
      p->zScript[i-1] = 0;
    }

    /* If the line consists of all spaces, make it an empty line */
    for(i=i-1; i>=p->iCur && isspace(p->zScript[i]); i--){}
    if( i<p->iCur ){
      p->zLine[0] = 0;
    }

    /* If the copy flag is set, write the line to standard output */
    if( p->copyFlag ){
      printf("%s\n", p->zLine);
    }

    /* If the line is not a comment line, then we are finished, so break
    ** out of the loop.  If the line is a comment, the loop will repeat in
    ** order to skip this line. */
    if( p->zLine[0]!='#' ) break;
  }
  return 1;
}

/*
** Look ahead to the next line and return TRUE if it is a blank line.
** But do not advance to the next line yet.
*/
static int nextIsBlank(Script *p){
  int i = p->iNext;
  if( i>=p->iEnd ) return 1;
  while( i<p->iEnd && isspace(p->zScript[i]) ){
    if( p->zScript[i]=='\n' ) return 1;
    i++;
  }
  return 0;
}

/*
** Advance the cursor to the start of the next record.  To do this,
** first skip over the tail section of the record in which we are
** currently located, then skip over blank lines.
**
** Return 1 on success.  Return 0 at end-of-file.
*/
static int findStartOfNextRecord(Script *p){

  /* Skip over any existing content to find a blank line */
  if( p->iCur>0 ){
    while( p->zLine[0] && p->iCur<p->iEnd ){
      nextLine(p);
    }
  }else{
    nextLine(p);
  }

  /* Skip over one or more blank lines to find the first line of the
  ** new record */
  while( p->zLine[0]==0 && p->iCur<p->iEnd ){
    nextLine(p);
  }

  /* Return 1 if we have not reached end of file. */
  return p->iCur<p->iEnd;
}

/*
** Find a single token in a string.  Return the index of the start
** of the token and the length of the token.
*/
static void findToken(const char *z, int *piStart, int *pLen){
  int i;
  int iStart;
  for(i=0; isspace(z[i]); i++){}
  *piStart = iStart = i;
  while( z[i] && !isspace(z[i]) ){ i++; }
  *pLen = i - iStart;
}

/*
** tokenize the current line in up to 3 tokens and store those values
** into p->azToken[0], p->azToken[1], and p->azToken[2].  Record the
** current line in p->startLine.
*/
static void tokenizeLine(Script *p){
  int i, j, k;
  int len, n;
  for(i=0; i<3; i++) p->azToken[i][0] = 0;
  p->startLine = p->nLine;
  for(i=j=0; j<p->len && i<3; i++){
    findToken(&p->zLine[j], &k, &len);
    j += k;
    n = len;
    if( n>=sizeof(p->azToken[0]) ){
      n = sizeof(p->azToken[0])-1;
    }
    memcpy(p->azToken[i], &p->zLine[j], n);
    p->azToken[i][n] = 0;
    j += n+1;
  }
}

/*
** The number columns in a row of the current result set
*/
static int nColumn = 0;

/*
** Comparison function for sorting the result set.
*/
static int rowCompare(const void *pA, const void *pB){
  const char **azA = (const char**)pA;
  const char **azB = (const char**)pB;
  int c = 0, i;
  for(i=0; c==0 && i<nColumn; i++){
    c = strcmp(azA[i], azB[i]);
  }
  return c;
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
  int nCmd = 0;                        /* Number of SQL statements processed */
  int nResult;                         /* Number of query results */
  char **azResult;                     /* Query result vector */
  Script sScript;                      /* Script parsing status */
  FILE *in;                            /* For reading script */
  

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
  if( zScriptFile==0 ){
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
    fprintf(stderr, "%s: cannot open for reading\n", zScriptFile);
    exit(1);
  }
  fseek(in, 0L, SEEK_END);
  nScript = ftell(in);
  zScript = malloc( nScript+1 );
  if( zScript==0 ){
    fprintf(stderr, "out of memory at %s:%d\n", __FILE__,__LINE__);
    exit(1);
  }
  fseek(in, 0L, SEEK_SET);
  fread(zScript, 1, nScript, in);
  fclose(in);
  zScript[nScript] = 0;

  /* Initialize the sScript structure so that the cursor will be pointing
  ** to the start of the first line in the file after nextLine() is called
  ** once. */
  memset(&sScript, 0, sizeof(sScript));
  sScript.zScript = zScript;
  sScript.zLine = zScript;
  sScript.iEnd = nScript;
  sScript.copyFlag = !verifyMode;

  /* Open the database engine under test
  */
  rc = pEngine->xConnect(pEngine->pAuxData, zConnection, &pConn);
  if( rc ){
    fprintf(stderr, "%s: unable to connect to database\n", argv[0]);
    exit(1);
  }

  /* Loop over all records in the file */
  while( findStartOfNextRecord(&sScript) ){

    /* Tokenizer the first line of the record.  This also records the
    ** line number of the first record in sScript.startLine */
    tokenizeLine(&sScript);

    /* Figure out the record type and do appropriate processing */
    if( strcmp(sScript.azToken[0],"statement")==0 ){
      int k = 0;

      /* Extract the SQL from second and subsequent lines of the
      ** record.  Copy the SQL into contiguous memory at the beginning
      ** of zScript - we are guaranteed to have enough space there. */
      while( nextLine(&sScript) && sScript.zLine[0] ){
        if( k>0 ) zScript[k++] = '\n';
        memmove(&zScript[k], sScript.zLine, sScript.len);
        k += sScript.len;
      }
      zScript[k] = 0;

      /* Run the statement.  Remember the results */
      rc = pEngine->xStatement(pConn, zScript);
      nCmd++;

      /* Check to see if we are expecting success or failure */
      if( strcmp(sScript.azToken[1],"ok")==0 ){
        /* do nothing if we expect success */
      }else if( strcmp(sScript.azToken[1],"error")==0 ){
        /* Invert the result if we expect failure */
        rc = !rc;
      }else{
        fprintf(stderr, "%s:%d: statement argument should be 'ok' or 'error'\n",
                zScriptFile, sScript.startLine);
        nErr++;
        rc = 0;
      }

      /* Report an error if the results do not match expectation */
      if( rc ){
        fprintf(stderr, "%s:%d: statement error\n",
                zScriptFile, sScript.startLine);
        nErr++;
      }
    }else if( strcmp(sScript.azToken[0],"query")==0 ){
      int k = 0;
      int c;

      /* Verify that the type string consists of one or more characters
      ** from the set "TIR". */
      for(k=0; (c = sScript.azToken[1][k])!=0; k++){
        if( c!='T' && c!='I' && c!='R' ){
          fprintf(stderr, "%s:%d: unknown type character '%c' in type string\n",
                  zScriptFile, sScript.startLine, c);
          nErr++;
          break;
        }
      }
      if( c!=0 ) continue;
      if( k<=0 ){
        fprintf(stderr, "%s:%d: missing type string\n",
                zScriptFile, sScript.startLine);
        nErr++;
        break;
      }

      /* Extract the SQL from second and subsequent lines of the record
      ** until the first "----" line or until end of record.
      */
      k = 0;
      while( !nextIsBlank(&sScript) && nextLine(&sScript) && sScript.zLine[0]
             && strcmp(sScript.zLine,"----")!=0 ){
        if( k>0 ) zScript[k++] = '\n';
        memmove(&zScript[k], sScript.zLine, sScript.len);
        k += sScript.len;
      }
      zScript[k] = 0;

      /* Run the query */
      nResult = 0;
      azResult = 0;
      rc = pEngine->xQuery(pConn, zScript, sScript.azToken[1],
                           &azResult, &nResult);
      nCmd++;
      if( rc ){
        fprintf(stderr, "%s:%d: query failed\n",
                zScriptFile, sScript.startLine);
        pEngine->xFreeResults(pConn, azResult, nResult);
        nErr++;
        continue;
      }

      /* Do any required sorting of query results */
      if( sScript.azToken[2][0]==0 || strcmp(sScript.azToken[2],"nosort")==0 ){
        /* Do no sorting */
      }else if( strcmp(sScript.azToken[2],"rowsort")==0 ){
        /* Row-oriented sorting */
        nColumn = strlen(sScript.azToken[1]);
        qsort(azResult, nResult/nColumn, sizeof(azResult[0])*nColumn,
              rowCompare);
      }else if( strcmp(sScript.azToken[2],"valuesort")==0 ){
        /* Sort all values independently */
        nColumn = 1;
        qsort(azResult, nResult, sizeof(azResult[0]), rowCompare);
      }else{
        fprintf(stderr, "%s:%d: unknown sort method: '%s'\n",
                zScriptFile, sScript.startLine, sScript.azToken[2]);
        nErr++;
      }

      if( verifyMode ){
        /* In verify mode, first skip over the ---- line if we are still
        ** pointing at it. */
        if( strcmp(sScript.zLine, "----")==0 ) nextLine(&sScript);

        /* Compare subsequent lines of the script against the results
        ** from the query.  Report an error if any differences are found.
        */
        for(i=0; i<nResult && sScript.zLine[0]; nextLine(&sScript), i++){
          if( strcmp(sScript.zLine, azResult[i])!=0 ){
            fprintf(stderr,"%s:%d: wrong result\n", zScriptFile,
                    sScript.nLine);
            nErr++;
            break;
          }
        }
      }else{
        /* In completion mode, first make sure we have output an ---- line.
        ** Output such a line now if we have not already done so.
        */
        if( strcmp(sScript.zLine, "----")!=0 ){
          printf("----\n");
        }

        /* Output the results obtained by running the query
        */
        for(i=0; i<nResult; i++){
          printf("%s\n", azResult[i]);
        }
        printf("\n");

        /* Skip over any existing results.  They will be ignored.
        */
        sScript.copyFlag = 0;
        while( sScript.zLine[0]!=0 && sScript.iCur<sScript.iEnd ){
          nextLine(&sScript);
        }
        sScript.copyFlag = 1;
      }

      /* Free the query results */
      pEngine->xFreeResults(pConn, azResult, nResult);
    }else{
      /* An unrecognized record type is an error */
      fprintf(stderr, "%s:%d: unknown record type: '%s'\n",
              zScriptFile, sScript.startLine, sScript.azToken[0]);
      nErr++;
    }
  }


  /* Shutdown the database connection.
  */
  rc = pEngine->xDisconnect(pConn);
  if( rc ){
    fprintf(stderr, "%s: disconnection from database failed\n", argv[0]);
    nErr++;
  }

  /* Report the number of errors and quit.
  */
  if( verifyMode || nErr ){
    printf("%s: %d errors out of %d SQL statement\n",
           zScriptFile, nErr, nCmd);
  }
  free(zScript);
  return nErr; 
}
