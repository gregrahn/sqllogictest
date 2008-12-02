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
** Here begins the implementation of the ODBC3 DbEngine object.
**
** This DbEngine expects an ODBC3 DSN named 'sqllogictest'
** and a database named 'slt' available through that DSN to exist.
** The DSN should be accessible to the current user.
** On connect, it will attempt to "DROP" all existing tables 
** from the database name 'slt' to reset it to a known status.
**
** The DSN name and DB name are controlled by the defines
** SLT_DSN and SLT_DB.
**
*/
#ifndef OMIT_ODBC  /* Omit this module if OMIT_ODBC is defined */

#ifdef WIN32
#include <windows.h>
#endif
#define SQL_NOUNICODEMAP
#include <sql.h>
#include <sqlext.h>


#define SLT_DSN "sqllogictest"
#define SLT_DB  "slt"


/* 
** Forward prototypes.
*/
static int ODBC3Statement(
  void *pConn,                /* Connection created by xConnect */
  const char *zSql            /* SQL statement to evaluate */
);

/*
** Structure used to hold handles needed for ODBC3 connection.
*/
typedef struct ODBC3_Handles ODBC3_Handles;
struct ODBC3_Handles {
  SQLHENV env;
  SQLHDBC dbc;
  SQLCHAR *zConnStr;
};

/*
** Utility function to display the details of an ODBC3 error.
*/
static void ODBC3_perror(char *fn,
                         SQLHANDLE handle,
                         SQLSMALLINT type)
{
  SQLSMALLINT i = 0;
  SQLINTEGER native;
  SQLCHAR state[ 7 ];
  SQLCHAR text[256];
  SQLSMALLINT len;
  SQLRETURN ret;

  do
  {
    ret = SQLGetDiagRec(type, 
                        handle, 
                        ++i, 
                        state, 
                        &native, 
                        text,
                        sizeof(text), 
                        &len );
    if (SQL_SUCCEEDED(ret))
    {
      fprintf(stderr,
              "%s:%s:%ld:%ld:%s\n", fn, state, (long)i, (long)native, text);
    }
  }
  while( SQL_SUCCEEDED(ret) );
}

/*
** Structure used to accumulate a result set.
*/
typedef struct ODBC3_resAccum ODBC3_resAccum;
struct ODBC3_resAccum {
  char **azValue;   /* Array of pointers to values, each malloced separately */
  int nAlloc;       /* Number of slots allocated in azValue */
  int nUsed;        /* Number of slots in azValue used */
};

/*
** Append a value to a result set.  zValue is copied into memory obtained
** from malloc.  Or if zValue is NULL, then a NULL pointer is appended.
*/
static void ODBC3_appendValue(ODBC3_resAccum *p, const char *zValue){
  char *z;
  if( zValue ){
#ifdef WIN32
    z = _strdup(zValue);
#else
    z = strdup(zValue);
#endif
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
    az = realloc(p->azValue, p->nAlloc*sizeof(p->azValue[0]));
    if( az==0 ){
      fprintf(stderr, "out of memory at %s:%d\n", __FILE__, __LINE__);
      exit(1);
    }
    p->azValue = az;
  }
  p->azValue[p->nUsed++] = z;
}

/*
** Drop all tables from the database on the current connection.
** This utility function goes to great lengths to ensure 
** only tables in the test database are dropped. 
*/
static int ODBC3_dropAllTables(ODBC3_Handles *pODBC3conn)
{
  int rc = 0;
  SQLRETURN ret;       /* ODBC API return status */
  SQLSMALLINT columns; /* number of columns in result-set */
  ODBC3_resAccum res;          /* query result accumulator */
  SQLUSMALLINT i;
  char zSql[512];
  SQLHSTMT stmt = SQL_NULL_HSTMT;

  /* zero out accumulator structure */
  memset(&res, 0, sizeof(res));

  /* Allocate a statement handle */
  ret = SQLAllocHandle(SQL_HANDLE_STMT, pODBC3conn->dbc, &stmt);
  if( !SQL_SUCCEEDED(ret) && (ret != SQL_SUCCESS_WITH_INFO) ){
    ODBC3_perror("SQLAllocHandle", pODBC3conn->dbc, SQL_HANDLE_DBC);
    return 1;
  }

  /* Retrieve a list of tables */
  /* TBD:  do we need to drop views, triggers, etc. here? */
  ret = SQLTables(stmt, NULL, 0, NULL, 0, NULL, 0, "TABLE", SQL_NTS);
  if( !SQL_SUCCEEDED(ret) && (ret != SQL_SUCCESS_WITH_INFO) ){
    ODBC3_perror("SQLTables", stmt, SQL_HANDLE_STMT);
    rc = 1;
  }
  
  if( !rc ){
    /* How many columns are there */
    SQLNumResultCols(stmt, &columns);
    if( columns != 5 ){
      /* Non-standard result set.  Could be non-standard ODBC
      ** driver, or we're looking at wrong DB.  Return an 
      ** error and force them to fix this by hand. 
      ** We don't want to accidentally delete something important. */
      fprintf(stderr, 
              "Result set of tables has wrong number of columns: %ld\n",
              (long)columns);
      rc = 1;
    }
  }

  if( !rc ){
    /* Loop through the rows in the result-set */
    do {
      ret = SQLFetch(stmt);
      if (SQL_SUCCEEDED(ret)) {
        /* Loop through the columns in the row */
        for( i=1; i<=columns; i++ ){
          SQLINTEGER indicator;
          char zBuffer[512];
          /* retrieve column data as a string */
          ret = SQLGetData(stmt, 
                           i, SQL_C_CHAR,
                           zBuffer, sizeof(zBuffer), 
                           &indicator);
          if (SQL_SUCCEEDED(ret)) {
            /* Handle null columns */
            if (indicator == SQL_NULL_DATA){
              strcpy(zBuffer, "NULL");
            } else if( *zBuffer == '\0' ) {
              strcpy(zBuffer, "(empty)");
            }
            /* add it to the result list */
            ODBC3_appendValue(&res, zBuffer);
          }
        } /* end for i */
      }
    } while (SQL_SUCCEEDED(ret) || (ret ==  SQL_SUCCESS_WITH_INFO));
  }
  
  if( stmt != SQL_NULL_HSTMT ){
    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
  }

  if( !rc ){
  
    /* Find the name of the database (defaults to SLT_DB).
    ** When looping through the tables to delete, only delete
    ** tables from that database.
    */
    char zDbName[512] = SLT_DB;
    char *pc1 = zDbName;
    char *pc2 = strstr(pODBC3conn->zConnStr, "DATABASE=");
    if( pc2 ){
      pc2 += 9;
      while( *pc2 && (*pc2!=';') ) *pc1++ = *pc2++;
      *pc1 = '\0';
    }
  
    /* for each valid table found, drop it */
    for( i=0; !rc && (i+4<res.nUsed); i+=5 ){
      if(    (0 == strcmp(res.azValue[i], zDbName)
               || 0 == strcmp(res.azValue[i], "NULL"))
          && (strlen(res.azValue[i+2])>0)
          && (0 == strcmp(res.azValue[i+3], "TABLE"))
      ){
        sprintf(zSql, "DROP TABLE %s", res.azValue[i+2]);
        rc = ODBC3Statement(pODBC3conn, zSql);
      }
    }
  }
  
  return rc;
}


/*
** This routine is called to open a connection to a new, empty database.
** The zConnectStr argument is the value of the -odbc command-line
** option.  This is intended to contain information on how to connect to
** the database engine.  The zConnectStr argument will be NULL if there
** is no -odbc on the command-line.  
**
** An object that describes the newly opened and initialized database
** connection is returned by writing into *ppConn.
**
** This routine returns 0 on success and non-zero if there are any errors.
*/
static int ODBC3Connect(
  void *NotUsed,              /* Argument from DbEngine object.  Not used */
  const char *zConnectStr,    /* Connection string */
  void **ppConn               /* Write completed connection here */
){
  int rc = 0;
  SQLRETURN ret; /* ODBC API return status */
  ODBC3_Handles *pODBC3conn = NULL;
  char szConnStrIn[512] = "";

  /* Allocate a structure to hold all of our ODBC3 handles */
  pODBC3conn = (ODBC3_Handles *)malloc(sizeof(ODBC3_Handles));
  if( !pODBC3conn ){
    fprintf(stderr, "Out of memory at %s:%d\n", __FILE__, __LINE__);
    return 1;
  }
  pODBC3conn->env = SQL_NULL_HENV;
  pODBC3conn->dbc = SQL_NULL_HDBC;
  pODBC3conn->zConnStr = NULL;

  /* Allocate an environment handle */
  ret = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &pODBC3conn->env);
  if( !SQL_SUCCEEDED(ret) ){
    ODBC3_perror("SQLAllocHandle", pODBC3conn->env, SQL_HANDLE_ENV);
    rc = 1;
  }
  
  /* We want ODBC 3 support */
  if( !rc ){
    ret = SQLSetEnvAttr(pODBC3conn->env, SQL_ATTR_ODBC_VERSION, (void *) SQL_OV_ODBC3, 0);
    if( !SQL_SUCCEEDED(ret) ){
      ODBC3_perror("SQLSetEnvAttr", pODBC3conn->env, SQL_HANDLE_ENV);
      rc = 1;
    }
  }
  
  /* Allocate a database connection (dbc) handle */
  if( !rc ){
    ret = SQLAllocHandle(SQL_HANDLE_DBC, pODBC3conn->env, &pODBC3conn->dbc);
    if( !SQL_SUCCEEDED(ret) ){
      ODBC3_perror("SQLAllocHandle", pODBC3conn->env, SQL_HANDLE_ENV);
      rc = 1;
    }
  }
  
  /* Allocate storage space for the returned connection information.
  */
  if( !rc ){
    pODBC3conn->zConnStr = (SQLCHAR *)malloc(1024 * sizeof(SQLCHAR));
    if( !pODBC3conn->zConnStr ){
      fprintf(stderr, "Out of memory at %s:%d\n", __FILE__, __LINE__);
      rc = 1;
    }
  }
  
  if( !rc ){
    SQLSMALLINT outStrLen;

    /* Build the connection string.   If a DSN or DATABASE
    ** is not specified, use the defaults.
    */
    if( !zConnectStr || !strstr(zConnectStr, "DSN=") ){
      strcat(szConnStrIn, "DSN=" SLT_DSN ";");
    }
    if( !zConnectStr || !strstr(zConnectStr, "DATABASE=") ){
      strcat(szConnStrIn, "DATABASE=" SLT_DB ";");
    }
    if( zConnectStr ){
      strcat(szConnStrIn, zConnectStr);
    }

    /* Open a connection to the new database.
    */
    /* TBD: should we use SQLConnect() here? */
    ret = SQLDriverConnect(pODBC3conn->dbc, 
                           NULL, 
                           (SQLCHAR *)szConnStrIn, 
                           SQL_NTS,
                           pODBC3conn->zConnStr, 
                           1024 * sizeof(SQLCHAR), 
                           &outStrLen,
                           SQL_DRIVER_COMPLETE);
    if( !SQL_SUCCEEDED(ret) && (ret != SQL_SUCCESS_WITH_INFO) ){
      ODBC3_perror("SQLDriverConnect", pODBC3conn->dbc, SQL_HANDLE_DBC);
      rc = 1;
    }
  }
  
  /* TBD:  should we call CREATE DATABASE 'slt'?
  ** This would require removing any DATABASE name from 
  ** the connection string, connecting to the DSN only,
  ** creating the db, then reconnecting the DSN with the
  ** the database name specified. */

  /* Loop over all tables, etc. available in the database and drop them,
  ** thus resetting it to an empty database.  */
  if( !rc ){
    rc = ODBC3_dropAllTables(pODBC3conn);
  }
  
  /* TBD: is there a way to specify synchronous=OFF or equivalent */

  /* TBD: should we free up anything allocated on error? */
  
  /* store connection info */
  *ppConn = (void*)pODBC3conn;

  return rc;  
}

/*
** Evaluate the single SQL statement given in zSql.  Return 0 on success.
** return non-zero if any error occurs.
*/
static int ODBC3Statement(
  void *pConn,                /* Connection created by xConnect */
  const char *zSql            /* SQL statement to evaluate */
){
  int rc = 0;
  SQLRETURN ret; /* ODBC API return status */
  ODBC3_Handles *pODBC3conn = pConn;
  SQLHSTMT stmt = SQL_NULL_HSTMT;

  /* Allocate a statement handle */
  ret = SQLAllocHandle(SQL_HANDLE_STMT, pODBC3conn->dbc, &stmt);
  if( !SQL_SUCCEEDED(ret) && (ret != SQL_SUCCESS_WITH_INFO) ){
    ODBC3_perror("SQLAllocHandle", pODBC3conn->dbc, SQL_HANDLE_DBC);
    return 1;
  }

  ret = SQLExecDirect(stmt, (SQLCHAR *)zSql, SQL_NTS);
  if( !SQL_SUCCEEDED(ret) && (ret != SQL_SUCCESS_WITH_INFO) ){
    ODBC3_perror("SQLExecDirect", stmt, SQL_HANDLE_STMT);
    rc = 1;
  }

  if( stmt != SQL_NULL_HSTMT ){
    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
  }

 return rc;
}

/*
** This interface runs a query and accumulates the results into an array
** of pointers to strings.  *pazResult is made to point to the resulting
** array and *pnResult is set to the number of elements in the array.
**
** NULL values in the result set should be represented by a string "NULL".
** Empty strings should be shown as "(empty)".  Unprintable and
** control characters should be rendered as "@".
**
** Return 0 on success and 1 if there is an error.  It is not necessary
** to initialize *pazResult or *pnResult if an error occurs.
*/
static int ODBC3Query(
  void *pConn,                /* Connection created by xConnect */
  const char *zSql,           /* SQL statement to evaluate */
  const char *zType,          /* One character for each column of result */
  char ***pazResult,          /* RETURN:  Array of result values */
  int *pnResult               /* RETURN:  Number of result values */
){
  int rc = 0;
  SQLRETURN ret;              /* ODBC API return status */
  ODBC3_Handles *pODBC3conn = pConn;
  ODBC3_resAccum res;         /* query result accumulator */
  char zBuffer[512];          /* Buffer to render numbers */
  SQLSMALLINT columns;        /* number of columns in result-set */
  SQLHSTMT stmt = SQL_NULL_HSTMT;
  SQLUSMALLINT i;

  /* zero out accumulator structure */
  memset(&res, 0, sizeof(res));

  /* Allocate a statement handle */
  ret = SQLAllocHandle(SQL_HANDLE_STMT, pODBC3conn->dbc, &stmt);
  if( !SQL_SUCCEEDED(ret) && (ret != SQL_SUCCESS_WITH_INFO) ){
    ODBC3_perror("SQLAllocHandle", pODBC3conn->dbc, SQL_HANDLE_DBC);
    return 1;
  }

  ret = SQLExecDirect(stmt, (SQLCHAR *)zSql, SQL_NTS);
  if( !SQL_SUCCEEDED(ret) && (ret != SQL_SUCCESS_WITH_INFO) ){
    ODBC3_perror("SQLExecDirect", stmt, SQL_HANDLE_STMT);
    rc = 1;
  }

  if( !rc ){
    /* How many columns are there */
    ret = SQLNumResultCols(stmt, &columns);
    if( !SQL_SUCCEEDED(ret) && (ret != SQL_SUCCESS_WITH_INFO) ){
      ODBC3_perror("SQLNumResultCols", stmt, SQL_HANDLE_STMT);
      rc = 1;
    }
    if( strlen(zType)!=columns ){
      fprintf(stderr, "Wrong number of result columns: Expected %d but got %d\n",
              (int)strlen(zType), (int)columns);
      rc = 1;
    }
  }

  if( !rc ){
    /* Loop through the rows in the result-set */
    do {
      ret = SQLFetch(stmt);
      if( SQL_SUCCEEDED(ret) ){
        /* Loop through the columns */
        for(i = 1; !rc && (i <= columns); i++){
          SQLINTEGER indicator = 0;
          switch( zType[i-1] ){
            case 'T': {
              /* retrieve column data as a string */
              ret = SQLGetData(stmt, 
                               i, 
                               SQL_C_CHAR,
                               zBuffer, 
                               sizeof(zBuffer), 
                               &indicator);
              if( SQL_SUCCEEDED(ret) ){
                char *z;
                if( indicator == SQL_NULL_DATA ) strcpy(zBuffer, "NULL");
                if( zBuffer[0]==0 ) strcpy(zBuffer, "(empty)");
                ODBC3_appendValue(&res, zBuffer);
                /* Convert non-printing and control characters to '@' */
                z = res.azValue[res.nUsed-1];
                while( *z ){
                  if( *z<' ' || *z>'~' ){ *z = '@'; }
                  z++;
                }
              }
              break;
            }
            case 'I': {
              long int li = 0L;
              SQLGetData(stmt, 
                         i, 
                         SQL_C_SLONG,
                         &li, 
                         sizeof(li), 
                         &indicator);
              if( indicator == SQL_NULL_DATA ){
                strcpy(zBuffer, "NULL");
              }else{
                sprintf(zBuffer, "%ld", li);
              }
              ODBC3_appendValue(&res, zBuffer);
              break;
            }
            case 'R': {
              double r = 0.0f;
              SQLGetData(stmt, 
                         i, 
                         SQL_C_DOUBLE,
                         &r, 
                         sizeof(r), 
                         &indicator);
              if( indicator == SQL_NULL_DATA ){
                strcpy(zBuffer, "NULL");
              }else{
                sprintf(zBuffer, "%.3f", r);
              }
              ODBC3_appendValue(&res, zBuffer);
              break;
            }
            default: {
              fprintf(stderr, "Unknown character in type-string: %c\n", zType[i-1]);
              rc = 1;
            }
          } /* end switch */
        } /* end for i */
      }
    } while( !rc && SQL_SUCCEEDED(ret) );
  }
  
  if( stmt != SQL_NULL_HSTMT ){
    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
  }

  *pazResult = res.azValue;
  *pnResult = res.nUsed;

  return rc;
}


/*
** This interface is called to free the memory that was returned
** by xQuery.
**
** It might be the case that nResult==0 or azResult==0.
*/
static int ODBC3FreeResults(
  void *pConn,                /* Connection created by xConnect */
  char **azResult,            /* The results to be freed */
  int nResult                 /* Number of rows of result */
){
  int i;
  for(i=0; i<nResult; i++){
    free(azResult[i]);
  }
  free(azResult);
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
** method.  The ODBC3 interface takes the latter approach.
*/
static int ODBC3Disconnect(
  void *pConn                 /* Connection created by xConnect */
){
  int rc = 0;
  SQLRETURN ret; /* ODBC API return status */
  ODBC3_Handles *pODBC3conn = pConn;
  
  if ( !pODBC3conn ){
    fprintf(stderr, "Invalid ODBC3 connection object\n");
    return 1;
  }

  if( pODBC3conn->dbc != SQL_NULL_HDBC ){
    ret = SQLDisconnect(pODBC3conn->dbc);   /* disconnect from driver */
    if( !SQL_SUCCEEDED(ret) && (ret != SQL_SUCCESS_WITH_INFO) ){
      ODBC3_perror("SQLDisconnect", pODBC3conn->dbc, SQL_HANDLE_DBC);
      rc = 1;
    }
  }

  if( pODBC3conn->dbc != SQL_NULL_HDBC ){
    SQLFreeHandle(SQL_HANDLE_DBC, pODBC3conn->dbc);
  }
  if( pODBC3conn->env != SQL_NULL_HENV ){
    SQLFreeHandle(SQL_HANDLE_ENV, pODBC3conn->env);
  }

  if( pODBC3conn->zConnStr ){
    free(pODBC3conn->zConnStr);
  }

  pODBC3conn->env = SQL_NULL_HENV;
  pODBC3conn->dbc = SQL_NULL_HDBC;
  pODBC3conn->zConnStr = NULL;
  
  free(pODBC3conn);
  return rc;
}

/*
** This routine registers the ODBC3 database engine with the main
** driver.  New database engine interfaces should have a single
** routine similar to this one.  The main() function below should be
** modified to call that routine upon startup.
*/
void registerODBC3(void){
  /*
  ** This is the object that defines the database engine interface.
  */
  static const DbEngine ODBC3DbEngine = {
     "ODBC3",             /* zName */
     0,                    /* pAuxData */
     ODBC3Connect,        /* xConnect */
     ODBC3Statement,      /* xStatement */
     ODBC3Query,          /* xQuery */
     ODBC3FreeResults,    /* xFreeResults */
     ODBC3Disconnect      /* xDisconnect */
  };
  sqllogictestRegisterEngine(&ODBC3DbEngine);
}

/*
**************** End of the ODBC3 database engine interface *****************
*****************************************************************************/
#endif /* OMIT_ODBC */
