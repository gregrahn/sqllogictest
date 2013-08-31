for /R ..\test %%i in (*.test) do sqllogictest -odbc "DSN=slt.psql;UID=slt;PWD=slt;" -verify %%i
