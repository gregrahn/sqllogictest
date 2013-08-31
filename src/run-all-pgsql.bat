for /R ..\test %%i in (*.test) do sqllogictest -odbc "DSN=slt.pgsql;UID=slt;PWD=slt;" -verify %%i
