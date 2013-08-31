for /R ..\test %%i in (*.test) do sqllogictest -odbc "DSN=slt.mssql;UID=slt;PWD=slt;" -verify %%i
