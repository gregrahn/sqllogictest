for /R ..\test %%i in (*.test) do sqllogictest -odbc "UID=slt;" -verify %%i
