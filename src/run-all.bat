for /R ..\test %%i in (*.test) do sqllogictest -verify %%i
for /R ..\test %%i in (*.test) do sqllogictest -verify -parameter optimizer=64 %%i
for /R ..\test %%i in (*.test) do sqllogictest -verify -parameter optimizer=255 %%i
