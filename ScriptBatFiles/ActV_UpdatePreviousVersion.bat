@echo off 
setlocal enableextensions disabledelayedexpansion

set "search0=^>7.1.5"
set "replace0=^>6.7.15"


set "search1="5.1.1"
set "replace1="2.1.6"

set "search2="7.0.9"
set "replace2="6.0.19"

set "search3="7.0.2"
set "replace3="6.0.1"

set "search4="2.1.0"
set "replace4="2.1.0"

set "search5="4.0.0"
set "replace5="3.0.1"

set "search6="7.0.9"
set "replace6="6.0.19"

set "search7="7.0.4"
set "replace7="6.0.8"

set "search8="7.0.0"
set "replace8="6.0.2"

set "search9="7.0.9"
set "replace9="6.0.19"

set "search10="2.0.0"
set "replace10="2.0.0"


set "textFile0=..\EFCore.BulkExtensions\EFCore.BulkExtensions.csproj"
set "textFile1=..\EFCore.BulkExtensions.PerProvider\EFCore.BulkExtensions.MySql\EFCore.BulkExtensions.MySql.csproj"
set "textFile2=..\EFCore.BulkExtensions.PerProvider\EFCore.BulkExtensions.PostgreSql\EFCore.BulkExtensions.PostgreSql.csproj"
set "textFile3=..\EFCore.BulkExtensions.PerProvider\EFCore.BulkExtensions.Sqlite\EFCore.BulkExtensions.Sqlite.csproj"
set "textFile4=..\EFCore.BulkExtensions.PerProvider\EFCore.BulkExtensions.SqlServer\EFCore.BulkExtensions.SqlServer.csproj"


for /f "delims=" %%i in ('type "%textFile0%" ^& break ^> "%textFile0%" ') do (
	set "line=%%i"
	setlocal enabledelayedexpansion
	>>"%textFile0%" echo(!line:%search0%=%replace0%!
	endlocal
)
for /f "delims=" %%i in ('type "%textFile0%" ^& break ^> "%textFile0%" ') do (
	set "line=%%i"
	setlocal enabledelayedexpansion
	>>"%textFile0%" echo(!line:%search1%=%replace1%!
	endlocal
)
for /f "delims=" %%i in ('type "%textFile0%" ^& break ^> "%textFile0%" ') do (
	set "line=%%i"
	setlocal enabledelayedexpansion
	>>"%textFile0%" echo(!line:%search2%=%replace2%!
	endlocal
)
for /f "delims=" %%i in ('type "%textFile0%" ^& break ^> "%textFile0%" ') do (
	set "line=%%i"
	setlocal enabledelayedexpansion
	>>"%textFile0%" echo(!line:%search3%=%replace3%!
	endlocal
)
for /f "delims=" %%i in ('type "%textFile0%" ^& break ^> "%textFile0%" ') do (
	set "line=%%i"
	setlocal enabledelayedexpansion
	>>"%textFile0%" echo(!line:%search4%=%replace4%!
	endlocal
)
for /f "delims=" %%i in ('type "%textFile0%" ^& break ^> "%textFile0%" ') do (
	set "line=%%i"
	setlocal enabledelayedexpansion
	>>"%textFile0%" echo(!line:%search5%=%replace5%!
	endlocal
)
for /f "delims=" %%i in ('type "%textFile0%" ^& break ^> "%textFile0%" ') do (
	set "line=%%i"
	setlocal enabledelayedexpansion
	>>"%textFile0%" echo(!line:%search6%=%replace6%!
	endlocal
)
for /f "delims=" %%i in ('type "%textFile0%" ^& break ^> "%textFile0%" ') do (
	set "line=%%i"
	setlocal enabledelayedexpansion
	>>"%textFile0%" echo(!line:%search7%=%replace7%!
	endlocal
)
for /f "delims=" %%i in ('type "%textFile0%" ^& break ^> "%textFile0%" ') do (
	set "line=%%i"
	setlocal enabledelayedexpansion
	>>"%textFile0%" echo(!line:%search8%=%replace8%!
	endlocal
)
for /f "delims=" %%i in ('type "%textFile0%" ^& break ^> "%textFile0%" ') do (
	set "line=%%i"
	setlocal enabledelayedexpansion
	>>"%textFile0%" echo(!line:%search9%=%replace9%!
	endlocal
)
for /f "delims=" %%i in ('type "%textFile0%" ^& break ^> "%textFile0%" ') do (
	set "line=%%i"
	setlocal enabledelayedexpansion
	>>"%textFile0%" echo(!line:%search10%=%replace10%!
	endlocal
)

for /f "delims=" %%i in ('type "%textFile1%" ^& break ^> "%textFile1%" ') do (
	set "line=%%i"
	setlocal enabledelayedexpansion
	>>"%textFile1%" echo(!line:%search0%=%replace0%!
	endlocal
)
for /f "delims=" %%i in ('type "%textFile1%" ^& break ^> "%textFile1%" ') do (
	set "line=%%i"
	setlocal enabledelayedexpansion
	>>"%textFile1%" echo(!line:%search1%=%replace1%!
	endlocal
)
for /f "delims=" %%i in ('type "%textFile1%" ^& break ^> "%textFile1%" ') do (
	set "line=%%i"
	setlocal enabledelayedexpansion
	>>"%textFile1%" echo(!line:%search2%=%replace2%!
	endlocal
)
for /f "delims=" %%i in ('type "%textFile1%" ^& break ^> "%textFile1%" ') do (
	set "line=%%i"
	setlocal enabledelayedexpansion
	>>"%textFile1%" echo(!line:%search3%=%replace3%!
	endlocal
)
for /f "delims=" %%i in ('type "%textFile1%" ^& break ^> "%textFile1%" ') do (
	set "line=%%i"
	setlocal enabledelayedexpansion
	>>"%textFile1%" echo(!line:%search4%=%replace4%!
	endlocal
)
for /f "delims=" %%i in ('type "%textFile1%" ^& break ^> "%textFile1%" ') do (
	set "line=%%i"
	setlocal enabledelayedexpansion
	>>"%textFile1%" echo(!line:%search5%=%replace5%!
	endlocal
)
for /f "delims=" %%i in ('type "%textFile1%" ^& break ^> "%textFile1%" ') do (
	set "line=%%i"
	setlocal enabledelayedexpansion
	>>"%textFile1%" echo(!line:%search6%=%replace6%!
	endlocal
)
for /f "delims=" %%i in ('type "%textFile1%" ^& break ^> "%textFile1%" ') do (
	set "line=%%i"
	setlocal enabledelayedexpansion
	>>"%textFile1%" echo(!line:%search7%=%replace7%!
	endlocal
)
for /f "delims=" %%i in ('type "%textFile1%" ^& break ^> "%textFile1%" ') do (
	set "line=%%i"
	setlocal enabledelayedexpansion
	>>"%textFile1%" echo(!line:%search8%=%replace8%!
	endlocal
)
for /f "delims=" %%i in ('type "%textFile1%" ^& break ^> "%textFile1%" ') do (
	set "line=%%i"
	setlocal enabledelayedexpansion
	>>"%textFile1%" echo(!line:%search9%=%replace9%!
	endlocal
)
for /f "delims=" %%i in ('type "%textFile1%" ^& break ^> "%textFile1%" ') do (
	set "line=%%i"
	setlocal enabledelayedexpansion
	>>"%textFile1%" echo(!line:%search10%=%replace10%!
	endlocal
)


for /f "delims=" %%i in ('type "%textFile2%" ^& break ^> "%textFile2%" ') do (
	set "line=%%i"
	setlocal enabledelayedexpansion
	>>"%textFile2%" echo(!line:%search0%=%replace0%!
	endlocal
)
for /f "delims=" %%i in ('type "%textFile2%" ^& break ^> "%textFile2%" ') do (
	set "line=%%i"
	setlocal enabledelayedexpansion
	>>"%textFile2%" echo(!line:%search1%=%replace1%!
	endlocal
)
for /f "delims=" %%i in ('type "%textFile2%" ^& break ^> "%textFile2%" ') do (
	set "line=%%i"
	setlocal enabledelayedexpansion
	>>"%textFile2%" echo(!line:%search2%=%replace2%!
	endlocal
)
for /f "delims=" %%i in ('type "%textFile2%" ^& break ^> "%textFile2%" ') do (
	set "line=%%i"
	setlocal enabledelayedexpansion
	>>"%textFile2%" echo(!line:%search3%=%replace3%!
	endlocal
)
for /f "delims=" %%i in ('type "%textFile2%" ^& break ^> "%textFile2%" ') do (
	set "line=%%i"
	setlocal enabledelayedexpansion
	>>"%textFile2%" echo(!line:%search4%=%replace4%!
	endlocal
)
for /f "delims=" %%i in ('type "%textFile2%" ^& break ^> "%textFile2%" ') do (
	set "line=%%i"
	setlocal enabledelayedexpansion
	>>"%textFile2%" echo(!line:%search5%=%replace5%!
	endlocal
)
for /f "delims=" %%i in ('type "%textFile2%" ^& break ^> "%textFile2%" ') do (
	set "line=%%i"
	setlocal enabledelayedexpansion
	>>"%textFile2%" echo(!line:%search6%=%replace6%!
	endlocal
)
for /f "delims=" %%i in ('type "%textFile2%" ^& break ^> "%textFile2%" ') do (
	set "line=%%i"
	setlocal enabledelayedexpansion
	>>"%textFile2%" echo(!line:%search7%=%replace7%!
	endlocal
)
for /f "delims=" %%i in ('type "%textFile2%" ^& break ^> "%textFile2%" ') do (
	set "line=%%i"
	setlocal enabledelayedexpansion
	>>"%textFile2%" echo(!line:%search8%=%replace8%!
	endlocal
)
for /f "delims=" %%i in ('type "%textFile2%" ^& break ^> "%textFile2%" ') do (
	set "line=%%i"
	setlocal enabledelayedexpansion
	>>"%textFile2%" echo(!line:%search9%=%replace9%!
	endlocal
)
for /f "delims=" %%i in ('type "%textFile2%" ^& break ^> "%textFile2%" ') do (
	set "line=%%i"
	setlocal enabledelayedexpansion
	>>"%textFile2%" echo(!line:%search10%=%replace10%!
	endlocal
)


for /f "delims=" %%i in ('type "%textFile2%" ^& break ^> "%textFile2%" ') do (
	set "line=%%i"
	setlocal enabledelayedexpansion
	>>"%textFile2%" echo(!line:%search0%=%replace0%!
	endlocal
)
for /f "delims=" %%i in ('type "%textFile2%" ^& break ^> "%textFile2%" ') do (
	set "line=%%i"
	setlocal enabledelayedexpansion
	>>"%textFile2%" echo(!line:%search1%=%replace1%!
	endlocal
)
for /f "delims=" %%i in ('type "%textFile2%" ^& break ^> "%textFile2%" ') do (
	set "line=%%i"
	setlocal enabledelayedexpansion
	>>"%textFile2%" echo(!line:%search2%=%replace2%!
	endlocal
)
for /f "delims=" %%i in ('type "%textFile2%" ^& break ^> "%textFile2%" ') do (
	set "line=%%i"
	setlocal enabledelayedexpansion
	>>"%textFile2%" echo(!line:%search3%=%replace3%!
	endlocal
)
for /f "delims=" %%i in ('type "%textFile2%" ^& break ^> "%textFile2%" ') do (
	set "line=%%i"
	setlocal enabledelayedexpansion
	>>"%textFile2%" echo(!line:%search4%=%replace4%!
	endlocal
)
for /f "delims=" %%i in ('type "%textFile2%" ^& break ^> "%textFile2%" ') do (
	set "line=%%i"
	setlocal enabledelayedexpansion
	>>"%textFile2%" echo(!line:%search5%=%replace5%!
	endlocal
)
for /f "delims=" %%i in ('type "%textFile2%" ^& break ^> "%textFile2%" ') do (
	set "line=%%i"
	setlocal enabledelayedexpansion
	>>"%textFile2%" echo(!line:%search6%=%replace6%!
	endlocal
)
for /f "delims=" %%i in ('type "%textFile2%" ^& break ^> "%textFile2%" ') do (
	set "line=%%i"
	setlocal enabledelayedexpansion
	>>"%textFile2%" echo(!line:%search7%=%replace7%!
	endlocal
)
for /f "delims=" %%i in ('type "%textFile2%" ^& break ^> "%textFile2%" ') do (
	set "line=%%i"
	setlocal enabledelayedexpansion
	>>"%textFile2%" echo(!line:%search8%=%replace8%!
	endlocal
)
for /f "delims=" %%i in ('type "%textFile2%" ^& break ^> "%textFile2%" ') do (
	set "line=%%i"
	setlocal enabledelayedexpansion
	>>"%textFile2%" echo(!line:%search9%=%replace9%!
	endlocal
)
for /f "delims=" %%i in ('type "%textFile2%" ^& break ^> "%textFile2%" ') do (
	set "line=%%i"
	setlocal enabledelayedexpansion
	>>"%textFile2%" echo(!line:%search10%=%replace10%!
	endlocal
)


for /f "delims=" %%i in ('type "%textFile3%" ^& break ^> "%textFile3%" ') do (
	set "line=%%i"
	setlocal enabledelayedexpansion
	>>"%textFile3%" echo(!line:%search0%=%replace0%!
	endlocal
)
for /f "delims=" %%i in ('type "%textFile3%" ^& break ^> "%textFile3%" ') do (
	set "line=%%i"
	setlocal enabledelayedexpansion
	>>"%textFile3%" echo(!line:%search1%=%replace1%!
	endlocal
)
for /f "delims=" %%i in ('type "%textFile3%" ^& break ^> "%textFile3%" ') do (
	set "line=%%i"
	setlocal enabledelayedexpansion
	>>"%textFile3%" echo(!line:%search2%=%replace2%!
	endlocal
)
for /f "delims=" %%i in ('type "%textFile3%" ^& break ^> "%textFile3%" ') do (
	set "line=%%i"
	setlocal enabledelayedexpansion
	>>"%textFile3%" echo(!line:%search3%=%replace3%!
	endlocal
)
for /f "delims=" %%i in ('type "%textFile3%" ^& break ^> "%textFile3%" ') do (
	set "line=%%i"
	setlocal enabledelayedexpansion
	>>"%textFile3%" echo(!line:%search4%=%replace4%!
	endlocal
)
for /f "delims=" %%i in ('type "%textFile3%" ^& break ^> "%textFile3%" ') do (
	set "line=%%i"
	setlocal enabledelayedexpansion
	>>"%textFile3%" echo(!line:%search5%=%replace5%!
	endlocal
)
for /f "delims=" %%i in ('type "%textFile3%" ^& break ^> "%textFile3%" ') do (
	set "line=%%i"
	setlocal enabledelayedexpansion
	>>"%textFile3%" echo(!line:%search6%=%replace6%!
	endlocal
)
for /f "delims=" %%i in ('type "%textFile3%" ^& break ^> "%textFile3%" ') do (
	set "line=%%i"
	setlocal enabledelayedexpansion
	>>"%textFile3%" echo(!line:%search7%=%replace7%!
	endlocal
)
for /f "delims=" %%i in ('type "%textFile3%" ^& break ^> "%textFile3%" ') do (
	set "line=%%i"
	setlocal enabledelayedexpansion
	>>"%textFile3%" echo(!line:%search8%=%replace8%!
	endlocal
)
for /f "delims=" %%i in ('type "%textFile3%" ^& break ^> "%textFile3%" ') do (
	set "line=%%i"
	setlocal enabledelayedexpansion
	>>"%textFile3%" echo(!line:%search9%=%replace9%!
	endlocal
)
for /f "delims=" %%i in ('type "%textFile3%" ^& break ^> "%textFile3%" ') do (
	set "line=%%i"
	setlocal enabledelayedexpansion
	>>"%textFile3%" echo(!line:%search10%=%replace10%!
	endlocal
)


for /f "delims=" %%i in ('type "%textFile4%" ^& break ^> "%textFile4%" ') do (
	set "line=%%i"
	setlocal enabledelayedexpansion
	>>"%textFile4%" echo(!line:%search0%=%replace0%!
	endlocal
)
for /f "delims=" %%i in ('type "%textFile4%" ^& break ^> "%textFile4%" ') do (
	set "line=%%i"
	setlocal enabledelayedexpansion
	>>"%textFile4%" echo(!line:%search1%=%replace1%!
	endlocal
)
for /f "delims=" %%i in ('type "%textFile4%" ^& break ^> "%textFile4%" ') do (
	set "line=%%i"
	setlocal enabledelayedexpansion
	>>"%textFile4%" echo(!line:%search2%=%replace2%!
	endlocal
)
for /f "delims=" %%i in ('type "%textFile4%" ^& break ^> "%textFile4%" ') do (
	set "line=%%i"
	setlocal enabledelayedexpansion
	>>"%textFile4%" echo(!line:%search3%=%replace3%!
	endlocal
)
for /f "delims=" %%i in ('type "%textFile4%" ^& break ^> "%textFile4%" ') do (
	set "line=%%i"
	setlocal enabledelayedexpansion
	>>"%textFile4%" echo(!line:%search4%=%replace4%!
	endlocal
)
for /f "delims=" %%i in ('type "%textFile4%" ^& break ^> "%textFile4%" ') do (
	set "line=%%i"
	setlocal enabledelayedexpansion
	>>"%textFile4%" echo(!line:%search5%=%replace5%!
	endlocal
)
for /f "delims=" %%i in ('type "%textFile4%" ^& break ^> "%textFile4%" ') do (
	set "line=%%i"
	setlocal enabledelayedexpansion
	>>"%textFile4%" echo(!line:%search6%=%replace6%!
	endlocal
)
for /f "delims=" %%i in ('type "%textFile4%" ^& break ^> "%textFile4%" ') do (
	set "line=%%i"
	setlocal enabledelayedexpansion
	>>"%textFile4%" echo(!line:%search7%=%replace7%!
	endlocal
)
for /f "delims=" %%i in ('type "%textFile4%" ^& break ^> "%textFile4%" ') do (
	set "line=%%i"
	setlocal enabledelayedexpansion
	>>"%textFile4%" echo(!line:%search8%=%replace8%!
	endlocal
)
for /f "delims=" %%i in ('type "%textFile4%" ^& break ^> "%textFile4%" ') do (
	set "line=%%i"
	setlocal enabledelayedexpansion
	>>"%textFile4%" echo(!line:%search9%=%replace9%!
	endlocal
)
for /f "delims=" %%i in ('type "%textFile4%" ^& break ^> "%textFile4%" ') do (
	set "line=%%i"
	setlocal enabledelayedexpansion
	>>"%textFile4%" echo(!line:%search10%=%replace10%!
	endlocal
)