@echo off 
    setlocal enableextensions disabledelayedexpansion
	
    set "search=^>7.1.5"
    set "replace=^>7.1.6"
	
	set "search2=SaveChanges sort dependecies"
    set "replace2=Minor fixes"
	
    set "textFile1=..\EFCore.BulkExtensions.PerProvider\EFCore.BulkExtensions.MySql\EFCore.BulkExtensions.MySql.csproj"
	set "textFile2=..\EFCore.BulkExtensions.PerProvider\EFCore.BulkExtensions.PostgreSql\EFCore.BulkExtensions.PostgreSql.csproj"
	set "textFile3=..\EFCore.BulkExtensions.PerProvider\EFCore.BulkExtensions.Sqlite\EFCore.BulkExtensions.Sqlite.csproj"
	set "textFile4=..\EFCore.BulkExtensions.PerProvider\EFCore.BulkExtensions.SqlServer\EFCore.BulkExtensions.SqlServer.csproj"
	
    for /f "delims=" %%i in ('type "%textFile1%" ^& break ^> "%textFile1%" ') do (
        set "line=%%i"
        setlocal enabledelayedexpansion
        >>"%textFile1%" echo(!line:%search%=%replace%!
        endlocal
    )
	for /f "delims=" %%i in ('type "%textFile1%" ^& break ^> "%textFile1%" ') do (
        set "line=%%i"
        setlocal enabledelayedexpansion
        >>"%textFile1%" echo(!line:%search2%=%replace2%!
        endlocal
    )
	
    for /f "delims=" %%i in ('type "%textFile2%" ^& break ^> "%textFile2%" ') do (
        set "line=%%i"
        setlocal enabledelayedexpansion
        >>"%textFile2%" echo(!line:%search%=%replace%!
        endlocal
    )	
    for /f "delims=" %%i in ('type "%textFile2%" ^& break ^> "%textFile2%" ') do (
        set "line=%%i"
        setlocal enabledelayedexpansion
        >>"%textFile2%" echo(!line:%search2%=%replace2%!
        endlocal
    )
	
	for /f "delims=" %%i in ('type "%textFile3%" ^& break ^> "%textFile3%" ') do (
        set "line=%%i"
        setlocal enabledelayedexpansion
        >>"%textFile3%" echo(!line:%search%=%replace%!
        endlocal
    )
	for /f "delims=" %%i in ('type "%textFile3%" ^& break ^> "%textFile3%" ') do (
        set "line=%%i"
        setlocal enabledelayedexpansion
        >>"%textFile3%" echo(!line:%search2%=%replace2%!
        endlocal
    )
	
    for /f "delims=" %%i in ('type "%textFile4%" ^& break ^> "%textFile4%" ') do (
        set "line=%%i"
        setlocal enabledelayedexpansion
        >>"%textFile4%" echo(!line:%search%=%replace%!
        endlocal
    )	
    for /f "delims=" %%i in ('type "%textFile4%" ^& break ^> "%textFile4%" ') do (
        set "line=%%i"
        setlocal enabledelayedexpansion
        >>"%textFile4%" echo(!line:%search2%=%replace2%!
        endlocal
    )	