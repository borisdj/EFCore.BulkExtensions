:: Bat procedures that copies Source files from main project to each per provider project, skippes main .csproj file and other provjder adapters
:: When used lines for linking files via Include/Remove should first be removed from .csproject per provider

robocopy "EFCore.BulkExtensions" "EFCore.BulkExtensions.MySql " /E /XF "*.csproj" /XD "bin" "obj" "SqlAdapters"
robocopy "EFCore.BulkExtensions\SqlAdapters" "EFCore.BulkExtensions.MySql\SqlAdapters" *.*
robocopy "EFCore.BulkExtensions\SqlAdapters\MySql " "EFCore.BulkExtensions.MySql\SqlAdapters\MySql " /E

robocopy "EFCore.BulkExtensions" "EFCore.BulkExtensions.PostgreSql" /E /XF "EFCore.BulkExtensions.csproj" /XD "bin" "obj" "SqlAdapters"
robocopy "EFCore.BulkExtensions\SqlAdapters" "EFCore.BulkExtensions.PostgreSql\SqlAdapters" *.*
robocopy "EFCore.BulkExtensions\SqlAdapters\PostgreSql" "EFCore.BulkExtensions.PostgreSql\SqlAdapters\PostgreSql" /E

robocopy "EFCore.BulkExtensions" "EFCore.BulkExtensions.Sqlite" /E /XF "EFCore.BulkExtensions.csproj" /XD "bin" "obj" "SqlAdapters"
robocopy "EFCore.BulkExtensions\SqlAdapters" "EFCore.BulkExtensions.Sqlite\SqlAdapters" *.*
robocopy "EFCore.BulkExtensions\SqlAdapters\Sqlite" "EFCore.BulkExtensions.Sqlite\SqlAdapters\Sqlite" /E

robocopy "EFCore.BulkExtensions" "EFCore.BulkExtensions.SqlServer" /E /XF "EFCore.BulkExtensions.csproj" /XD "bin" "obj" "SqlAdapters"
robocopy "EFCore.BulkExtensions\SqlAdapters" "EFCore.BulkExtensions.SqlServer\SqlAdapters" *.*
robocopy "EFCore.BulkExtensions\SqlAdapters\SqlServer" "EFCore.BulkExtensions.SqlServer\SqlAdapters\SqlServer" /E
