:: Bat procedures that copies Source files from main project to each per provider project, skippes main .csproj file and other provider adapters
:: When used lines for linking files via Include/Remove should first be removed from .csproject per provider

robocopy "..\EFCore.BulkExtensions" "..\EFCore.BulkExtensions.PerProvider\EFCore.BulkExtensions.MySql" /E /XF "*.csproj" "*.png" /XD "bin" "obj" "SqlAdapters"
robocopy "..\EFCore.BulkExtensions\SqlAdapters" "..\EFCore.BulkExtensions.PerProvider\EFCore.BulkExtensions.MySql\SqlAdapters" *.*
robocopy "..\EFCore.BulkExtensions\SqlAdapters\MySql " "..\EFCore.BulkExtensions.PerProvider\EFCore.BulkExtensions.MySql\SqlAdapters\MySql" /E

robocopy "..\EFCore.BulkExtensions" "..\EFCore.BulkExtensions.PerProvider\EFCore.BulkExtensions.PostgreSql" /E /XF "*.csproj" "*.png" /XD "bin" "obj" "SqlAdapters"
robocopy "..\EFCore.BulkExtensions\SqlAdapters" "..\EFCore.BulkExtensions.PerProvider\EFCore.BulkExtensions.PostgreSql\SqlAdapters" *.*
robocopy "..\EFCore.BulkExtensions\SqlAdapters\PostgreSql" "..\EFCore.BulkExtensions.PerProvider\EFCore.BulkExtensions.PostgreSql\SqlAdapters\PostgreSql" /E

robocopy "..\EFCore.BulkExtensions" "..\EFCore.BulkExtensions.PerProvider\EFCore.BulkExtensions.Sqlite" /E /XF "*.csproj" "*.png" /XD "bin" "obj" "SqlAdapters"
robocopy "..\EFCore.BulkExtensions\SqlAdapters" "..\EFCore.BulkExtensions.PerProvider\EFCore.BulkExtensions.Sqlite\SqlAdapters" *.*
robocopy "..\EFCore.BulkExtensions\SqlAdapters\Sqlite" "..\EFCore.BulkExtensions.PerProvider\EFCore.BulkExtensions.Sqlite\SqlAdapters\Sqlite" /E

robocopy "..\EFCore.BulkExtensions" "..\EFCore.BulkExtensions.PerProvider\EFCore.BulkExtensions.SqlServer" /E /XF "*.csproj" "*.png" /XD "bin" "obj" "SqlAdapters"
robocopy "..\EFCore.BulkExtensions\SqlAdapters" "..\EFCore.BulkExtensions.PerProvider\EFCore.BulkExtensions.SqlServer\SqlAdapters" *.*
robocopy "..\EFCore.BulkExtensions\SqlAdapters\SqlServer" "..\EFCore.BulkExtensions.PerProvider\EFCore.BulkExtensions.SqlServer\SqlAdapters\SqlServer" /E
