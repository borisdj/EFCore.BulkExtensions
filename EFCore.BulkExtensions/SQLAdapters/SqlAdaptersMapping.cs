using EFCore.BulkExtensions.SQLAdapters.PostgreSql;
using EFCore.BulkExtensions.SQLAdapters.SQLite;
using EFCore.BulkExtensions.SQLAdapters.SQLServer;
using Microsoft.EntityFrameworkCore;
using System.Collections.Generic;

namespace EFCore.BulkExtensions.SqlAdapters
{
    public enum DbServer
    {
        SqlServer,
        Sqlite,
        PostgreSql, // ProviderName can be added as  optional Attribute of Enum so it can be defined when not the same, like Npgsql for PostgreSql
        //MySql,
    }

    public static class SqlAdaptersMapping
    {
        public static readonly Dictionary<DbServer, ISqlOperationsAdapter> SqlOperationAdapterMapping =
            new Dictionary<DbServer, ISqlOperationsAdapter>
            {
                {DbServer.SqlServer, new SqlOperationsServerAdapter()},
                {DbServer.Sqlite, new SqLiteOperationsAdapter()},
                {DbServer.PostgreSql, new PostgreSqlAdapter()}
            };

        public static readonly Dictionary<DbServer, IQueryBuilderSpecialization> SqlQueryBuilderSpecializationMapping =
            new Dictionary<DbServer, IQueryBuilderSpecialization>
            {
                {DbServer.SqlServer, new SqlServerDialect()},
                {DbServer.Sqlite, new SqLiteDialect()},
                {DbServer.PostgreSql, new PostgreSqlDialect()}
            };

        public static ISqlOperationsAdapter CreateBulkOperationsAdapter(DbContext context)
        {
            var providerType = GetDatabaseType(context);
            return SqlOperationAdapterMapping[providerType];
        }

        public static IQueryBuilderSpecialization GetAdapterDialect(DbContext context)
        {
            var providerType = GetDatabaseType(context);
            return GetAdapterDialect(providerType);
        }
        
        public static IQueryBuilderSpecialization GetAdapterDialect(DbServer providerType)
        {
            return SqlQueryBuilderSpecializationMapping[providerType];
        }

        public static DbServer GetDatabaseType(DbContext context)
        {
            var databaseType = context.Database.ProviderName.EndsWith(DbServer.Sqlite.ToString()) ? DbServer.Sqlite : DbServer.SqlServer;
            if(context.Database.ProviderName.StartsWith("Npgsql"))
                databaseType = DbServer.PostgreSql;
            return databaseType;
        }
    }
}
