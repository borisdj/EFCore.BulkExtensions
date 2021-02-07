using System;
using System.Collections.Generic;
using System.Linq;
using EFCore.BulkExtensions.SQLAdapters.SQLite;
using EFCore.BulkExtensions.SQLAdapters.SQLServer;
using Microsoft.EntityFrameworkCore;

namespace EFCore.BulkExtensions.SqlAdapters
{
    public enum DbServer
    {
        SqlServer,
        Sqlite,
        //PostgreSql, // ProviderName can be added as  optional Attribute of Enum so it can be defined when not the same, like Npgsql for PostgreSql
        //MySql,
    }

    public static class SqlAdaptersMapping
    {
        public static readonly Dictionary<DbServer, ISqlOperationsAdapter> SqlOperationAdapterMapping =
            new Dictionary<DbServer, ISqlOperationsAdapter>
            {
                {DbServer.Sqlite, new SqLiteOperationsAdapter()},
                {DbServer.SqlServer, new SqlOperationsServerAdapter()}
            };

        public static readonly Dictionary<DbServer, IQueryBuilderSpecialization> SqlQueryBuilderSpecializationMapping =
            new Dictionary<DbServer, IQueryBuilderSpecialization>
            {
                {DbServer.Sqlite, new SqLiteDialect()},
                {DbServer.SqlServer, new SqlServerDialect()}
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
            return context.Database.ProviderName.EndsWith(DbServer.Sqlite.ToString()) ? DbServer.Sqlite : DbServer.SqlServer;
        }
    }
}
