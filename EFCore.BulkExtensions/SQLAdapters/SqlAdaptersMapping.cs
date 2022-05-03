using EFCore.BulkExtensions.SQLAdapters.PostgreSql;
using EFCore.BulkExtensions.SQLAdapters.SQLite;
using EFCore.BulkExtensions.SQLAdapters.SQLServer;
using Microsoft.EntityFrameworkCore;
using System.Collections.Generic;
using System.ComponentModel;

namespace EFCore.BulkExtensions.SqlAdapters;

public enum DbServer
{
    [Description("SqlServer")] //DbProvider name in attribute
    SQLServer,

    [Description("Sqlite")]
    SQLite,

    [Description("Npgql")]
    PostgreSQL,

    //[Description("MySQL")]
    //MySQL,
}

public static class SqlAdaptersMapping
{
    public static readonly Dictionary<DbServer, ISqlOperationsAdapter> SqlOperationAdapterMapping =
        new Dictionary<DbServer, ISqlOperationsAdapter>
        {
            {DbServer.SQLServer, new SqlOperationsServerAdapter()},
            {DbServer.SQLite, new SqliteOperationsAdapter()},
            {DbServer.PostgreSQL, new PostgreSqlAdapter()}
        };

    public static readonly Dictionary<DbServer, IQueryBuilderSpecialization> SqlQueryBuilderSpecializationMapping =
        new Dictionary<DbServer, IQueryBuilderSpecialization>
        {
            {DbServer.SQLServer, new SqlServerDialect()},
            {DbServer.SQLite, new SqliteDialect()},
            {DbServer.PostgreSQL, new PostgreSqlDialect()}
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
        var databaseType = DbServer.SQLServer;
        if (context.Database.IsSqlite())
            databaseType = DbServer.SQLite;
        if (context.Database.IsNpgsql())
            databaseType = DbServer.PostgreSQL;

        return databaseType;
    }
}
