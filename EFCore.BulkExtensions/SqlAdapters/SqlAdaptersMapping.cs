using System;

namespace EFCore.BulkExtensions.SqlAdapters;

/// <summary>
/// A list of database servers supported by EFCore.BulkExtensions
/// </summary>
public enum SqlType
{
    /// <summary>
    /// Indicates database is Microsoft's SQL Server
    /// </summary>
    SqlServer,

    /// <summary>
    /// Indicates database is SQLite
    /// </summary>
    Sqlite,

    /// <summary>
    /// Indicates database is PostgreSQL
    /// </summary>
    PostgreSql,

    /// <summary>
    ///  Indicates database is MySQL
    /// </summary>
    MySql,
}

#pragma warning disable CS1591 // No XML comment required here
public static class SqlAdaptersMapping
{
    public static string? ProviderName { get; set; }

    public static SqlType DatabaseType { get; set; }

    private static IDbServer? _dbServer { get; set; }

    /// <summary>
    /// Contains a list of methods to generate Adapters and helpers instances
    /// </summary>
    public static IDbServer? DbServer {
        get
        {
            // Context.Database. methods:
            //   IsSqlServer()
            //   IsNpgsql()
            //   IsMySql()
            //   IsSqlite()
            // requires specific provider so instead here used -ProviderName

            var ignoreCase = StringComparison.InvariantCultureIgnoreCase;

            SqlType databaseType = SqlType.SqlServer;                                       // ProviderName: Microsoft.EntityFrameworkCore.SqlServer

            if (ProviderName?.EndsWith(SqlType.PostgreSql.ToString(), ignoreCase) ?? false) // ProviderName: Npgsql.EntityFrameworkCore.PostgreSQL
            {
                databaseType = SqlType.PostgreSql;
            }
            else if (ProviderName?.EndsWith(SqlType.MySql.ToString(), ignoreCase) ?? false) // ProviderName: Pomelo.EntityFrameworkCore.MySql
            {
                databaseType = SqlType.MySql;
            }
            else if(ProviderName?.EndsWith(SqlType.Sqlite.ToString(), ignoreCase) ?? false) // ProviderName: Microsoft.EntityFrameworkCore.Sqlite
            {
                databaseType = SqlType.Sqlite;
            }

            if (_dbServer == null || _dbServer.Type != databaseType)
            {
                string namespaceSqlAdaptersTEXT = "EFCore.BulkExtensions.SqlAdapters";
                Type? dbServerType = null;

                if (databaseType == SqlType.SqlServer)
                {
                    dbServerType = Type.GetType(namespaceSqlAdaptersTEXT + ".SqlServer.SqlServerDbServer");
                }
                else if (databaseType == SqlType.PostgreSql)
                {
                    dbServerType = Type.GetType(namespaceSqlAdaptersTEXT + ".PostgreSql.PostgreSqlDbServer");
                }
                else if (databaseType == SqlType.MySql)
                {
                    dbServerType = Type.GetType(namespaceSqlAdaptersTEXT + ".MySql.MySqlDbServer");
                }
                else if (databaseType == SqlType.Sqlite)
                {
                    dbServerType = Type.GetType(namespaceSqlAdaptersTEXT + ".Sqlite.SqliteDbServer");
                }

                var dbServerInstance = Activator.CreateInstance(dbServerType ?? typeof(int));
                _dbServer = dbServerInstance as IDbServer;
            }
            return _dbServer;
        }
    }

#pragma warning restore CS1591 // No XML comment required here

    /// <summary>
    /// Creates the bulk operations adapter
    /// </summary>
    /// <returns></returns>
    public static ISqlOperationsAdapter CreateBulkOperationsAdapter()
    {
        return DbServer!.Adapter;
    }

    /// <summary>
    /// Returns the Adapter dialect to be used
    /// </summary>
    /// <returns></returns>
    public static IQueryBuilderSpecialization GetAdapterDialect()
    {
        return DbServer!.Dialect;
    }

    /// <summary>
    /// Returns the Database type
    /// </summary>
    /// <returns></returns>
    public static SqlType GetDatabaseType()
    {
        return DbServer!.Type;
    }

    /// <summary>
    /// Returns per provider QueryBuilder instance, containing a compilation of SQL queries used in EFCore.
    /// </summary>
    /// <returns></returns>
    public static SqlQueryBuilder GetQueryBuilder()
    {
        return DbServer!.QueryBuilder;
    }
}
