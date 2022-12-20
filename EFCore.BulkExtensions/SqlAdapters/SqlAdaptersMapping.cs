using System;
using System.ComponentModel;
using System.Reflection;

namespace EFCore.BulkExtensions.SqlAdapters;

/// <summary>
/// A list of database servers supported by EFCore.BulkExtensions
/// </summary>
public enum DbServerType
{
    /// <summary>
    /// Indicates database is Microsoft's SQL Server
    /// </summary>
    [Description("SqlServer")]
    SQLServer,

    /// <summary>
    /// Indicates database is SQL Lite
    /// </summary>
    [Description("SQLite")]
    SQLite,

    /// <summary>
    /// Indicates database is Postgres
    /// </summary>
    [Description("PostgreSql")]
    PostgreSQL,

    /// <summary>
    ///  Indicates database is MySQL
    /// </summary>
    [Description("MySql")]
    MySQL,
}

#pragma warning disable CS1591 // No XML comment required here
public static class SqlAdaptersMapping
{
    public static string? ProviderName { get; set; }

    public static DbServerType DbServerType { get; set; }

    private static IDbServer? _dbServer { get; set; }

    /// <summary>
    /// Contains a list of methods to generate Adapters and helpers instances
    /// </summary>
    public static IDbServer? DbServer {
        get
        {
            //Context.Database. methods: -IsSqlServer() -IsNpgsql() -IsMySql() -IsSqlite() requires specific provider so instead here used -ProviderName

            DbServerType serverType = DbServerType.SQLServer;
            if (ProviderName?.ToLower().EndsWith(DbServerType.PostgreSQL.ToString().ToLower()) ?? false)
            {
                serverType = DbServerType.PostgreSQL;
            }
            else if (ProviderName?.ToLower().EndsWith(DbServerType.MySQL.ToString().ToLower()) ?? false)
            {
                serverType = DbServerType.MySQL;
            }
            else if(ProviderName?.ToLower().EndsWith(DbServerType.SQLite.ToString().ToLower()) ?? false)
            {
                serverType = DbServerType.SQLite;
            }

            if (_dbServer == null || _dbServer.Type != serverType)
            {
                string EFCoreBulkExtensionsSqlAdaptersTEXT = "EFCore.BulkExtensions.SqlAdapters";
                Type? dbServerType = null;

                if (serverType == DbServerType.SQLServer)
                {
                    dbServerType = Type.GetType(EFCoreBulkExtensionsSqlAdaptersTEXT + ".SqlServer.SqlServerDbServer");
                }
                else if (serverType == DbServerType.PostgreSQL)
                {
                    dbServerType = Type.GetType(EFCoreBulkExtensionsSqlAdaptersTEXT + ".PostgreSql.PostgreSqlDbServer");
                }
                else if (serverType == DbServerType.MySQL)
                {
                    dbServerType = Type.GetType(EFCoreBulkExtensionsSqlAdaptersTEXT + ".MySql.MySqlDbServer");
                }
                else if (serverType == DbServerType.SQLite)
                {
                    dbServerType = Type.GetType(EFCoreBulkExtensionsSqlAdaptersTEXT + ".SQLite.SqlLiteDbServer");
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
    public static DbServerType GetDatabaseType()
    {
        return DbServer!.Type;
    }

    /// <summary>
    /// Returns per provider QueryBuilder instance, containing a compilation of SQL queries used in EFCore.
    /// </summary>
    /// <returns></returns>
    public static SqlAdapters.QueryBuilderExtensions GetQueryBuilder()
    {
        return DbServer!.QueryBuilder;
    }
}
