using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
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

    /// <summary>
    ///  Indicates database is Oracle
    /// </summary>
    Oracle,
}

#pragma warning disable CS1591 // No XML comment required here
public static class SqlAdaptersMapping
{
    public static Func<DbContext, IDbServer>? Provider { get; set; }

    private static IDbServer? _dbServer { get; set; }

    /// <summary>
    /// Contains a list of methods to generate Adapters and helpers instances
    /// </summary>
    public static IDbServer DbServer(DbContext context)
    {
        var fromService = TryGetServer(context);
        if (fromService != null)
        {
            return fromService;
        }

        var fromProvider = Provider?.Invoke(context);
        if (fromProvider != null)
        {
            return fromProvider;
        }

        var ignoreCase = StringComparison.InvariantCultureIgnoreCase;
        var databaseType = SqlType.SqlServer;
        var providerName = context.Database.ProviderName;
        if (providerName?.EndsWith(SqlType.PostgreSql.ToString(), ignoreCase) ?? false) // ProviderName: Npgsql.EntityFrameworkCore.PostgreSQL
        {
            databaseType = SqlType.PostgreSql;
        }
        else if (providerName?.EndsWith(SqlType.MySql.ToString(), ignoreCase) ?? false) // ProviderName: Pomelo.EntityFrameworkCore.MySql
        {
            databaseType = SqlType.MySql;
        }
        else if (providerName?.EndsWith(SqlType.Sqlite.ToString(), ignoreCase) ?? false) // ProviderName: Microsoft.EntityFrameworkCore.Sqlite
        {
            databaseType = SqlType.Sqlite;
        }
        else if (providerName?.Contains(SqlType.Oracle.ToString(), ignoreCase) ?? false) // ProviderName: Microsoft.EntityFrameworkCore.Sqlite
        {
            databaseType = SqlType.Oracle;
        }

        if (_dbServer == null || _dbServer.Type != databaseType)
        {
            static Type GetType(SqlType type)
            {
                var typeName = type.ToString();
                var assemblyName = typeof(SqlAdaptersMapping).Assembly.GetName().Name!.Replace(".Core", $".{typeName}");

                return Type.GetType($"EFCore.BulkExtensions.SqlAdapters.{typeName}.{typeName}DbServer,{assemblyName}") ??
                    throw new InvalidOperationException("Failed to resolve type.");
            }

            _dbServer = Activator.CreateInstance(GetType(databaseType)) as IDbServer;
        }


        return _dbServer ?? throw new InvalidOperationException("Failed to create DbServer");
    }

    private static IDbServer? TryGetServer(DbContext context)
    {
        try
        {
            return context.Database.GetService<IDbServer>();
        }
        catch (InvalidOperationException)
        {
            // There is no "TryGetService" or something similar.
            return null;
        }
    }

#pragma warning restore CS1591 // No XML comment required here
}
