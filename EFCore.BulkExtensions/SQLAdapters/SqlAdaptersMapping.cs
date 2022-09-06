using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using System.Collections.Generic;
using System.ComponentModel;

namespace EFCore.BulkExtensions.SqlAdapters;

/// <summary>
/// A list of database servers supported by EFCore.BulkExtensions
/// </summary>
public enum DbServer
{
    /// <summary>
    /// Indicates database is Microsoft's SQL Server
    /// </summary>
    [Description("SqlServer")] //DbProvider name in attribute
    SQLServer,

    /// <summary>
    /// Indicates database is SQL Lite
    /// </summary>
    [Description("Sqlite")]
    SQLite,

    /// <summary>
    /// Indicates database is Npgql
    /// </summary>
    [Description("Npgql")]
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
    /// <summary>
    /// Contains a list of methods to generate Adpaters and helpers instances
    /// </summary>
    public static IDbServer? DbServer { get; set; }

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
    public static DbServer GetDatabaseType()
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
