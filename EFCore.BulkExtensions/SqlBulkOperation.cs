using EFCore.BulkExtensions.SqlAdapters;
using Microsoft.EntityFrameworkCore;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace EFCore.BulkExtensions;

/// <summary>
/// Describes the operation type
/// </summary>
public enum OperationType
{
    /// <summary>
    /// Operation to insert a list of entities
    /// </summary>
    Insert,
    /// <summary>
    /// Operation to insert or update a list of entities
    /// </summary>
    InsertOrUpdate,
    /// <summary>
    /// Operation to sync source table with a list of entities by inserting (or updating) and deleting records
    /// </summary>
    InsertOrUpdateOrDelete,
    /// <summary>
    /// Operation to update a list of entities
    /// </summary>
    Update,
    /// <summary>
    /// Operation to delete a list of entities
    /// </summary>
    Delete,
    /// <summary>
    /// Operation to read a list of entities
    /// </summary>
    Read,
    /// <summary>
    /// Operation to truncate source table
    /// </summary>
    Truncate,
    /// <summary>
    /// Operation to use Entity Change Tracker to update/insert/delete entities
    /// </summary>
    SaveChanges,
}

internal static class SqlBulkOperation
{
    internal static string ColumnMappingExceptionMessage => "The given ColumnMapping does not match up with any column in the source or destination";

    public static void Insert<T>(DbContext context, Type type, IList<T> entities, TableInfo tableInfo, Action<decimal> progress)
    {
        var adapter = SqlAdaptersMapping.CreateBulkOperationsAdapter(context);
        adapter.Insert(context, type, entities, tableInfo, progress);
    }

    public static async Task InsertAsync<T>(DbContext context, Type type, IList<T> entities, TableInfo tableInfo, Action<decimal> progress, CancellationToken cancellationToken)
    {
        var adapter = SqlAdaptersMapping.CreateBulkOperationsAdapter(context);
        await adapter.InsertAsync(context, type, entities, tableInfo, progress, cancellationToken);
    }

    public static void Merge<T>(DbContext context, Type type, IList<T> entities, TableInfo tableInfo, OperationType operationType, Action<decimal> progress) where T : class
    {
        var adapter = SqlAdaptersMapping.CreateBulkOperationsAdapter(context);
        adapter.Merge(context, type, entities, tableInfo, operationType, progress);
    }

    public static async Task MergeAsync<T>(DbContext context, Type type, IList<T> entities, TableInfo tableInfo, OperationType operationType, Action<decimal> progress, CancellationToken cancellationToken = default) where T : class
    {
        var adapter = SqlAdaptersMapping.CreateBulkOperationsAdapter(context);
        await adapter.MergeAsync(context, type, entities, tableInfo, operationType, progress, cancellationToken);
    }

    public static void Read<T>(DbContext context, Type type, IList<T> entities, TableInfo tableInfo, Action<decimal> progress) where T : class
    {
        if (tableInfo.BulkConfig.UseTempDB) // dropTempTableIfExists
        {
            context.Database.ExecuteSqlRaw(SqlQueryBuilder.DropTable(tableInfo.FullTempTableName, tableInfo.BulkConfig.UseTempDB));
        }
        var adapter = SqlAdaptersMapping.CreateBulkOperationsAdapter(context);
        adapter.Read(context, type, entities, tableInfo, progress);
    }

    public static async Task ReadAsync<T>(DbContext context, Type type, IList<T> entities, TableInfo tableInfo, Action<decimal> progress, CancellationToken cancellationToken) where T : class
    {
        if (tableInfo.BulkConfig.UseTempDB) // dropTempTableIfExists
        {
            await context.Database.ExecuteSqlRawAsync(SqlQueryBuilder.DropTable(tableInfo.FullTempTableName, tableInfo.BulkConfig.UseTempDB), cancellationToken).ConfigureAwait(false);
        }
        var adapter = SqlAdaptersMapping.CreateBulkOperationsAdapter(context);
        await adapter.ReadAsync(context, type, entities, tableInfo, progress, cancellationToken);
    }

    public static void Truncate(DbContext context, TableInfo tableInfo)
    {
        var adapter = SqlAdaptersMapping.CreateBulkOperationsAdapter(context);
        adapter.Truncate(context, tableInfo);
    }

    public static async Task TruncateAsync(DbContext context, TableInfo tableInfo, CancellationToken cancellationToken)
    {
        var adapter = SqlAdaptersMapping.CreateBulkOperationsAdapter(context);
        await adapter.TruncateAsync(context, tableInfo, cancellationToken).ConfigureAwait(false);
    }
}
