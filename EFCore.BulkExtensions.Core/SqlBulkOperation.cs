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

    public static void Insert<T>(BulkContext context, Type type, IEnumerable<T> entities, TableInfo tableInfo, Action<decimal>? progress)
    {
        context.Adapter.Insert(context, type, entities, tableInfo, progress);
    }

    public static async Task InsertAsync<T>(BulkContext context, Type type, IEnumerable<T> entities, TableInfo tableInfo, Action<decimal>? progress, CancellationToken cancellationToken)
    {
        await context.Adapter.InsertAsync(context, type, entities, tableInfo, progress, cancellationToken).ConfigureAwait(false);
    }

    public static void Merge<T>(BulkContext context, Type type, IEnumerable<T> entities, TableInfo tableInfo, OperationType operationType, Action<decimal>? progress) where T : class
    {
        context.Adapter.Merge(context, type, entities, tableInfo, operationType, progress);
    }

    public static async Task MergeAsync<T>(BulkContext context, Type type, IEnumerable<T> entities, TableInfo tableInfo, OperationType operationType, Action<decimal>? progress, CancellationToken cancellationToken = default) where T : class
    {
        await context.Adapter.MergeAsync(context, type, entities, tableInfo, operationType, progress, cancellationToken).ConfigureAwait(false);
    }

    public static void Read<T>(BulkContext context, Type type, IEnumerable<T> entities, TableInfo tableInfo, Action<decimal>? progress) where T : class
    {
        context.Adapter.Read(context, type, entities, tableInfo, progress);
    }

    public static async Task ReadAsync<T>(BulkContext context, Type type, IEnumerable<T> entities, TableInfo tableInfo, Action<decimal>? progress, CancellationToken cancellationToken) where T : class
    {
        await context.Adapter.ReadAsync(context, type, entities, tableInfo, progress, cancellationToken).ConfigureAwait(false);
    }

    public static void Truncate(BulkContext context, TableInfo tableInfo)
    {
        context.Adapter.Truncate(context, tableInfo);
    }

    public static async Task TruncateAsync(BulkContext context, TableInfo tableInfo, CancellationToken cancellationToken)
    {
        await context.Adapter.TruncateAsync(context, tableInfo, cancellationToken).ConfigureAwait(false);
    }
}
