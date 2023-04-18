using Microsoft.EntityFrameworkCore;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace EFCore.BulkExtensions.SqlAdapters;

/// <summary>
/// Contains a list of SQL operations
/// </summary>
public interface ISqlOperationsAdapter
{
    /// <summary>
    /// Inserts a list of entities
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <param name="context"></param>
    /// <param name="type"></param>
    /// <param name="entities"></param>
    /// <param name="tableInfo"></param>
    /// <param name="progress"></param>
    void Insert<T>(DbContext context, Type type, IList<T> entities, TableInfo tableInfo, Action<decimal>? progress);

    /// <summary>
    /// Inserts a list of entities
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <param name="context"></param>
    /// <param name="type"></param>
    /// <param name="entities"></param>
    /// <param name="tableInfo"></param>
    /// <param name="progress"></param>
    /// <param name="cancellationToken"></param>
    Task InsertAsync<T>(DbContext context, Type type, IList<T> entities, TableInfo tableInfo, Action<decimal>? progress, CancellationToken cancellationToken);

    /// <summary>
    /// Merges a list of entities with a table source
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <param name="context"></param>
    /// <param name="type"></param>
    /// <param name="entities"></param>
    /// <param name="tableInfo"></param>
    /// <param name="operationType"></param>
    /// <param name="progress"></param>
    void Merge<T>(DbContext context, Type type, IList<T> entities, TableInfo tableInfo, OperationType operationType, Action<decimal>? progress) where T : class;

    /// <summary>
    /// Merges a list of entities with a table source
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <param name="context"></param>
    /// <param name="type"></param>
    /// <param name="entities"></param>
    /// <param name="tableInfo"></param>
    /// <param name="operationType"></param>
    /// <param name="progress"></param>
    /// <param name="cancellationToken"></param>
    Task MergeAsync<T>(DbContext context, Type type, IList<T> entities, TableInfo tableInfo, OperationType operationType, Action<decimal>? progress, CancellationToken cancellationToken) where T : class;

    /// <summary>
    /// Reads a list of entities from database
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <param name="context"></param>
    /// <param name="type"></param>
    /// <param name="entities"></param>
    /// <param name="tableInfo"></param>
    /// <param name="progress"></param>
    void Read<T>(DbContext context, Type type, IList<T> entities, TableInfo tableInfo, Action<decimal>? progress) where T : class;

    /// <summary>
    /// Reads a list of entities from database
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <param name="context"></param>
    /// <param name="type"></param>
    /// <param name="entities"></param>
    /// <param name="tableInfo"></param>
    /// <param name="progress"></param>
    /// <param name="cancellationToken"></param>
    Task ReadAsync<T>(DbContext context, Type type, IList<T> entities, TableInfo tableInfo, Action<decimal>? progress, CancellationToken cancellationToken) where T : class;

    /// <summary>
    /// Truncates a table
    /// </summary>
    /// <param name="context"></param>
    /// <param name="tableInfo"></param>
    void Truncate(DbContext context, TableInfo tableInfo);

    /// <summary>
    /// Truncates a table
    /// </summary>
    /// <param name="context"></param>
    /// <param name="tableInfo"></param>
    /// <param name="cancellationToken"></param>
    Task TruncateAsync(DbContext context, TableInfo tableInfo, CancellationToken cancellationToken);
}
