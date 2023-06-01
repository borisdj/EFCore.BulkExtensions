using Microsoft.EntityFrameworkCore;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;

namespace EFCore.BulkExtensions;

/// <summary>
/// Contains a list of Batch IQuerable extensions
/// </summary>
public static class IQueryableBatchExtensions
{
    // Delete methods
    #region BatchDelete
    /// <summary>
    /// Extension method to batch delete data
    /// </summary>
    /// <param name="query"></param>
    /// <returns></returns>
    public static int BatchDelete(this IQueryable query)
    {
        var (context, sql, sqlParameters) = GetBatchDeleteArguments(query);
        return context.Database.ExecuteSqlRaw(sql, sqlParameters);
    }

    /// <summary>
    /// Extension method to batch delete data
    /// </summary>
    /// <param name="query"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    public static async Task<int> BatchDeleteAsync(this IQueryable query, CancellationToken cancellationToken = default)
    {
        var (context, sql, sqlParameters) = GetBatchDeleteArguments(query);
        return await context.Database.ExecuteSqlRawAsync(sql, sqlParameters, cancellationToken).ConfigureAwait(false);
    }

    private static (DbContext, string, List<object>) GetBatchDeleteArguments(IQueryable query)
    {
        var context = BatchUtil.GetDbContext(query);
        if (context is null)
        {
            throw new ArgumentException("Unable to determine context");
        }
        var (sql, sqlParameters) = BatchUtil.GetSqlDelete(query, context);
        return (context, sql, sqlParameters);
    }
    #endregion

    // Update methods
    #region BatchUpdate
    /// <summary>
    /// Extension method to batch update data
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <param name="query"></param>
    /// <param name="updateValues"></param>
    /// <param name="updateColumns"></param>
    /// <returns></returns>
    public static int BatchUpdate<T>(this IQueryable<T> query, object updateValues, List<string> ?updateColumns = null) where T : class
    {
        var (context, sql, sqlParameters) = GetBatchUpdateArguments(query, updateValues, updateColumns);
        return context.Database.ExecuteSqlRaw(sql, sqlParameters);
    }

    /// <summary>
    /// Extension method to batch update data
    /// </summary>
    /// <param name="query"></param>
    /// <param name="updateValues"></param>
    /// <param name="updateColumns"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    public static async Task<int> BatchUpdateAsync(this IQueryable query, object updateValues, List<string>? updateColumns = null, CancellationToken cancellationToken = default)
    {
        var (context, sql, sqlParameters) = GetBatchUpdateArguments((IQueryable<object>)query, updateValues, updateColumns);
        return await context.Database.ExecuteSqlRawAsync(sql, sqlParameters, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Extension method to batch update data
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <param name="query"></param>
    /// <param name="updateExpression"></param>
    /// <param name="type"></param>
    /// <returns></returns>
    public static int BatchUpdate<T>(this IQueryable<T> query, Expression<Func<T, T>> updateExpression, Type? type = null) where T : class
    {
        var (context, sql, sqlParameters) = GetBatchUpdateArguments(query, updateExpression: updateExpression, type: type);
        return context.Database.ExecuteSqlRaw(sql, sqlParameters);
    }

    /// <summary>
    /// Extension method to batch update data
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <param name="query"></param>
    /// <param name="updateExpression"></param>
    /// <param name="type"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    public static async Task<int> BatchUpdateAsync<T>(this IQueryable<T> query, Expression<Func<T, T>> updateExpression, Type? type = null, CancellationToken cancellationToken = default) where T : class
    {
        var (context, sql, sqlParameters) = GetBatchUpdateArguments(query, updateExpression: updateExpression, type: type);
        return await context.Database.ExecuteSqlRawAsync(sql, sqlParameters, cancellationToken).ConfigureAwait(false);
    }

    private static (DbContext, string, List<object>) GetBatchUpdateArguments<T>(IQueryable<T> query, object? updateValues = null, List<string>? updateColumns = null, Expression<Func<T, T>>? updateExpression = null, Type? type = null) where T : class
    {
        type ??= typeof(T);
        var context = BatchUtil.GetDbContext(query);
        if (context is null)
        {
            throw new ArgumentException("Unable to determine context");
        }
        var (sql, sqlParameters) = updateExpression == null ? BatchUtil.GetSqlUpdate(query, context, type, updateValues, updateColumns)
                                                            : BatchUtil.GetSqlUpdate(query, context, type, updateExpression);
        return (context, sql, sqlParameters);
    }
    #endregion
}
