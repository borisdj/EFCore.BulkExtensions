using Microsoft.EntityFrameworkCore;
using System;
using System.Collections.Generic;
using System.Data.Common;
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
    [Obsolete("As of EF 7 there are native method ExcuteDelete.")] // ExcuteDelete does not support: context.Items.Include(x => x.ItemHistories).Where(x => !x.ItemHistories.Any()).ExecuteDelete(); // 'Include' could not be translated
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
    [Obsolete("As of EF 7 there are native method ExcuteDelete.")]
    public static async Task<int> BatchDeleteAsync(this IQueryable query, CancellationToken cancellationToken = default)
    {
        var (context, sql, sqlParameters) = GetBatchDeleteArguments(query);
        return await context.Database.ExecuteSqlRawAsync(sql, sqlParameters, cancellationToken).ConfigureAwait(false);
    }

    private static (DbContext, string, List<DbParameter>) GetBatchDeleteArguments(IQueryable query)
    {
        var dbContext = BatchUtil.GetDbContext(query);
        if (dbContext is null)
        {
            throw new ArgumentException("Unable to determine context");
        }
        var context = BulkContext.Create(dbContext);
        var (sql, sqlParameters) = BatchUtil.GetSqlDelete(query, context);
        return (dbContext, sql, sqlParameters);
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
    [Obsolete("As of EF 7 there are native method ExcuteUpdate.")]
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
    [Obsolete]
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
    [Obsolete("As of EF 7 there are native method ExcuteUpdate.")]
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
    [Obsolete("As of EF 7 there are native method ExcuteUpdate.")]
    public static async Task<int> BatchUpdateAsync<T>(this IQueryable<T> query, Expression<Func<T, T>> updateExpression, Type? type = null, CancellationToken cancellationToken = default) where T : class
    {
        var (context, sql, sqlParameters) = GetBatchUpdateArguments(query, updateExpression: updateExpression, type: type);
        return await context.Database.ExecuteSqlRawAsync(sql, sqlParameters, cancellationToken).ConfigureAwait(false);
    }

    private static (DbContext, string, List<DbParameter>) GetBatchUpdateArguments<T>(IQueryable<T> query, object? updateValues = null, List<string>? updateColumns = null, Expression<Func<T, T>>? updateExpression = null, Type? type = null) where T : class
    {
        type ??= typeof(T);
        var dbContext = BatchUtil.GetDbContext(query);
        if (dbContext is null)
        {
            throw new ArgumentException("Unable to determine context");
        }
        var context = BulkContext.Create(dbContext);
        var (sql, sqlParameters) = updateExpression == null ? BatchUtil.GetSqlUpdate(query, context, type, updateValues, updateColumns)
                                                            : BatchUtil.GetSqlUpdate(query, context, type, updateExpression);
        return (dbContext, sql, sqlParameters);
    }
    #endregion
}
