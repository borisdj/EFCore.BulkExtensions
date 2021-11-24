using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;

namespace EFCore.BulkExtensions
{
    public static class IQueryableBatchExtensions
    {
        // Delete methods
        #region BatchDelete
        public static int BatchDelete(this IQueryable query)
        {
            var (context, sql, sqlParameters) = GetBatchDeleteArguments(query);
            return context.Database.ExecuteSqlRaw(sql, sqlParameters);
        }
        public static async Task<int> BatchDeleteAsync(this IQueryable query, CancellationToken cancellationToken = default)
        {
            var (context, sql, sqlParameters) = GetBatchDeleteArguments(query);
            return await context.Database.ExecuteSqlRawAsync(sql, sqlParameters, cancellationToken).ConfigureAwait(false);
        }

        private static (DbContext, string, List<object>) GetBatchDeleteArguments(IQueryable query)
        {
            var context = BatchUtil.GetDbContext(query);
            var (sql, sqlParameters) = BatchUtil.GetSqlDelete(query, context);
            return (context, sql, sqlParameters);
        }
        #endregion

        // Update methods
        #region BatchUpdate
        public static int BatchUpdate(this IQueryable query, object updateValues, List<string> updateColumns = null)
        {
            var (context, sql, sqlParameters) = GetBatchUpdateArguments((IQueryable<object>)query, updateValues, updateColumns);
            return context.Database.ExecuteSqlRaw(sql, sqlParameters);
        }
        public static async Task<int> BatchUpdateAsync(this IQueryable query, object updateValues, List<string> updateColumns = null, CancellationToken cancellationToken = default)
        {
            var (context, sql, sqlParameters) = GetBatchUpdateArguments((IQueryable<object>)query, updateValues, updateColumns);
            return await context.Database.ExecuteSqlRawAsync(sql, sqlParameters, cancellationToken).ConfigureAwait(false);
        }

        public static int BatchUpdate<T>(this IQueryable<T> query, Expression<Func<T, T>> updateExpression, Type type = null) where T : class
        {
            var (context, sql, sqlParameters) = GetBatchUpdateArguments(query, updateExpression: updateExpression, type: type);
            return context.Database.ExecuteSqlRaw(sql, sqlParameters);
        }
        public static async Task<int> BatchUpdateAsync<T>(this IQueryable<T> query, Expression<Func<T, T>> updateExpression, Type type = null, CancellationToken cancellationToken = default) where T : class
        {
            var (context, sql, sqlParameters) = GetBatchUpdateArguments(query, updateExpression: updateExpression, type: type);
            return await context.Database.ExecuteSqlRawAsync(sql, sqlParameters, cancellationToken).ConfigureAwait(false);
        }

        private static (DbContext, string, List<object>) GetBatchUpdateArguments<T>(IQueryable<T> query, object updateValues = null, List<string> updateColumns = null, Expression<Func<T, T>> updateExpression = null, Type type = null) where T : class
        {
            type ??= typeof(T);
            var context = BatchUtil.GetDbContext(query);
            var (sql, sqlParameters) = updateExpression == null ? BatchUtil.GetSqlUpdate(query, context, updateValues, updateColumns)
                                                                : BatchUtil.GetSqlUpdate(query, context, type, updateExpression);
            return (context, sql, sqlParameters);
        }
        #endregion
    }
}
