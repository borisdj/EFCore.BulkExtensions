using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.EntityFrameworkCore;

namespace EFCore.BulkExtensions
{
    public static class IQueryableBatchExtensions
    {
        public static int BatchDelete<T>(this IQueryable<T> query, Dictionary<string, object> parametersDict = null) where T : class
        {
            DbContext context = BatchUtil.GetDbContext(query);
            (string sql, var sqlParameters) = BatchUtil.GetSqlDelete(query, context, parametersDict);
            return BatchUtil.ExecuteSql(context, sql, sqlParameters);
        }

        public static int BatchUpdate<T>(this IQueryable<T> query, T updateValues, List<string> updateColumns = null, Dictionary<string, object> parametersDict = null) where T : class, new()
        {
            DbContext context = BatchUtil.GetDbContext(query);
            var (sql, sqlParameters) = BatchUtil.GetSqlUpdate(query, context, updateValues, updateColumns, parametersDict);
            return BatchUtil.ExecuteSql(context, sql, sqlParameters);
        }

        public static int BatchUpdate<T>(this IQueryable<T> query, Expression<Func<T, T>> updateExpression, Dictionary<string, object> parametersDict = null) where T : class
        {
            var context = BatchUtil.GetDbContext(query);
            var (sql, sqlParameters) = BatchUtil.GetSqlUpdate(query, context, updateExpression, parametersDict);
            return BatchUtil.ExecuteSql(context, sql, sqlParameters);
        }

        // Async methods

        public static async Task<int> BatchDeleteAsync<T>(this IQueryable<T> query, Dictionary<string, object> parametersDict = null, CancellationToken cancellationToken = default) where T : class
        {
            DbContext context = BatchUtil.GetDbContext(query);
            (string sql, var sqlParameters) = BatchUtil.GetSqlDelete(query, context, parametersDict);
            return await BatchUtil.ExecuteSqlAsync(context, sql, sqlParameters, cancellationToken).ConfigureAwait(false);
        }

        public static async Task<int> BatchUpdateAsync<T>(this IQueryable<T> query, T updateValues, List<string> updateColumns = null, Dictionary<string, object> parametersDict = null, CancellationToken cancellationToken = default) where T : class, new()
        {
            DbContext context = BatchUtil.GetDbContext(query);
            var (sql, sqlParameters) = BatchUtil.GetSqlUpdate(query, context, updateValues, updateColumns, parametersDict);
            return await BatchUtil.ExecuteSqlAsync(context, sql, sqlParameters, cancellationToken).ConfigureAwait(false);
        }

        public static async Task<int> BatchUpdateAsync<T>(this IQueryable<T> query, Expression<Func<T, T>> updateExpression, Dictionary<string, object> parametersDict = null, CancellationToken cancellationToken = default) where T : class
        {
            var context = BatchUtil.GetDbContext(query);
            var (sql, sqlParameters) = BatchUtil.GetSqlUpdate(query, context, updateExpression, parametersDict);
            return await BatchUtil.ExecuteSqlAsync(context, sql, sqlParameters, cancellationToken).ConfigureAwait(false);
        }
    }
}
