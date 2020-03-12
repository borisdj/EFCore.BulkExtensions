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
        public static int BatchDelete(this IQueryable query)
        {
            DbContext context = BatchUtil.GetDbContext(query);
            (string sql, var sqlParameters) = BatchUtil.GetSqlDelete(query, context);
            return context.Database.ExecuteSqlRaw(sql, sqlParameters);
        }

        public static int BatchUpdate(this IQueryable query, object updateValues, List<string> updateColumns = null)
        {
            DbContext context = BatchUtil.GetDbContext(query);
            var (sql, sqlParameters) = BatchUtil.GetSqlUpdate(query, context, updateValues, updateColumns);
            return context.Database.ExecuteSqlRaw(sql, sqlParameters);
        }

        public static int BatchUpdate<T>(this IQueryable<T> query, Expression<Func<T, T>> updateExpression) where T : class
        {
            var context = BatchUtil.GetDbContext(query);
            var (sql, sqlParameters) = BatchUtil.GetSqlUpdate(query, context, updateExpression);
            return context.Database.ExecuteSqlRaw(sql, sqlParameters);
        }

        public static int BatchUpdate(this IQueryable query, Type type, Expression<Func<object, object>> updateExpression)
        {
            var context = BatchUtil.GetDbContext(query);
            var (sql, sqlParameters) = BatchUtil.GetSqlUpdate(query, context, type, updateExpression);
            return context.Database.ExecuteSqlRaw(sql, sqlParameters);
        }

        // Async methods

        public static async Task<int> BatchDeleteAsync(this IQueryable query, CancellationToken cancellationToken = default)
        {
            DbContext context = BatchUtil.GetDbContext(query);
            (string sql, var sqlParameters) = BatchUtil.GetSqlDelete(query, context);
            return await context.Database.ExecuteSqlRawAsync(sql, sqlParameters, cancellationToken).ConfigureAwait(false);
        }

        public static async Task<int> BatchUpdateAsync(this IQueryable query, object updateValues, List<string> updateColumns = null, CancellationToken cancellationToken = default)
        {
            DbContext context = BatchUtil.GetDbContext(query);
            var (sql, sqlParameters) = BatchUtil.GetSqlUpdate(query, context, updateValues, updateColumns);
            return await context.Database.ExecuteSqlRawAsync(sql, sqlParameters, cancellationToken).ConfigureAwait(false);
        }

        public static async Task<int> BatchUpdateAsync<T>(this IQueryable<T> query, Expression<Func<T, T>> updateExpression, CancellationToken cancellationToken = default) where T : class
        {
            var context = BatchUtil.GetDbContext(query);
            var (sql, sqlParameters) = BatchUtil.GetSqlUpdate(query, context, updateExpression);
            return await context.Database.ExecuteSqlRawAsync(sql, sqlParameters, cancellationToken).ConfigureAwait(false);
        }

        public static async Task<int> BatchUpdateAsync(this IQueryable query, Type type, Expression<Func<object, object>> updateExpression, CancellationToken cancellationToken = default)
        {
            var context = BatchUtil.GetDbContext(query);
            var (sql, sqlParameters) = BatchUtil.GetSqlUpdate(query, context, type, updateExpression);
            return await context.Database.ExecuteSqlRawAsync(sql, sqlParameters, cancellationToken).ConfigureAwait(false);
        }
    }
}
