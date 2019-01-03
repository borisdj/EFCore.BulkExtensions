using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Linq.Expressions;
using System.Threading.Tasks;
using Microsoft.EntityFrameworkCore;

namespace EFCore.BulkExtensions
{
    public static class IQueryableBatchExtensions
    {
        public static int BatchDelete<T>(this IQueryable<T> query) where T : class, new()
        {
            DbContext context = BatchUtil.GetDbContext(query);
            string sql = BatchUtil.GetSqlDelete(query);
            return context.Database.ExecuteSqlCommand(sql);
        }

        public static int BatchUpdate<T>(this IQueryable<T> query, T updateValues, List<string> updateColumns = null) where T : class, new()
        {
            DbContext context = BatchUtil.GetDbContext(query);
            List<SqlParameter> parameters = new List<SqlParameter>();
            string sql = BatchUtil.GetSqlUpdate(query, context, updateValues, updateColumns, parameters);
            return context.Database.ExecuteSqlCommand(sql, parameters.ToArray());
        }


        public static int BatchUpdate<T>(this IQueryable<T> query, Expression<Func<T, bool>> updateExpression) where T : class, new()
        {
            var context = BatchUtil.GetDbContext(query);
            var (sql, sp) = BatchUtil.GetSqlUpdate(query, updateExpression);
            return  context.Database.ExecuteSqlCommand(sql, sp);
        }

        // Async methods

        public static async Task<int> BatchDeleteAsync<T>(this IQueryable<T> query) where T : class, new()
        {
            DbContext context = BatchUtil.GetDbContext(query);
            string sql = BatchUtil.GetSqlDelete(query);
            return await context.Database.ExecuteSqlCommandAsync(sql);
        }

        public static async Task<int> BatchUpdateAsync<T>(this IQueryable<T> query, T updateValues, List<string> updateColumns = null) where T : class, new()
        {
            DbContext context = BatchUtil.GetDbContext(query);
            List<SqlParameter> parameters = new List<SqlParameter>();
            string sql = BatchUtil.GetSqlUpdate(query, context, updateValues, updateColumns, parameters);
            return await context.Database.ExecuteSqlCommandAsync(sql, parameters.ToArray());
        }

        public static async Task<int> BatchUpdateAsync<T>(this IQueryable<T> query, Expression<Func<T, bool>> updateExpression) where T : class, new()
        {
            var context = BatchUtil.GetDbContext(query);
            var (sql, sp) = BatchUtil.GetSqlUpdate(query, updateExpression);
            return await context.Database.ExecuteSqlCommandAsync(sql, sp);
        }
    }
}
