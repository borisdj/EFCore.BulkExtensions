using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.EntityFrameworkCore;

namespace EFCore.BulkExtensions
{
    public static class IQueryableBatchExtensions
    {
        public static void BatchDelete<T>(this IQueryable<T> query) where T : class, new()
        {
            DbContext context = BatchUtil.GetDbContext(query);
            string sql = BatchUtil.GetSqlDelete(query, context);
            context.Database.ExecuteSqlCommand(sql);
        }

        public static void BatchUpdate<T>(this IQueryable<T> query, T updateValues, List<string> updateColumns = null) where T : class, new()
        {
            DbContext context = BatchUtil.GetDbContext(query);
            string sql = BatchUtil.GetSqlUpdate(query, context, updateValues, updateColumns);
            context.Database.ExecuteSqlCommand(sql);
        }

        // Async methods

        public static async Task BatchDeleteAsync<T>(this IQueryable<T> query) where T : class, new()
        {
            DbContext context = BatchUtil.GetDbContext(query);
            string sql = BatchUtil.GetSqlDelete(query, context);
            await context.Database.ExecuteSqlCommandAsync(sql);
        }

        public static async Task BatchUpdateAsync<T>(this IQueryable<T> query, T updateValues, List<string> updateColumns = null) where T : class, new()
        {
            DbContext context = BatchUtil.GetDbContext(query);
            string sql = BatchUtil.GetSqlUpdate(query, context, updateValues, updateColumns);
            await context.Database.ExecuteSqlCommandAsync(sql);
        }
    }
}