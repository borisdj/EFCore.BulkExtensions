using System.Collections.Generic;
using System.Linq;
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
            string sql = BatchUtil.GetSqlUpdate(query, context, updateValues, updateColumns);
            return context.Database.ExecuteSqlCommand(sql);
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
            string sql = BatchUtil.GetSqlUpdate(query, context, updateValues, updateColumns);
            return await context.Database.ExecuteSqlCommandAsync(sql);
        }
    }
}