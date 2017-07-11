using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.EntityFrameworkCore;

namespace EFCore.BulkExtensions
{
    public static class DbContextBulkExtensions
    {
        public static void BulkInsert<T>(this DbContext context, IList<T> entities, BulkConfig bulkConfig = null) where T : class
        {
            DbContextBulkTransaction.Execute<T>(context, entities, OperationType.Insert, bulkConfig);
        }

        public static void BulkInsertOrUpdate<T>(this DbContext context, IList<T> entities, BulkConfig bulkConfig = null) where T : class
        {
            DbContextBulkTransaction.Execute<T>(context, entities, OperationType.InsertOrUpdate, bulkConfig);
        }

        public static void BulkUpdate<T>(this DbContext context, IList<T> entities, BulkConfig bulkConfig = null) where T : class
        {
            DbContextBulkTransaction.Execute<T>(context, entities, OperationType.Update, bulkConfig);
        }

        public static void BulkDelete<T>(this DbContext context, IList<T> entities, BulkConfig bulkConfig = null) where T : class
        {
            DbContextBulkTransaction.Execute<T>(context, entities, OperationType.Delete, bulkConfig);
        }

        // Async methods

        public static async Task BulkInsertAsync<T>(this DbContext context, IList<T> entities, BulkConfig bulkConfig = null) where T : class
        {
            await DbContextBulkTransaction.ExecuteAsync<T>(context, entities, OperationType.Insert, bulkConfig);
        }

        public static async Task BulkInsertOrUpdateAsync<T>(this DbContext context, IList<T> entities, BulkConfig bulkConfig = null) where T : class
        {
            await DbContextBulkTransaction.ExecuteAsync<T>(context, entities, OperationType.InsertOrUpdate, bulkConfig);
        }

        public static async Task BulkUpdateAsync<T>(this DbContext context, IList<T> entities, BulkConfig bulkConfig = null) where T : class
        {
            await DbContextBulkTransaction.ExecuteAsync<T>(context, entities, OperationType.Update, bulkConfig);
        }

        public static async Task BulkDeleteAsync<T>(this DbContext context, IList<T> entities, BulkConfig bulkConfig = null) where T : class
        {
            await DbContextBulkTransaction.ExecuteAsync<T>(context, entities, OperationType.Delete, bulkConfig);
        }
    }
}
