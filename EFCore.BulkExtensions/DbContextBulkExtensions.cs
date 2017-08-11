using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.EntityFrameworkCore;

namespace EFCore.BulkExtensions
{
    public static class DbContextBulkExtensions
    {
        public static void BulkInsert<T>(this DbContext context, IList<T> entities, BulkConfig bulkConfig = null, Action<decimal> progress = null) where T : class
        {
            DbContextBulkTransaction.Execute<T>(context, entities, OperationType.Insert,  bulkConfig, progress);
        }

        public static void BulkInsertOrUpdate<T>(this DbContext context, IList<T> entities, BulkConfig bulkConfig = null, Action<decimal> progress = null) where T : class
        {
            DbContextBulkTransaction.Execute<T>(context, entities, OperationType.InsertOrUpdate,  bulkConfig, progress);
        }

        public static void BulkUpdate<T>(this DbContext context, IList<T> entities, BulkConfig bulkConfig = null, Action<decimal> progress = null) where T : class
        {
            DbContextBulkTransaction.Execute<T>(context, entities, OperationType.Update,  bulkConfig, progress);
        }

        public static void BulkDelete<T>(this DbContext context, IList<T> entities, BulkConfig bulkConfig = null, Action<decimal> progress = null) where T : class
        {
            DbContextBulkTransaction.Execute<T>(context, entities, OperationType.Delete,  bulkConfig, progress);
        }

        // Async methods

        public static Task BulkInsertAsync<T>(this DbContext context, IList<T> entities, BulkConfig bulkConfig = null, Action<decimal> progress = null) where T : class
        {
            return DbContextBulkTransaction.ExecuteAsync<T>(context, entities, OperationType.Insert,  bulkConfig, progress);
        }

        public static Task BulkInsertOrUpdateAsync<T>(this DbContext context, IList<T> entities, BulkConfig bulkConfig = null, Action<decimal> progress = null) where T : class
        {
            return DbContextBulkTransaction.ExecuteAsync<T>(context, entities, OperationType.InsertOrUpdate,  bulkConfig, progress);
        }

        public static Task BulkUpdateAsync<T>(this DbContext context, IList<T> entities, BulkConfig bulkConfig = null, Action<decimal> progress = null) where T : class
        {
            return DbContextBulkTransaction.ExecuteAsync<T>(context, entities, OperationType.Update,  bulkConfig, progress);
        }

        public static Task BulkDeleteAsync<T>(this DbContext context, IList<T> entities, BulkConfig bulkConfig = null, Action<decimal> progress = null) where T : class
        {
            return DbContextBulkTransaction.ExecuteAsync<T>(context, entities, OperationType.Delete,  bulkConfig, progress);
        }
    }
}
