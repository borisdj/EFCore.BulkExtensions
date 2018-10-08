using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.EntityFrameworkCore;

namespace EFCore.BulkExtensions
{
    public static class DbContextBulkExtensions
    {
        public static IEnumerable<OperationStats> BulkInsert<T>(this DbContext context, IList<T> entities, BulkConfig bulkConfig = null, Action<decimal> progress = null) where T : class
        {
            return DbContextBulkTransaction.Execute(context, entities, OperationType.Insert, bulkConfig, progress);
        }

        public static IEnumerable<OperationStats> BulkInsertOrUpdate<T>(this DbContext context, IList<T> entities, BulkConfig bulkConfig = null, Action<decimal> progress = null) where T : class
        {
            return DbContextBulkTransaction.Execute(context, entities, OperationType.InsertOrUpdate, bulkConfig, progress);
        }

        public static IEnumerable<OperationStats> BulkUpdate<T>(this DbContext context, IList<T> entities, BulkConfig bulkConfig = null, Action<decimal> progress = null) where T : class
        {
            return DbContextBulkTransaction.Execute(context, entities, OperationType.Update, bulkConfig, progress);
        }

        public static IEnumerable<OperationStats> BulkDelete<T>(this DbContext context, IList<T> entities, BulkConfig bulkConfig = null, Action<decimal> progress = null) where T : class
        {
            return DbContextBulkTransaction.Execute(context, entities, OperationType.Delete, bulkConfig, progress);
        }

        // Async methods

        public static Task<IEnumerable<OperationStats>> BulkInsertAsync<T>(this DbContext context, IList<T> entities, BulkConfig bulkConfig = null, Action<decimal> progress = null) where T : class
        {
            return DbContextBulkTransaction.ExecuteAsync(context, entities, OperationType.Insert, bulkConfig, progress);
        }

        public static Task<IEnumerable<OperationStats>> BulkInsertOrUpdateAsync<T>(this DbContext context, IList<T> entities, BulkConfig bulkConfig = null, Action<decimal> progress = null) where T : class
        {
            return DbContextBulkTransaction.ExecuteAsync(context, entities, OperationType.InsertOrUpdate, bulkConfig, progress);
        }

        public static Task<IEnumerable<OperationStats>> BulkUpdateAsync<T>(this DbContext context, IList<T> entities, BulkConfig bulkConfig = null, Action<decimal> progress = null) where T : class
        {
            return DbContextBulkTransaction.ExecuteAsync(context, entities, OperationType.Update, bulkConfig, progress);
        }

        public static Task<IEnumerable<OperationStats>> BulkDeleteAsync<T>(this DbContext context, IList<T> entities, BulkConfig bulkConfig = null, Action<decimal> progress = null) where T : class
        {
            return DbContextBulkTransaction.ExecuteAsync(context, entities, OperationType.Delete, bulkConfig, progress);
        }
    }
}
