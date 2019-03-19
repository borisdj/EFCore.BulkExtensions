using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.EntityFrameworkCore;

namespace EFCore.BulkExtensions
{
    public static class DbContextBulkExtensions
    {
        public static void BulkInsert<T>(this DbContext context, IList<T> entities, BulkConfig bulkConfig = null, Action<decimal> progress = null) where T : class
        {
            DbContextBulkTransaction.Execute(context, entities, OperationType.Insert, bulkConfig, progress);
        }

        public static void BulkInsert<T>(this DbContext context, IList<T> entities, Action<BulkConfig> bulkAction, Action<decimal> progress = null) where T : class
        {
            BulkConfig bulkConfig = new BulkConfig();
            bulkAction?.Invoke(bulkConfig);
            DbContextBulkTransaction.Execute(context, entities, OperationType.Insert, bulkConfig, progress);
        }

        public static void BulkInsertOrUpdate<T>(this DbContext context, IList<T> entities, BulkConfig bulkConfig = null, Action<decimal> progress = null) where T : class
        {
            DbContextBulkTransaction.Execute(context, entities, OperationType.InsertOrUpdate, bulkConfig, progress);
        }

        public static void BulkInsertOrUpdate<T>(this DbContext context, IList<T> entities, Action<BulkConfig> bulkAction, Action<decimal> progress = null) where T : class
        {
            BulkConfig bulkConfig = new BulkConfig();
            bulkAction?.Invoke(bulkConfig);
            DbContextBulkTransaction.Execute(context, entities, OperationType.InsertOrUpdate, bulkConfig, progress);
        }

        public static void BulkInsertOrUpdateOrDelete<T>(this DbContext context, IList<T> entities, BulkConfig bulkConfig = null, Action<decimal> progress = null) where T : class
        {
            DbContextBulkTransaction.Execute(context, entities, OperationType.InsertOrUpdateDelete, bulkConfig, progress);
        }

        public static void BulkInsertOrUpdateOrDelete<T>(this DbContext context, IList<T> entities, Action<BulkConfig> bulkAction, Action<decimal> progress = null) where T : class
        {
            BulkConfig bulkConfig = new BulkConfig();
            bulkAction?.Invoke(bulkConfig);
            DbContextBulkTransaction.Execute(context, entities, OperationType.InsertOrUpdateDelete, bulkConfig, progress);
        }

        public static void BulkUpdate<T>(this DbContext context, IList<T> entities, BulkConfig bulkConfig = null, Action<decimal> progress = null) where T : class
        {
            DbContextBulkTransaction.Execute(context, entities, OperationType.Update, bulkConfig, progress);
        }

        public static void BulkUpdate<T>(this DbContext context, IList<T> entities, Action<BulkConfig> bulkAction, Action<decimal> progress = null) where T : class
        {
            BulkConfig bulkConfig = new BulkConfig();
            bulkAction?.Invoke(bulkConfig);
            DbContextBulkTransaction.Execute(context, entities, OperationType.Update, bulkConfig, progress);
        }

        public static void BulkDelete<T>(this DbContext context, IList<T> entities, BulkConfig bulkConfig = null, Action<decimal> progress = null) where T : class
        {
            DbContextBulkTransaction.Execute(context, entities, OperationType.Delete, bulkConfig, progress);
        }

        public static void BulkDelete<T>(this DbContext context, IList<T> entities, Action<BulkConfig> bulkAction, Action<decimal> progress = null) where T : class
        {
            BulkConfig bulkConfig = new BulkConfig();
            bulkAction?.Invoke(bulkConfig);
            DbContextBulkTransaction.Execute(context, entities, OperationType.Delete, bulkConfig, progress);
        }

        public static void BulkRead<T>(this DbContext context, IList<T> entities, BulkConfig bulkConfig = null, Action<decimal> progress = null) where T : class
        {
            DbContextBulkTransaction.Execute(context, entities, OperationType.Read, bulkConfig, progress);
        }

        public static void BulkRead<T>(this DbContext context, IList<T> entities, Action<BulkConfig> bulkAction, Action<decimal> progress = null) where T : class
        {
            BulkConfig bulkConfig = new BulkConfig();
            bulkAction?.Invoke(bulkConfig);
            DbContextBulkTransaction.Execute(context, entities, OperationType.Read, bulkConfig, progress);
        }

        // Async methods

        public static Task BulkInsertAsync<T>(this DbContext context, IList<T> entities, BulkConfig bulkConfig = null, Action<decimal> progress = null) where T : class
        {
            return DbContextBulkTransaction.ExecuteAsync(context, entities, OperationType.Insert, bulkConfig, progress);
        }

        public static Task BulkInsertAsync<T>(this DbContext context, IList<T> entities, Action<BulkConfig> bulkAction, Action<decimal> progress = null) where T : class
        {
            BulkConfig bulkConfig = new BulkConfig();
            bulkAction?.Invoke(bulkConfig);
            return DbContextBulkTransaction.ExecuteAsync(context, entities, OperationType.Insert, bulkConfig, progress);
        }

        public static Task BulkInsertOrUpdateAsync<T>(this DbContext context, IList<T> entities, BulkConfig bulkConfig = null, Action<decimal> progress = null) where T : class
        {
            return DbContextBulkTransaction.ExecuteAsync(context, entities, OperationType.InsertOrUpdate, bulkConfig, progress);
        }

        /*public static Task BulkInsertOrUpdateAsync<T>(this DbContext context, IList<T> entities, Action<BulkConfig> bulkAction, Action<decimal> progress = null) where T : class
        {
            BulkConfig bulkConfig = new BulkConfig();
            bulkAction?.Invoke(bulkConfig);
            return DbContextBulkTransaction.ExecuteAsync(context, entities, OperationType.InsertOrUpdate, bulkConfig, progress);
        }*/

        public static Task BulkInsertOrUpdateOrDeleteAsync<T>(this DbContext context, IList<T> entities, BulkConfig bulkConfig = null, Action<decimal> progress = null) where T : class
        {
            return DbContextBulkTransaction.ExecuteAsync(context, entities, OperationType.InsertOrUpdateDelete, bulkConfig, progress);
        }

        public static Task BulkInsertOrUpdateOrDeleteAsync<T>(this DbContext context, IList<T> entities, Action<BulkConfig> bulkAction, Action<decimal> progress = null) where T : class
        {
            BulkConfig bulkConfig = new BulkConfig();
            bulkAction?.Invoke(bulkConfig);
            return DbContextBulkTransaction.ExecuteAsync(context, entities, OperationType.InsertOrUpdateDelete, bulkConfig, progress);
        }

        public static Task BulkUpdateAsync<T>(this DbContext context, IList<T> entities, BulkConfig bulkConfig = null, Action<decimal> progress = null) where T : class
        {
            return DbContextBulkTransaction.ExecuteAsync(context, entities, OperationType.Update, bulkConfig, progress);
        }

        public static Task BulkUpdateAsync<T>(this DbContext context, IList<T> entities, Action<BulkConfig> bulkAction, Action<decimal> progress = null) where T : class
        {
            BulkConfig bulkConfig = new BulkConfig();
            bulkAction?.Invoke(bulkConfig);
            return DbContextBulkTransaction.ExecuteAsync(context, entities, OperationType.Update, bulkConfig, progress);
        }

        public static Task BulkDeleteAsync<T>(this DbContext context, IList<T> entities, BulkConfig bulkConfig = null, Action<decimal> progress = null) where T : class
        {
            return DbContextBulkTransaction.ExecuteAsync(context, entities, OperationType.Delete, bulkConfig, progress);
        }

        public static Task BulkDeleteAsync<T>(this DbContext context, IList<T> entities, Action<BulkConfig> bulkAction, Action<decimal> progress = null) where T : class
        {
            BulkConfig bulkConfig = new BulkConfig();
            bulkAction?.Invoke(bulkConfig);
            return DbContextBulkTransaction.ExecuteAsync(context, entities, OperationType.Delete, bulkConfig, progress);
        }

        public static Task BulkReadAsync<T>(this DbContext context, IList<T> entities, BulkConfig bulkConfig = null, Action<decimal> progress = null) where T : class
        {
            return DbContextBulkTransaction.ExecuteAsync(context, entities, OperationType.Read, bulkConfig, progress);
        }

        public static Task BulkReadAsync<T>(this DbContext context, IList<T> entities, Action<BulkConfig> bulkAction, Action<decimal> progress = null) where T : class
        {
            BulkConfig bulkConfig = new BulkConfig();
            bulkAction?.Invoke(bulkConfig);
            return DbContextBulkTransaction.ExecuteAsync(context, entities, OperationType.Read, bulkConfig, progress);
        }
    }
}
