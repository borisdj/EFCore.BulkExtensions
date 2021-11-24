using Microsoft.EntityFrameworkCore;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace EFCore.BulkExtensions
{
    public static class DbContextBulkExtensions
    {
        // Insert methods
        #region BulkInsert
        public static void BulkInsert<T>(this DbContext context, IList<T> entities, BulkConfig bulkConfig = null, Action<decimal> progress = null, Type type = null) where T : class
        {
            DbContextBulkTransaction.Execute(context, type, entities, OperationType.Insert, bulkConfig, progress);
        }
        public static Task BulkInsertAsync<T>(this DbContext context, IList<T> entities, BulkConfig bulkConfig = null, Action<decimal> progress = null, Type type = null, CancellationToken cancellationToken = default) where T : class
        {
            return DbContextBulkTransaction.ExecuteAsync(context, type, entities, OperationType.Insert, bulkConfig, progress, cancellationToken);
        }

        public static void BulkInsert<T>(this DbContext context, IList<T> entities, Action<BulkConfig> bulkAction, Action<decimal> progress = null, Type type = null) where T : class
        {
            BulkConfig bulkConfig = new BulkConfig();
            bulkAction?.Invoke(bulkConfig);
            DbContextBulkTransaction.Execute(context, type, entities, OperationType.Insert, bulkConfig, progress);
        }
        public static Task BulkInsertAsync<T>(this DbContext context, IList<T> entities, Action<BulkConfig> bulkAction, Action<decimal> progress = null, Type type = null, CancellationToken cancellationToken = default) where T : class
        {
            BulkConfig bulkConfig = new BulkConfig();
            bulkAction?.Invoke(bulkConfig);
            return DbContextBulkTransaction.ExecuteAsync(context, type, entities, OperationType.Insert, bulkConfig, progress, cancellationToken);
        }
        #endregion

        // InsertOrUpdate methods
        #region BulkInsertOrUpdate
        public static void BulkInsertOrUpdate<T>(this DbContext context, IList<T> entities, BulkConfig bulkConfig = null, Action<decimal> progress = null, Type type = null) where T : class
        {
            DbContextBulkTransaction.Execute(context, type, entities, OperationType.InsertOrUpdate, bulkConfig, progress);
        }
        public static Task BulkInsertOrUpdateAsync<T>(this DbContext context, IList<T> entities, BulkConfig bulkConfig = null, Action<decimal> progress = null, Type type = null, CancellationToken cancellationToken = default) where T : class
        {
            return DbContextBulkTransaction.ExecuteAsync(context, type, entities, OperationType.InsertOrUpdate, bulkConfig, progress, cancellationToken);
        }

        public static void BulkInsertOrUpdate<T>(this DbContext context, IList<T> entities, Action<BulkConfig> bulkAction, Action<decimal> progress = null, Type type = null) where T : class
        {
            BulkConfig bulkConfig = new BulkConfig();
            bulkAction?.Invoke(bulkConfig);
            DbContextBulkTransaction.Execute(context, type, entities, OperationType.InsertOrUpdate, bulkConfig, progress);
        }
        public static Task BulkInsertOrUpdateAsync<T>(this DbContext context, IList<T> entities, Action<BulkConfig> bulkAction, Action<decimal> progress = null, Type type = null, CancellationToken cancellationToken = default) where T : class
        {
            BulkConfig bulkConfig = new BulkConfig();
            bulkAction?.Invoke(bulkConfig);
            return DbContextBulkTransaction.ExecuteAsync(context, type, entities, OperationType.InsertOrUpdate, bulkConfig, progress, cancellationToken);
        }
        #endregion

        // InsertOrUpdateOrDelete methods
        #region BulkInsertOrUpdateOrDelete
        public static void BulkInsertOrUpdateOrDelete<T>(this DbContext context, IList<T> entities, BulkConfig bulkConfig = null, Action<decimal> progress = null, Type type = null) where T : class
        {
            DbContextBulkTransaction.Execute(context, type, entities, OperationType.InsertOrUpdateOrDelete, bulkConfig, progress);
        }
        public static Task BulkInsertOrUpdateOrDeleteAsync<T>(this DbContext context, IList<T> entities, BulkConfig bulkConfig = null, Action<decimal> progress = null, Type type = null, CancellationToken cancellationToken = default) where T : class
        {
            return DbContextBulkTransaction.ExecuteAsync(context, type, entities, OperationType.InsertOrUpdateOrDelete, bulkConfig, progress, cancellationToken);
        }
        public static void BulkInsertOrUpdateOrDelete<T>(this DbContext context, IList<T> entities, Action<BulkConfig> bulkAction, Action<decimal> progress = null, Type type = null) where T : class
        {
            BulkConfig bulkConfig = new BulkConfig();
            bulkAction?.Invoke(bulkConfig);
            DbContextBulkTransaction.Execute(context, type, entities, OperationType.InsertOrUpdateOrDelete, bulkConfig, progress);
        }
        public static Task BulkInsertOrUpdateOrDeleteAsync<T>(this DbContext context, IList<T> entities, Action<BulkConfig> bulkAction, Action<decimal> progress = null, Type type = null, CancellationToken cancellationToken = default) where T : class
        {
            BulkConfig bulkConfig = new BulkConfig();
            bulkAction?.Invoke(bulkConfig);
            return DbContextBulkTransaction.ExecuteAsync(context, type, entities, OperationType.InsertOrUpdateOrDelete, bulkConfig, progress, cancellationToken);
        }
        #endregion

        // Update methods
        #region BulkUpdate
        public static void BulkUpdate<T>(this DbContext context, IList<T> entities, BulkConfig bulkConfig = null, Action<decimal> progress = null, Type type = null) where T : class
        {
            DbContextBulkTransaction.Execute(context, type, entities, OperationType.Update, bulkConfig, progress);
        }
        public static Task BulkUpdateAsync<T>(this DbContext context, IList<T> entities, BulkConfig bulkConfig = null, Action<decimal> progress = null, Type type = null, CancellationToken cancellationToken = default) where T : class
        {
            return DbContextBulkTransaction.ExecuteAsync(context, type, entities, OperationType.Update, bulkConfig, progress, cancellationToken);
        }
        public static void BulkUpdate<T>(this DbContext context, IList<T> entities, Action<BulkConfig> bulkAction, Action<decimal> progress = null, Type type = null) where T : class
        {
            BulkConfig bulkConfig = new BulkConfig();
            bulkAction?.Invoke(bulkConfig);
            DbContextBulkTransaction.Execute(context, type, entities, OperationType.Update, bulkConfig, progress);
        }
        public static Task BulkUpdateAsync<T>(this DbContext context, IList<T> entities, Action<BulkConfig> bulkAction, Action<decimal> progress = null, Type type = null, CancellationToken cancellationToken = default) where T : class
        {
            BulkConfig bulkConfig = new BulkConfig();
            bulkAction?.Invoke(bulkConfig);
            return DbContextBulkTransaction.ExecuteAsync(context, type, entities, OperationType.Update, bulkConfig, progress, cancellationToken);
        }
        #endregion

        // Delete methods
        #region BulkDelete
        public static void BulkDelete<T>(this DbContext context, IList<T> entities, BulkConfig bulkConfig = null, Action<decimal> progress = null, Type type = null) where T : class
        {
            DbContextBulkTransaction.Execute(context, type, entities, OperationType.Delete, bulkConfig, progress);
        }
        public static Task BulkDeleteAsync<T>(this DbContext context, IList<T> entities, BulkConfig bulkConfig = null, Action<decimal> progress = null, Type type = null, CancellationToken cancellationToken = default) where T : class
        {
            return DbContextBulkTransaction.ExecuteAsync(context, type, entities, OperationType.Delete, bulkConfig, progress, cancellationToken);
        }

        public static void BulkDelete<T>(this DbContext context, IList<T> entities, Action<BulkConfig> bulkAction, Action<decimal> progress = null, Type type = null) where T : class
        {
            BulkConfig bulkConfig = new BulkConfig();
            bulkAction?.Invoke(bulkConfig);
            DbContextBulkTransaction.Execute(context, type, entities, OperationType.Delete, bulkConfig, progress);
        }
        public static Task BulkDeleteAsync<T>(this DbContext context, IList<T> entities, Action<BulkConfig> bulkAction, Action<decimal> progress = null, Type type = null, CancellationToken cancellationToken = default) where T : class
        {
            BulkConfig bulkConfig = new BulkConfig();
            bulkAction?.Invoke(bulkConfig);
            return DbContextBulkTransaction.ExecuteAsync(context, type, entities, OperationType.Delete, bulkConfig, progress, cancellationToken);
        }
        #endregion

        // Read methods
        #region BulkRead
        public static void BulkRead<T>(this DbContext context, IList<T> entities, BulkConfig bulkConfig = null, Action<decimal> progress = null, Type type = null) where T : class
        {
            DbContextBulkTransaction.Execute(context, type, entities, OperationType.Read, bulkConfig, progress);
        }
        public static Task BulkReadAsync<T>(this DbContext context, IList<T> entities, BulkConfig bulkConfig = null, Action<decimal> progress = null, Type type = null, CancellationToken cancellationToken = default) where T : class
        {
            return DbContextBulkTransaction.ExecuteAsync(context, type, entities, OperationType.Read, bulkConfig, progress, cancellationToken);
        }

        public static void BulkRead<T>(this DbContext context, IList<T> entities, Action<BulkConfig> bulkAction, Action<decimal> progress = null, Type type = null) where T : class
        {
            BulkConfig bulkConfig = new BulkConfig();
            bulkAction?.Invoke(bulkConfig);
            DbContextBulkTransaction.Execute(context, type, entities, OperationType.Read, bulkConfig, progress);
        }
        public static Task BulkReadAsync<T>(this DbContext context, IList<T> entities, Action<BulkConfig> bulkAction, Action<decimal> progress = null, Type type = null, CancellationToken cancellationToken = default) where T : class
        {
            BulkConfig bulkConfig = new BulkConfig();
            bulkAction?.Invoke(bulkConfig);
            return DbContextBulkTransaction.ExecuteAsync(context, type, entities, OperationType.Read, bulkConfig, progress, cancellationToken);
        }
        #endregion

        // Truncate methods
        #region Truncate
        public static void Truncate<T>(this DbContext context, Type type = null) where T : class
        {
            DbContextBulkTransaction.Execute(context, type, new List<T>(), OperationType.Truncate, null, null);
        }
        public static Task TruncateAsync<T>(this DbContext context, Type type = null, CancellationToken cancellationToken = default) where T : class
        {
            return DbContextBulkTransaction.ExecuteAsync(context, type, new List<T>(), OperationType.Truncate, null, null, cancellationToken);
        }
        #endregion
    }
}
