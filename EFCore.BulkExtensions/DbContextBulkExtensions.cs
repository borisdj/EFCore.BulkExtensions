using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.EntityFrameworkCore;

namespace EFCore.BulkExtensions
{
    public static class DbContextBulkExtensions
    {
        // Insert methods
        #region BulkInsert
        public static void BulkInsert<T>(this DbContext context, IList<T> entities, BulkConfig bulkConfig = null, Action<decimal> progress = null) where T : class
        {
            DbContextBulkTransaction.Execute(context, entities, OperationType.Insert, bulkConfig, progress);
        }
        public static Task BulkInsertAsync<T>(this DbContext context, IList<T> entities, BulkConfig bulkConfig = null, Action<decimal> progress = null, CancellationToken cancellationToken = default) where T : class
        {
            return DbContextBulkTransaction.ExecuteAsync(context, entities, OperationType.Insert, bulkConfig, progress, cancellationToken);
        }

        public static void BulkInsert<T>(this DbContext context, IList<T> entities, Action<BulkConfig> bulkAction, Action<decimal> progress = null) where T : class
        {
            BulkConfig bulkConfig = new BulkConfig();
            bulkAction?.Invoke(bulkConfig);
            DbContextBulkTransaction.Execute(context, entities, OperationType.Insert, bulkConfig, progress);
        }
        public static Task BulkInsertAsync<T>(this DbContext context, IList<T> entities, Action<BulkConfig> bulkAction, Action<decimal> progress = null, CancellationToken cancellationToken = default) where T : class
        {
            BulkConfig bulkConfig = new BulkConfig();
            bulkAction?.Invoke(bulkConfig);
            return DbContextBulkTransaction.ExecuteAsync(context, entities, OperationType.Insert, bulkConfig, progress, cancellationToken);
        }

        public static void BulkInsert(this DbContext context, Type entityType, IList<object> entities, BulkConfig bulkConfig = null, Action<decimal> progress = null)
        {
            DbContextBulkTransaction.Execute(context, entityType, entities, OperationType.Insert, bulkConfig, progress);
        }
        public static Task BulkInsertAsync(this DbContext context, Type entityType, IList<object> entities, BulkConfig bulkConfig = null, Action<decimal> progress = null, CancellationToken cancellationToken = default)
        {
            return DbContextBulkTransaction.ExecuteAsync(context, entityType, entities, OperationType.Insert, bulkConfig, progress, cancellationToken);
        }
        
        public static void BulkInsert(this DbContext context, Type entityType, IList<object> entities, Action<BulkConfig> bulkAction, Action<decimal> progress = null)
        {
            BulkConfig bulkConfig = new BulkConfig();
            bulkAction?.Invoke(bulkConfig);
            DbContextBulkTransaction.Execute(context, entityType, entities, OperationType.Insert, bulkConfig, progress);
        }
        public static Task BulkInsertAsync(this DbContext context, Type entityType, IList<object> entities, Action<BulkConfig> bulkAction, Action<decimal> progress = null, CancellationToken cancellationToken = default)
        {
            BulkConfig bulkConfig = new BulkConfig();
            bulkAction?.Invoke(bulkConfig);
            return DbContextBulkTransaction.ExecuteAsync(context, entityType, entities, OperationType.Insert, bulkConfig, progress, cancellationToken);
        }
        #endregion

        // InsertOrUpdate methods
        #region BulkInsertOrUpdate
        public static void BulkInsertOrUpdate<T>(this DbContext context, IList<T> entities, BulkConfig bulkConfig = null, Action<decimal> progress = null) where T : class
        {
            DbContextBulkTransaction.Execute(context, entities, OperationType.InsertOrUpdate, bulkConfig, progress);
        }
        public static Task BulkInsertOrUpdateAsync<T>(this DbContext context, IList<T> entities, BulkConfig bulkConfig = null, Action<decimal> progress = null, CancellationToken cancellationToken = default) where T : class
        {
            return DbContextBulkTransaction.ExecuteAsync(context, entities, OperationType.InsertOrUpdate, bulkConfig, progress, cancellationToken);
        }

        public static void BulkInsertOrUpdate<T>(this DbContext context, IList<T> entities, Action<BulkConfig> bulkAction, Action<decimal> progress = null) where T : class
        {
            BulkConfig bulkConfig = new BulkConfig();
            bulkAction?.Invoke(bulkConfig);
            DbContextBulkTransaction.Execute(context, entities, OperationType.InsertOrUpdate, bulkConfig, progress);
        }
        public static Task BulkInsertOrUpdateAsync<T>(this DbContext context, IList<T> entities, Action<BulkConfig> bulkAction, Action<decimal> progress = null, CancellationToken cancellationToken = default) where T : class
        {
            BulkConfig bulkConfig = new BulkConfig();
            bulkAction?.Invoke(bulkConfig);
            return DbContextBulkTransaction.ExecuteAsync(context, entities, OperationType.InsertOrUpdate, bulkConfig, progress, cancellationToken);
        }

        public static void BulkInsertOrUpdate(this DbContext context, Type entityType, IList<object> entities, BulkConfig bulkConfig = null, Action<decimal> progress = null)
        {
            DbContextBulkTransaction.Execute(context, entityType, entities, OperationType.InsertOrUpdate, bulkConfig, progress);
        }
        public static Task BulkInsertOrUpdateAsync(this DbContext context, Type entityType, IList<object> entities, BulkConfig bulkConfig = null, Action<decimal> progress = null, CancellationToken cancellationToken = default)
        {
            return DbContextBulkTransaction.ExecuteAsync(context, entityType, entities, OperationType.InsertOrUpdate, bulkConfig, progress, cancellationToken);
        }

        public static void BulkInsertOrUpdate(this DbContext context, Type entityType, IList<object> entities, Action<BulkConfig> bulkAction, Action<decimal> progress = null)
        {
            BulkConfig bulkConfig = new BulkConfig();
            bulkAction?.Invoke(bulkConfig);
            DbContextBulkTransaction.Execute(context, entityType, entities, OperationType.InsertOrUpdate, bulkConfig, progress);
        }

        public static Task BulkInsertOrUpdateAsync(this DbContext context, Type entityType, IList<object> entities, Action<BulkConfig> bulkAction, Action<decimal> progress = null, CancellationToken cancellationToken = default)
        {
            BulkConfig bulkConfig = new BulkConfig();
            bulkAction?.Invoke(bulkConfig);
            return DbContextBulkTransaction.ExecuteAsync(context, entityType, entities, OperationType.InsertOrUpdate, bulkConfig, progress, cancellationToken);
        }
        #endregion

        // InsertOrUpdateOrDelete methods
        #region BulkInsertOrUpdateOrDelete
        public static void BulkInsertOrUpdateOrDelete<T>(this DbContext context, IList<T> entities, BulkConfig bulkConfig = null, Action<decimal> progress = null) where T : class
        {
            DbContextBulkTransaction.Execute(context, entities, OperationType.InsertOrUpdateDelete, bulkConfig, progress);
        }
        public static Task BulkInsertOrUpdateOrDeleteAsync<T>(this DbContext context, IList<T> entities, BulkConfig bulkConfig = null, Action<decimal> progress = null, CancellationToken cancellationToken = default) where T : class
        {
            return DbContextBulkTransaction.ExecuteAsync(context, entities, OperationType.InsertOrUpdateDelete, bulkConfig, progress, cancellationToken);
        }
        public static void BulkInsertOrUpdateOrDelete<T>(this DbContext context, IList<T> entities, Action<BulkConfig> bulkAction, Action<decimal> progress = null) where T : class
        {
            BulkConfig bulkConfig = new BulkConfig();
            bulkAction?.Invoke(bulkConfig);
            DbContextBulkTransaction.Execute(context, entities, OperationType.InsertOrUpdateDelete, bulkConfig, progress);
        }
        public static Task BulkInsertOrUpdateOrDeleteAsync<T>(this DbContext context, IList<T> entities, Action<BulkConfig> bulkAction, Action<decimal> progress = null, CancellationToken cancellationToken = default) where T : class
        {
            BulkConfig bulkConfig = new BulkConfig();
            bulkAction?.Invoke(bulkConfig);
            return DbContextBulkTransaction.ExecuteAsync(context, entities, OperationType.InsertOrUpdateDelete, bulkConfig, progress, cancellationToken);
        }

        public static void BulkInsertOrUpdateOrDelete(this DbContext context, Type entityType, IList<object> entities, BulkConfig bulkConfig = null, Action<decimal> progress = null)
        {
            DbContextBulkTransaction.Execute(context, entityType, entities, OperationType.InsertOrUpdateDelete, bulkConfig, progress);
        }
        public static Task BulkInsertOrUpdateOrDeleteAsync(this DbContext context, Type entityType, IList<object> entities, BulkConfig bulkConfig = null, Action<decimal> progress = null, CancellationToken cancellationToken = default)
        {
            return DbContextBulkTransaction.ExecuteAsync(context, entityType, entities, OperationType.InsertOrUpdateDelete, bulkConfig, progress, cancellationToken);
        }

        public static void BulkInsertOrUpdateOrDelete(this DbContext context, Type entityType, IList<object> entities, Action<BulkConfig> bulkAction, Action<decimal> progress = null)
        {
            BulkConfig bulkConfig = new BulkConfig();
            bulkAction?.Invoke(bulkConfig);
            DbContextBulkTransaction.Execute(context, entityType, entities, OperationType.InsertOrUpdateDelete, bulkConfig, progress);
        }
        public static Task BulkInsertOrUpdateOrDeleteAsync(this DbContext context, Type entityType, IList<object> entities, Action<BulkConfig> bulkAction, Action<decimal> progress = null, CancellationToken cancellationToken = default)
        {
            BulkConfig bulkConfig = new BulkConfig();
            bulkAction?.Invoke(bulkConfig);
            return DbContextBulkTransaction.ExecuteAsync(context, entityType, entities, OperationType.InsertOrUpdateDelete, bulkConfig, progress, cancellationToken);
        }
        #endregion

        // Update methods
        #region BulkUpdate
        public static void BulkUpdate<T>(this DbContext context, IList<T> entities, BulkConfig bulkConfig = null, Action<decimal> progress = null) where T : class
        {
            DbContextBulkTransaction.Execute(context, entities, OperationType.Update, bulkConfig, progress);
        }
        public static Task BulkUpdateAsync<T>(this DbContext context, IList<T> entities, BulkConfig bulkConfig = null, Action<decimal> progress = null, CancellationToken cancellationToken = default) where T : class
        {
            return DbContextBulkTransaction.ExecuteAsync(context, entities, OperationType.Update, bulkConfig, progress, cancellationToken);
        }
        public static void BulkUpdate<T>(this DbContext context, IList<T> entities, Action<BulkConfig> bulkAction, Action<decimal> progress = null) where T : class
        {
            BulkConfig bulkConfig = new BulkConfig();
            bulkAction?.Invoke(bulkConfig);
            DbContextBulkTransaction.Execute(context, entities, OperationType.Update, bulkConfig, progress);
        }
        public static Task BulkUpdateAsync<T>(this DbContext context, IList<T> entities, Action<BulkConfig> bulkAction, Action<decimal> progress = null, CancellationToken cancellationToken = default) where T : class
        {
            BulkConfig bulkConfig = new BulkConfig();
            bulkAction?.Invoke(bulkConfig);
            return DbContextBulkTransaction.ExecuteAsync(context, entities, OperationType.Update, bulkConfig, progress, cancellationToken);
        }

        public static void BulkUpdate(this DbContext context, Type entityType, IList<object> entities, BulkConfig bulkConfig = null, Action<decimal> progress = null)
        {
            DbContextBulkTransaction.Execute(context, entityType, entities, OperationType.Update, bulkConfig, progress);
        }
        public static Task BulkUpdateAsync(this DbContext context, Type entityType, IList<object> entities, BulkConfig bulkConfig = null, Action<decimal> progress = null, CancellationToken cancellationToken = default)
        {
            return DbContextBulkTransaction.ExecuteAsync(context, entityType, entities, OperationType.Update, bulkConfig, progress, cancellationToken);
        }

        public static void BulkUpdate(this DbContext context, Type entityType, IList<object> entities, Action<BulkConfig> bulkAction, Action<decimal> progress = null)
        {
            BulkConfig bulkConfig = new BulkConfig();
            bulkAction?.Invoke(bulkConfig);
            DbContextBulkTransaction.Execute(context, entityType, entities, OperationType.Update, bulkConfig, progress);
        }
        public static Task BulkUpdateAsync(this DbContext context, Type entityType, IList<object> entities, Action<BulkConfig> bulkAction, Action<decimal> progress = null, CancellationToken cancellationToken = default)
        {
            BulkConfig bulkConfig = new BulkConfig();
            bulkAction?.Invoke(bulkConfig);
            return DbContextBulkTransaction.ExecuteAsync(context, entityType, entities, OperationType.Update, bulkConfig, progress, cancellationToken);
        }
        #endregion

        // Delete methods
        #region BulkDelete
        public static void BulkDelete<T>(this DbContext context, IList<T> entities, BulkConfig bulkConfig = null, Action<decimal> progress = null) where T : class
        {
            DbContextBulkTransaction.Execute(context, entities, OperationType.Delete, bulkConfig, progress);
        }
        public static Task BulkDeleteAsync<T>(this DbContext context, IList<T> entities, BulkConfig bulkConfig = null, Action<decimal> progress = null, CancellationToken cancellationToken = default) where T : class
        {
            return DbContextBulkTransaction.ExecuteAsync(context, entities, OperationType.Delete, bulkConfig, progress, cancellationToken);
        }

        public static void BulkDelete<T>(this DbContext context, IList<T> entities, Action<BulkConfig> bulkAction, Action<decimal> progress = null) where T : class
        {
            BulkConfig bulkConfig = new BulkConfig();
            bulkAction?.Invoke(bulkConfig);
            DbContextBulkTransaction.Execute(context, entities, OperationType.Delete, bulkConfig, progress);
        }
        public static Task BulkDeleteAsync<T>(this DbContext context, IList<T> entities, Action<BulkConfig> bulkAction, Action<decimal> progress = null, CancellationToken cancellationToken = default) where T : class
        {
            BulkConfig bulkConfig = new BulkConfig();
            bulkAction?.Invoke(bulkConfig);
            return DbContextBulkTransaction.ExecuteAsync(context, entities, OperationType.Delete, bulkConfig, progress, cancellationToken);
        }

        public static void BulkDelete(this DbContext context, Type entityType, IList<object> entities, BulkConfig bulkConfig = null, Action<decimal> progress = null)
        {
            DbContextBulkTransaction.Execute(context, entityType, entities, OperationType.Delete, bulkConfig, progress);
        }
        public static Task BulkDeleteAsync(this DbContext context, Type entityType, IList<object> entities, BulkConfig bulkConfig = null, Action<decimal> progress = null, CancellationToken cancellationToken = default)
        {
            return DbContextBulkTransaction.ExecuteAsync(context, entityType, entities, OperationType.Delete, bulkConfig, progress, cancellationToken);
        }

        public static void BulkDelete(this DbContext context, Type entityType, IList<object> entities, Action<BulkConfig> bulkAction, Action<decimal> progress = null)
        {
            BulkConfig bulkConfig = new BulkConfig();
            bulkAction?.Invoke(bulkConfig);
            DbContextBulkTransaction.Execute(context, entityType, entities, OperationType.Delete, bulkConfig, progress);
        }
        public static Task BulkDeleteAsync(this DbContext context, Type entityType, IList<object> entities, Action<BulkConfig> bulkAction, Action<decimal> progress = null, CancellationToken cancellationToken = default)
        {
            BulkConfig bulkConfig = new BulkConfig();
            bulkAction?.Invoke(bulkConfig);
            return DbContextBulkTransaction.ExecuteAsync(context, entityType, entities, OperationType.Delete, bulkConfig, progress, cancellationToken);
        }
        //--
        #endregion

        // Read methods
        #region BulkRead
        public static void BulkRead<T>(this DbContext context, IList<T> entities, BulkConfig bulkConfig = null, Action<decimal> progress = null) where T : class
        {
            DbContextBulkTransaction.Execute(context, entities, OperationType.Read, bulkConfig, progress);
        }
        public static Task BulkReadAsync<T>(this DbContext context, IList<T> entities, BulkConfig bulkConfig = null, Action<decimal> progress = null, CancellationToken cancellationToken = default) where T : class
        {
            return DbContextBulkTransaction.ExecuteAsync(context, entities, OperationType.Read, bulkConfig, progress, cancellationToken);
        }

        public static void BulkRead(this DbContext context, Type entityType, IList<object> entities, BulkConfig bulkConfig = null, Action<decimal> progress = null)
        {
            DbContextBulkTransaction.Execute(context, entityType, entities, OperationType.Read, bulkConfig, progress);
        }
        public static Task BulkReadAsync(this DbContext context, Type entityType, IList<object> entities, BulkConfig bulkConfig = null, Action<decimal> progress = null, CancellationToken cancellationToken = default)
        {
            return DbContextBulkTransaction.ExecuteAsync(context, entityType, entities, OperationType.Read, bulkConfig, progress, cancellationToken);
        }

        public static void BulkRead<T>(this DbContext context, IList<T> entities, Action<BulkConfig> bulkAction, Action<decimal> progress = null) where T : class
        {
            BulkConfig bulkConfig = new BulkConfig();
            bulkAction?.Invoke(bulkConfig);
            DbContextBulkTransaction.Execute(context, entities, OperationType.Read, bulkConfig, progress);
        }
        public static Task BulkReadAsync<T>(this DbContext context, IList<T> entities, Action<BulkConfig> bulkAction, Action<decimal> progress = null, CancellationToken cancellationToken = default) where T : class
        {
            BulkConfig bulkConfig = new BulkConfig();
            bulkAction?.Invoke(bulkConfig);
            return DbContextBulkTransaction.ExecuteAsync(context, entities, OperationType.Read, bulkConfig, progress, cancellationToken);
        }

        public static void BulkRead(this DbContext context, Type entityType, IList<object> entities, Action<BulkConfig> bulkAction, Action<decimal> progress = null)
        {
            BulkConfig bulkConfig = new BulkConfig();
            bulkAction?.Invoke(bulkConfig);
            DbContextBulkTransaction.Execute(context, entityType, entities, OperationType.Read, bulkConfig, progress);
        }
        public static Task BulkReadAsync(this DbContext context, Type entityType, IList<object> entities, Action<BulkConfig> bulkAction, Action<decimal> progress = null, CancellationToken cancellationToken = default)
        {
            BulkConfig bulkConfig = new BulkConfig();
            bulkAction?.Invoke(bulkConfig);
            return DbContextBulkTransaction.ExecuteAsync(context, entityType, entities, OperationType.Read, bulkConfig, progress, cancellationToken);
        }
        #endregion

        // Truncate
        #region Truncate
        public static void Truncate<T>(this DbContext context) where T : class
        {
            DbContextBulkTransaction.Execute<T>(context, new List<T>(), OperationType.Truncate, null, null);
        }
        public static Task TruncateAsync<T>(this DbContext context, CancellationToken cancellationToken = default) where T : class
        {
            return DbContextBulkTransaction.ExecuteAsync<T>(context, new List<T>(), OperationType.Truncate, null, null, cancellationToken);
        }

        public static void Truncate(this DbContext context, Type entityType)
        {
            DbContextBulkTransaction.Execute(context, entityType, new List<object>(), OperationType.Truncate, null, null);
        }
        public static Task TruncateAsync(this DbContext context, Type entityType, CancellationToken cancellationToken = default)
        {
            return DbContextBulkTransaction.ExecuteAsync(context, entityType, new List<object>(), OperationType.Truncate, null, null, cancellationToken);
        }
        #endregion
    }
}
