using Microsoft.EntityFrameworkCore;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace EFCore.BulkExtensions;

/// <summary>
/// Provides a list of bulk extension methods
/// </summary>
public static class DbContextBulkExtensions
{
    // Insert methods
    #region BulkInsert
    /// <summary>
    /// Extension method to bulk insert data
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <param name="dbContext"></param>
    /// <param name="entities"></param>
    /// <param name="bulkConfig"></param>
    /// <param name="progress"></param>
    /// <param name="type"></param>
    public static void BulkInsert<T>(this DbContext dbContext, IEnumerable<T> entities, BulkConfig? bulkConfig = null, Action<decimal>? progress = null, Type? type = null) where T : class
    {
        var context = BulkContext.Create(dbContext);
        DbContextBulkTransaction.Execute(context, type ?? typeof(T), entities, OperationType.Insert, bulkConfig, progress);
    }

    /// <summary>
    /// Extension method to bulk insert data
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <param name="dbContext"></param>
    /// <param name="entities"></param>
    /// <param name="bulkConfig"></param>
    /// <param name="progress"></param>
    /// <param name="type"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    public static Task BulkInsertAsync<T>(this DbContext dbContext, IEnumerable<T> entities, BulkConfig? bulkConfig = null, Action<decimal>? progress = null, Type? type = null, CancellationToken cancellationToken = default) where T : class
    {
        var context = BulkContext.Create(dbContext);
        return DbContextBulkTransaction.ExecuteAsync(context, type ?? typeof(T), entities, OperationType.Insert, bulkConfig, progress, cancellationToken);
    }

    /// <summary>
    /// Extension method to bulk insert data
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <param name="dbContext"></param>
    /// <param name="entities"></param>
    /// <param name="bulkAction"></param>
    /// <param name="progress"></param>
    /// <param name="type"></param>
    public static void BulkInsert<T>(this DbContext dbContext, IEnumerable<T> entities, Action<BulkConfig>? bulkAction, Action<decimal>? progress = null, Type? type = null) where T : class
    {
        var context = BulkContext.Create(dbContext);
        BulkConfig bulkConfig = new();
        bulkAction?.Invoke(bulkConfig);
        DbContextBulkTransaction.Execute(context, type ?? typeof(T), entities, OperationType.Insert, bulkConfig, progress);
    }

    /// <summary>
    /// Extension method to bulk insert data
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <param name="dbContext"></param>
    /// <param name="entities"></param>
    /// <param name="bulkAction"></param>
    /// <param name="progress"></param>
    /// <param name="type"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    public static Task BulkInsertAsync<T>(this DbContext dbContext, IEnumerable<T> entities, Action<BulkConfig>? bulkAction, Action<decimal>? progress = null, Type? type = null, CancellationToken cancellationToken = default) where T : class
    {
        var context = BulkContext.Create(dbContext);
        BulkConfig bulkConfig = new();
        bulkAction?.Invoke(bulkConfig);
        return DbContextBulkTransaction.ExecuteAsync(context, type ?? typeof(T), entities, OperationType.Insert, bulkConfig, progress, cancellationToken);
    }
    #endregion

    // InsertOrUpdate methods
    /// <summary>
    /// Extension method to bulk insert or update data
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <param name="dbContext"></param>
    /// <param name="entities"></param>
    /// <param name="bulkConfig"></param>
    /// <param name="progress"></param>
    /// <param name="type"></param>
    #region BulkInsertOrUpdate
    public static void BulkInsertOrUpdate<T>(this DbContext dbContext, IEnumerable<T> entities, BulkConfig? bulkConfig = null, Action<decimal>? progress = null, Type? type = null) where T : class
    {
        var context = BulkContext.Create(dbContext);
        DbContextBulkTransaction.Execute(context, type ?? typeof(T), entities, OperationType.InsertOrUpdate, bulkConfig, progress);
    }

    /// <summary>
    /// Extension method to bulk insert or update data
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <param name="dbContext"></param>
    /// <param name="entities"></param>
    /// <param name="bulkConfig"></param>
    /// <param name="progress"></param>
    /// <param name="type"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    public static Task BulkInsertOrUpdateAsync<T>(this DbContext dbContext, IEnumerable<T> entities, BulkConfig? bulkConfig = null, Action<decimal>? progress = null, Type? type = null, CancellationToken cancellationToken = default) where T : class
    {
        var context = BulkContext.Create(dbContext);
        return DbContextBulkTransaction.ExecuteAsync(context, type ?? typeof(T), entities, OperationType.InsertOrUpdate, bulkConfig, progress, cancellationToken);
    }

    /// <summary>
    /// Extension method to bulk insert or update data
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <param name="dbContext"></param>
    /// <param name="entities"></param>
    /// <param name="bulkAction"></param>
    /// <param name="progress"></param>
    /// <param name="type"></param>
    public static void BulkInsertOrUpdate<T>(this DbContext dbContext, IEnumerable<T> entities, Action<BulkConfig> bulkAction, Action<decimal>? progress = null, Type? type = null) where T : class
    {
        var context = BulkContext.Create(dbContext);
        BulkConfig bulkConfig = new();
        bulkAction?.Invoke(bulkConfig);
        DbContextBulkTransaction.Execute(context, type ?? typeof(T), entities, OperationType.InsertOrUpdate, bulkConfig, progress);
    }

    /// <summary>
    /// Extension method to bulk insert or update data
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <param name="dbContext"></param>
    /// <param name="entities"></param>
    /// <param name="bulkAction"></param>
    /// <param name="progress"></param>
    /// <param name="type"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    public static Task BulkInsertOrUpdateAsync<T>(this DbContext dbContext, IEnumerable<T> entities, Action<BulkConfig> bulkAction, Action<decimal>? progress = null, Type? type = null, CancellationToken cancellationToken = default) where T : class
    {
        var context = BulkContext.Create(dbContext);
        BulkConfig bulkConfig = new();
        bulkAction?.Invoke(bulkConfig);
        return DbContextBulkTransaction.ExecuteAsync(context, type ?? typeof(T), entities, OperationType.InsertOrUpdate, bulkConfig, progress, cancellationToken);
    }
    #endregion

    // InsertOrUpdateOrDelete methods
    #region BulkInsertOrUpdateOrDelete

    /// <summary>
    /// Extension method to bulk insert, update and delete data
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <param name="dbContext"></param>
    /// <param name="entities"></param>
    /// <param name="bulkConfig"></param>
    /// <param name="progress"></param>
    /// <param name="type"></param>
    public static void BulkInsertOrUpdateOrDelete<T>(this DbContext dbContext, IEnumerable<T> entities, BulkConfig? bulkConfig = null, Action<decimal>? progress = null, Type? type = null) where T : class
    {
        var context = BulkContext.Create(dbContext);
        DbContextBulkTransaction.Execute(context, type ?? typeof(T), entities, OperationType.InsertOrUpdateOrDelete, bulkConfig, progress);
    }

    /// <summary>
    /// Extension method to bulk insert, update and delete data
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <param name="dbContext"></param>
    /// <param name="entities"></param>
    /// <param name="bulkConfig"></param>
    /// <param name="progress"></param>
    /// <param name="type"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    public static Task BulkInsertOrUpdateOrDeleteAsync<T>(this DbContext dbContext, IEnumerable<T> entities, BulkConfig? bulkConfig = null, Action<decimal>? progress = null, Type? type = null, CancellationToken cancellationToken = default) where T : class
    {
        var context = BulkContext.Create(dbContext);
        return DbContextBulkTransaction.ExecuteAsync(context, type ?? typeof(T), entities, OperationType.InsertOrUpdateOrDelete, bulkConfig, progress, cancellationToken);
    }

    /// <summary>
    /// Extension method to bulk insert, update and delete data
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <param name="dbContext"></param>
    /// <param name="entities"></param>
    /// <param name="bulkAction"></param>
    /// <param name="progress"></param>
    /// <param name="type"></param>
    public static void BulkInsertOrUpdateOrDelete<T>(this DbContext dbContext, IEnumerable<T> entities, Action<BulkConfig>? bulkAction, Action<decimal>? progress = null, Type? type = null) where T : class
    {
        var context = BulkContext.Create(dbContext);
        BulkConfig bulkConfig = new();
        bulkAction?.Invoke(bulkConfig);
        DbContextBulkTransaction.Execute(context, type ?? typeof(T), entities, OperationType.InsertOrUpdateOrDelete, bulkConfig, progress);
    }

    /// <summary>
    /// Extension method to bulk insert, update and delete data
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <param name="dbContext"></param>
    /// <param name="entities"></param>
    /// <param name="bulkAction"></param>
    /// <param name="progress"></param>
    /// <param name="type"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    public static Task BulkInsertOrUpdateOrDeleteAsync<T>(this DbContext dbContext, IEnumerable<T> entities, Action<BulkConfig>? bulkAction, Action<decimal>? progress = null, Type? type = null, CancellationToken cancellationToken = default) where T : class
    {
        var context = BulkContext.Create(dbContext);
        BulkConfig bulkConfig = new();
        bulkAction?.Invoke(bulkConfig);
        return DbContextBulkTransaction.ExecuteAsync(context, type ?? typeof(T), entities, OperationType.InsertOrUpdateOrDelete, bulkConfig, progress, cancellationToken);
    }
    #endregion

    // Update methods
    #region BulkUpdate

    /// <summary>
    /// Extension method to bulk update data
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <param name="dbContext"></param>
    /// <param name="entities"></param>
    /// <param name="bulkConfig"></param>
    /// <param name="progress"></param>
    /// <param name="type"></param>
    public static void BulkUpdate<T>(this DbContext dbContext, IEnumerable<T> entities, BulkConfig? bulkConfig = null, Action<decimal>? progress = null, Type? type = null) where T : class
    {
        var context = BulkContext.Create(dbContext);
        DbContextBulkTransaction.Execute(context, type ?? typeof(T), entities, OperationType.Update, bulkConfig, progress);
    }

    /// <summary>
    /// Extension method to bulk update data
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <param name="dbContext"></param>
    /// <param name="entities"></param>
    /// <param name="bulkConfig"></param>
    /// <param name="progress"></param>
    /// <param name="type"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    public static Task BulkUpdateAsync<T>(this DbContext dbContext, IEnumerable<T> entities, BulkConfig? bulkConfig = null, Action<decimal>? progress = null, Type? type = null, CancellationToken cancellationToken = default) where T : class
    {
        var context = BulkContext.Create(dbContext);
        return DbContextBulkTransaction.ExecuteAsync(context, type ?? typeof(T), entities, OperationType.Update, bulkConfig, progress, cancellationToken);
    }

    /// <summary>
    /// Extension method to bulk update data
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <param name="dbContext"></param>
    /// <param name="entities"></param>
    /// <param name="bulkAction"></param>
    /// <param name="progress"></param>
    /// <param name="type"></param>
    public static void BulkUpdate<T>(this DbContext dbContext, IEnumerable<T> entities, Action<BulkConfig>? bulkAction, Action<decimal>? progress = null, Type? type = null) where T : class
    {
        var context = BulkContext.Create(dbContext);
        BulkConfig bulkConfig = new();
        bulkAction?.Invoke(bulkConfig);
        DbContextBulkTransaction.Execute(context, type ?? typeof(T), entities, OperationType.Update, bulkConfig, progress);
    }

    /// <summary>
    /// Extension method to bulk update data
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <param name="dbContext"></param>
    /// <param name="entities"></param>
    /// <param name="bulkAction"></param>
    /// <param name="progress"></param>
    /// <param name="type"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    public static Task BulkUpdateAsync<T>(this DbContext dbContext, IEnumerable<T> entities, Action<BulkConfig>? bulkAction, Action<decimal>? progress = null, Type? type = null, CancellationToken cancellationToken = default) where T : class
    {
        var context = BulkContext.Create(dbContext);
        BulkConfig bulkConfig = new();
        bulkAction?.Invoke(bulkConfig);
        return DbContextBulkTransaction.ExecuteAsync(context, type ?? typeof(T), entities, OperationType.Update, bulkConfig, progress, cancellationToken);
    }
    #endregion

    // Delete methods
    #region BulkDelete

    /// <summary>
    /// Extension method to bulk delete data
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <param name="dbContext"></param>
    /// <param name="entities"></param>
    /// <param name="bulkConfig"></param>
    /// <param name="progress"></param>
    /// <param name="type"></param>
    public static void BulkDelete<T>(this DbContext dbContext, IEnumerable<T> entities, BulkConfig? bulkConfig = null, Action<decimal>? progress = null, Type? type = null) where T : class
    {
        var context = BulkContext.Create(dbContext);
        DbContextBulkTransaction.Execute(context, type ?? typeof(T), entities, OperationType.Delete, bulkConfig, progress);
    }

    /// <summary>
    /// Extension method to bulk delete data
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <param name="dbContext"></param>
    /// <param name="entities"></param>
    /// <param name="bulkConfig"></param>
    /// <param name="progress"></param>
    /// <param name="type"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    public static Task BulkDeleteAsync<T>(this DbContext dbContext, IEnumerable<T> entities, BulkConfig? bulkConfig = null, Action<decimal>? progress = null, Type? type = null, CancellationToken cancellationToken = default) where T : class
    {
        var context = BulkContext.Create(dbContext);
        return DbContextBulkTransaction.ExecuteAsync(context, type ?? typeof(T), entities, OperationType.Delete, bulkConfig, progress, cancellationToken);
    }

    /// <summary>
    /// Extension method to bulk delete data
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <param name="dbContext"></param>
    /// <param name="entities"></param>
    /// <param name="bulkAction"></param>
    /// <param name="progress"></param>
    /// <param name="type"></param>
    public static void BulkDelete<T>(this DbContext dbContext, IEnumerable<T> entities, Action<BulkConfig>? bulkAction, Action<decimal>? progress = null, Type? type = null) where T : class
    {
        var context = BulkContext.Create(dbContext);
        BulkConfig bulkConfig = new();
        bulkAction?.Invoke(bulkConfig);
        DbContextBulkTransaction.Execute(context, type ?? typeof(T), entities, OperationType.Delete, bulkConfig, progress);
    }

    /// <summary>
    /// Extension method to bulk delete data
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <param name="dbContext"></param>
    /// <param name="entities"></param>
    /// <param name="bulkAction"></param>
    /// <param name="progress"></param>
    /// <param name="type"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    public static Task BulkDeleteAsync<T>(this DbContext dbContext, IEnumerable<T> entities, Action<BulkConfig>? bulkAction, Action<decimal>? progress = null, Type? type = null, CancellationToken cancellationToken = default) where T : class
    {
        var context = BulkContext.Create(dbContext);
        BulkConfig bulkConfig = new();
        bulkAction?.Invoke(bulkConfig);
        return DbContextBulkTransaction.ExecuteAsync(context, type ?? typeof(T), entities, OperationType.Delete, bulkConfig, progress, cancellationToken);
    }
    #endregion

    // Read methods
    #region BulkRead

    /// <summary>
    /// Extension method to bulk read data
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <param name="dbContext"></param>
    /// <param name="entities"></param>
    /// <param name="bulkConfig"></param>
    /// <param name="progress"></param>
    /// <param name="type"></param>
    public static void BulkRead<T>(this DbContext dbContext, IEnumerable<T> entities, BulkConfig? bulkConfig = null, Action<decimal>? progress = null, Type? type = null) where T : class
    {
        var context = BulkContext.Create(dbContext);
        DbContextBulkTransaction.Execute(context, type ?? typeof(T), entities, OperationType.Read, bulkConfig, progress);
    }

    /// <summary>
    /// Extension method to bulk read data
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <param name="dbContext"></param>
    /// <param name="entities"></param>
    /// <param name="bulkConfig"></param>
    /// <param name="progress"></param>
    /// <param name="type"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    public static Task BulkReadAsync<T>(this DbContext dbContext, IEnumerable<T> entities, BulkConfig? bulkConfig = null, Action<decimal>? progress = null, Type? type = null, CancellationToken cancellationToken = default) where T : class
    {
        var context = BulkContext.Create(dbContext);
        return DbContextBulkTransaction.ExecuteAsync(context, type ?? typeof(T), entities, OperationType.Read, bulkConfig, progress, cancellationToken);
    }

    /// <summary>
    /// Extension method to bulk read data
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <param name="dbContext"></param>
    /// <param name="entities"></param>
    /// <param name="bulkAction"></param>
    /// <param name="progress"></param>
    /// <param name="type"></param>
    public static void BulkRead<T>(this DbContext dbContext, IEnumerable<T> entities, Action<BulkConfig>? bulkAction, Action<decimal>? progress = null, Type? type = null) where T : class
    {
        var context = BulkContext.Create(dbContext);
        BulkConfig bulkConfig = new();
        bulkAction?.Invoke(bulkConfig);
        DbContextBulkTransaction.Execute(context, type ?? typeof(T), entities, OperationType.Read, bulkConfig, progress);
    }

    /// <summary>
    /// Extension method to bulk read data
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <param name="dbContext"></param>
    /// <param name="entities"></param>
    /// <param name="bulkAction"></param>
    /// <param name="progress"></param>
    /// <param name="type"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    public static Task BulkReadAsync<T>(this DbContext dbContext, IEnumerable<T> entities, Action<BulkConfig>? bulkAction, Action<decimal>? progress = null, Type? type = null, CancellationToken cancellationToken = default) where T : class
    {
        var context = BulkContext.Create(dbContext);
        BulkConfig bulkConfig = new();
        bulkAction?.Invoke(bulkConfig);
        return DbContextBulkTransaction.ExecuteAsync(context, type ?? typeof(T), entities, OperationType.Read, bulkConfig, progress, cancellationToken);
    }
    #endregion

    // Truncate methods
    #region Truncate

    /// <summary>
    /// Extension method to truncate table
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <param name="dbContext"></param>
    /// <param name="bulkConfig"></param>
    /// <param name="type"></param>
    public static void Truncate<T>(this DbContext dbContext, BulkConfig? bulkConfig = null, Type? type = null) where T : class
    {
        var context = BulkContext.Create(dbContext);
        DbContextBulkTransaction.Execute(context, type ?? typeof(T), new List<T>(), OperationType.Truncate, bulkConfig, null);
    }

    /// <summary>
    /// Extension method to truncate table
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <param name="dbContext"></param>
    /// <param name="type"></param>
    /// <param name="bulkConfig"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    public static Task TruncateAsync<T>(this DbContext dbContext, BulkConfig? bulkConfig = null, Type? type = null, CancellationToken cancellationToken = default) where T : class
    {
        var context = BulkContext.Create(dbContext);
        return DbContextBulkTransaction.ExecuteAsync(context, type ?? typeof(T), new List<T>(), OperationType.Truncate, bulkConfig, null, cancellationToken);
    }

    /// <summary>
    /// Extension method to truncate table
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <param name="dbContext"></param>
    /// <param name="bulkAction"></param>
    /// <param name="type"></param>
    public static void Truncate<T>(this DbContext dbContext, Action<BulkConfig>? bulkAction, Type? type = null) where T : class
    {
        var context = BulkContext.Create(dbContext);
        BulkConfig bulkConfig = new();
        bulkAction?.Invoke(bulkConfig);
        DbContextBulkTransaction.Execute(context, type ?? typeof(T), new List<T>(), OperationType.Truncate, bulkConfig, null);
    }

    /// <summary>
    /// Extension method to truncate table
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <param name="dbContext"></param>
    /// <param name="type"></param>
    /// <param name="bulkAction"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    public static Task TruncateAsync<T>(this DbContext dbContext, Action<BulkConfig>? bulkAction, Type? type = null, CancellationToken cancellationToken = default) where T : class
    {
        var context = BulkContext.Create(dbContext);
        BulkConfig bulkConfig = new();
        bulkAction?.Invoke(bulkConfig);
        return DbContextBulkTransaction.ExecuteAsync(context, type ?? typeof(T), new List<T>(), OperationType.Truncate, bulkConfig, null, cancellationToken);
    }
    #endregion

    // SaveChanges methods
    #region SaveChanges
    /// <summary>
    /// Extension method for EFCore SaveChanges
    /// </summary>
    /// <param name="dbContext"></param>
    /// <param name="bulkConfig"></param>
    /// <param name="progress"></param>
    public static void BulkSaveChanges(this DbContext dbContext, BulkConfig? bulkConfig = null, Action<decimal>? progress = null)
    {
        var context = BulkContext.Create(dbContext);
        DbContextBulkTransaction.Execute(context, typeof(object), new List<object>(), OperationType.SaveChanges, bulkConfig, progress);
    }

    /// <summary>
    /// Extension method for EFCore SaveChanges
    /// </summary>
    /// <param name="dbContext"></param>
    /// <param name="bulkConfig"></param>
    /// <param name="progress"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    public static Task BulkSaveChangesAsync(this DbContext dbContext, BulkConfig? bulkConfig = null, Action<decimal>? progress = null, CancellationToken cancellationToken = default)
    {
        var context = BulkContext.Create(dbContext);
        return DbContextBulkTransaction.ExecuteAsync(context, typeof(object), new List<object>(), OperationType.SaveChanges, bulkConfig, progress, cancellationToken);
    }
    #endregion
}
