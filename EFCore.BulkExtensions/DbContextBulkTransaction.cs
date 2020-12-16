using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.EntityFrameworkCore;

namespace EFCore.BulkExtensions
{
    internal static class DbContextBulkTransaction
    {
        public static void Execute<T>(DbContext context, IList<T> entities, OperationType operationType, BulkConfig bulkConfig, Action<decimal> progress) where T : class
        {
            if (operationType != OperationType.Truncate && entities.Count == 0)
            {
                return;
            }
            TableInfo tableInfo = TableInfo.CreateInstance(context, entities, operationType, bulkConfig);

            if (operationType == OperationType.Insert && !tableInfo.BulkConfig.SetOutputIdentity)
            {
                SqlBulkOperation.Insert(context, entities, tableInfo, progress);
            }
            else if (operationType == OperationType.Read)
            {
                SqlBulkOperation.Read(context, entities, tableInfo, progress);
            }
            else if (operationType == OperationType.Truncate)
            {
                SqlBulkOperation.Truncate(context, tableInfo);
            }
            else
            {
                SqlBulkOperation.Merge(context, entities, tableInfo, operationType, progress);
            }
        }

        public static void Execute(DbContext context, Type type, IList<object> entities, OperationType operationType, BulkConfig bulkConfig, Action<decimal> progress)
        {
            if (operationType != OperationType.Truncate && entities.Count == 0)
            {
                return;
            }
            TableInfo tableInfo = TableInfo.CreateInstance(context, type, entities, operationType, bulkConfig);

            if (operationType == OperationType.Insert && !tableInfo.BulkConfig.SetOutputIdentity)
            {
                SqlBulkOperation.Insert(context, type, entities, tableInfo, progress);
            }
            else if (operationType == OperationType.Read)
            {
                SqlBulkOperation.Read(context, type, entities, tableInfo, progress);
            }
            else if (operationType == OperationType.Truncate)
            {
                SqlBulkOperation.Truncate(context, tableInfo);
            }
            else
            {
                SqlBulkOperation.Merge(context, type, entities, tableInfo, operationType, progress);
            }
        }

        public static Task ExecuteAsync<T>(DbContext context, IList<T> entities, OperationType operationType, BulkConfig bulkConfig, Action<decimal> progress, CancellationToken cancellationToken) where T : class
        {
            if (operationType != OperationType.Truncate && entities.Count == 0)
            {
                return Task.CompletedTask;
            }
            TableInfo tableInfo = TableInfo.CreateInstance(context, entities, operationType, bulkConfig);

            if (operationType == OperationType.Insert && !tableInfo.BulkConfig.SetOutputIdentity)
            {
                return SqlBulkOperation.InsertAsync(context, entities, tableInfo, progress, cancellationToken);
            }
            else if (operationType == OperationType.Read)
            {
                return SqlBulkOperation.ReadAsync(context, entities, tableInfo, progress, cancellationToken);
            }
            else if (operationType == OperationType.Truncate)
            {
                return SqlBulkOperation.TruncateAsync(context, tableInfo, cancellationToken);
            }
            else
            {
                return SqlBulkOperation.MergeAsync(context, entities, tableInfo, operationType, progress, cancellationToken);
            }
        }

        public static Task ExecuteAsync(DbContext context, Type type, IList<object> entities, OperationType operationType, BulkConfig bulkConfig, Action<decimal> progress, CancellationToken cancellationToken)
        {
            if (operationType != OperationType.Truncate && entities.Count == 0)
            {
                return Task.CompletedTask;
            }
            TableInfo tableInfo = TableInfo.CreateInstance(context, type, entities, operationType, bulkConfig);

            if (operationType == OperationType.Insert && !tableInfo.BulkConfig.SetOutputIdentity)
            {
                return SqlBulkOperation.InsertAsync(context, type, entities, tableInfo, progress, cancellationToken);
            }
            else if (operationType == OperationType.Read)
            {
                return SqlBulkOperation.ReadAsync(context, type, entities, tableInfo, progress, cancellationToken);
            }
            else if (operationType == OperationType.Truncate)
            {
                return SqlBulkOperation.TruncateAsync(context, tableInfo, cancellationToken);
            }
            else
            {
                return SqlBulkOperation.MergeAsync(context, type, entities, tableInfo, operationType, progress, cancellationToken);
            }
        }
    }
}
