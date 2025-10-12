using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace EFCore.BulkExtensions;

internal static class DbContextBulkTransaction
{
    public static void Execute<T>(BulkContext context, Type type, IEnumerable<T> entities, OperationType operationType, BulkConfig? bulkConfig, Action<decimal>? progress) where T : class
    {
        using (ActivitySources.StartExecuteActivity(operationType, entities.Count()))
        {
            if (!IsValidTransaction(entities, operationType, bulkConfig)) return;

            if (operationType == OperationType.SaveChanges)
            {
                DbContextBulkTransactionSaveChanges.SaveChanges(context.DbContext, bulkConfig, progress);
                return;
            }

            if (bulkConfig?.IncludeGraph == true)
            {
                DbContextBulkTransactionGraphUtil.ExecuteWithGraph(context, entities, operationType, bulkConfig, progress);
                return;
            }

            var tableInfo = TableInfo.CreateInstance(context, type, entities, operationType, bulkConfig);

            switch (operationType)
            {
                case OperationType.Insert when tableInfo.BulkConfig is { SetOutputIdentity: false, CustomSourceTableName: null }:
                    SqlBulkOperation.Insert(context, type, entities, tableInfo, progress);
                    break;

                case OperationType.Read:
                    SqlBulkOperation.Read(context, type, entities, tableInfo, progress);
                    break;

                case OperationType.Truncate:
                    SqlBulkOperation.Truncate(context, tableInfo);
                    break;

                default:
                    SqlBulkOperation.Merge(context, type, entities, tableInfo, operationType, progress);
                    break;
            }
        }
    }

    public static async Task ExecuteAsync<T>(BulkContext context, Type type, IEnumerable<T> entities, OperationType operationType, BulkConfig? bulkConfig, Action<decimal>? progress, CancellationToken cancellationToken = default) where T : class
    {
        using (ActivitySources.StartExecuteActivity(operationType, entities.Count()))
        {
            if (!IsValidTransaction(entities, operationType, bulkConfig)) return;

            if (operationType == OperationType.SaveChanges)
            {
                await DbContextBulkTransactionSaveChanges.SaveChangesAsync(context.DbContext, bulkConfig, progress, cancellationToken).ConfigureAwait(false);
                return;
            }

            if (bulkConfig?.IncludeGraph == true)
            {
                await DbContextBulkTransactionGraphUtil.ExecuteWithGraphAsync(context, entities, operationType, bulkConfig, progress, cancellationToken).ConfigureAwait(false);
                return;
            }

            var tableInfo = TableInfo.CreateInstance(context, type, entities, operationType, bulkConfig);

            switch (operationType)
            {
                case OperationType.Insert when !tableInfo.BulkConfig.SetOutputIdentity:
                    await SqlBulkOperation.InsertAsync(context, type, entities, tableInfo, progress, cancellationToken).ConfigureAwait(false);
                    break;

                case OperationType.Read:
                    await SqlBulkOperation.ReadAsync(context, type, entities, tableInfo, progress, cancellationToken).ConfigureAwait(false);
                    break;

                case OperationType.Truncate:
                    await SqlBulkOperation.TruncateAsync(context, tableInfo, cancellationToken).ConfigureAwait(false);
                    break;

                default:
                    await SqlBulkOperation.MergeAsync(context, type, entities, tableInfo, operationType, progress, cancellationToken).ConfigureAwait(false);
                    break;
            }
        }
    }

    #region Transaction Validators
    private static bool IsValidTransaction<T>(IEnumerable<T> entities, OperationType operationType, BulkConfig? bulkConfig)
    {
        return entities.Any() ||
               operationType == OperationType.Truncate ||
               operationType == OperationType.SaveChanges ||
               operationType == OperationType.InsertOrUpdateOrDelete ||
               bulkConfig is { CustomSourceTableName: not null } ||
               bulkConfig is { DataReader: not null };
    }
    #endregion
}
