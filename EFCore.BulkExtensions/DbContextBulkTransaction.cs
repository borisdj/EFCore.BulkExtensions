using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;

namespace EFCore.BulkExtensions
{
    internal static partial class DbContextBulkTransaction
    {
        public static void Execute<T>(DbContext context, IList<T> entities, OperationType operationType, BulkConfig bulkConfig, Action<decimal> progress) where T : class
        {
            using (ActivitySources.StartExecuteActivity(operationType, entities.Count))
            {
                if (operationType != OperationType.Truncate && entities.Count == 0)
                {
                    return;
                }

                if (bulkConfig.IncludeGraph)
                {
                    DbContextBulkTransactionGraphUtil.ExecuteWithGraph(context, entities, operationType, bulkConfig, progress);
                }
                else
                {
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
            }
        }

        public static void Execute(DbContext context, Type type, IList<object> entities, OperationType operationType, BulkConfig bulkConfig, Action<decimal> progress)
        {
            using (ActivitySources.StartExecuteActivity(operationType, entities.Count))
            {
                if (operationType != OperationType.Truncate && entities.Count == 0)
                {
                    return;
                }

                if (bulkConfig.IncludeGraph)
                {
                    DbContextBulkTransactionGraphUtil.ExecuteWithGraph(context, entities, operationType, bulkConfig, progress);
                }
                else
                {
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
            }
        }

        public static async Task ExecuteAsync<T>(DbContext context, IList<T> entities, OperationType operationType, BulkConfig bulkConfig, Action<decimal> progress, CancellationToken cancellationToken) where T : class
        {
            using (ActivitySources.StartExecuteActivity(operationType, entities.Count))
            {
                if (operationType != OperationType.Truncate && entities.Count == 0)
                {
                    return;
                }

                if (bulkConfig.IncludeGraph)
                {
                    await DbContextBulkTransactionGraphUtil.ExecuteWithGraphAsync(context, entities, operationType, bulkConfig, progress, cancellationToken);
                }
                else
                {
                    TableInfo tableInfo = TableInfo.CreateInstance(context, entities, operationType, bulkConfig);

                    if (operationType == OperationType.Insert && !tableInfo.BulkConfig.SetOutputIdentity)
                    {
                        await SqlBulkOperation.InsertAsync(context, entities, tableInfo, progress, cancellationToken);
                    }
                    else if (operationType == OperationType.Read)
                    {
                        await SqlBulkOperation.ReadAsync(context, entities, tableInfo, progress, cancellationToken);
                    }
                    else if (operationType == OperationType.Truncate)
                    {
                        await SqlBulkOperation.TruncateAsync(context, tableInfo, cancellationToken);
                    }
                    else
                    {
                        await SqlBulkOperation.MergeAsync(context, entities, tableInfo, operationType, progress, cancellationToken);
                    }
                }
            }
        }

        public static async Task ExecuteAsync(DbContext context, Type type, IList<object> entities, OperationType operationType, BulkConfig bulkConfig, Action<decimal> progress, CancellationToken cancellationToken)
        {
            using (ActivitySources.StartExecuteActivity(operationType, entities.Count))
            {
                if (operationType != OperationType.Truncate && entities.Count == 0)
                {
                    return;
                }

                if (bulkConfig.IncludeGraph)
                {
                    await DbContextBulkTransactionGraphUtil.ExecuteWithGraphAsync(context, entities, operationType, bulkConfig, progress, cancellationToken);
                }
                else
                {
                    TableInfo tableInfo = TableInfo.CreateInstance(context, type, entities, operationType, bulkConfig);

                    if (operationType == OperationType.Insert && !tableInfo.BulkConfig.SetOutputIdentity)
                    {
                        await SqlBulkOperation.InsertAsync(context, type, entities, tableInfo, progress, cancellationToken);
                    }
                    else if (operationType == OperationType.Read)
                    {
                        await SqlBulkOperation.ReadAsync(context, type, entities, tableInfo, progress, cancellationToken);
                    }
                    else if (operationType == OperationType.Truncate)
                    {
                        await SqlBulkOperation.TruncateAsync(context, tableInfo, cancellationToken);
                    }
                    else
                    {
                        await SqlBulkOperation.MergeAsync(context, type, entities, tableInfo, operationType, progress, cancellationToken);
                    }
                }
            }
        }
    }
}
