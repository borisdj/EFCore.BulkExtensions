using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.EntityFrameworkCore;

namespace EFCore.BulkExtensions
{
    internal static class DbContextBulkTransaction
    {
        public static IEnumerable<OperationStats> Execute<T>(DbContext context, IList<T> entities, OperationType operationType, BulkConfig bulkConfig, Action<decimal> progress) where T : class
        {
            TableInfo tableInfo = TableInfo.CreateInstance(context, entities, operationType, bulkConfig);

            if (operationType == OperationType.Insert && !tableInfo.BulkConfig.SetOutputIdentity)
            {
                return SqlBulkOperation.Insert(context, entities, tableInfo, progress);
            }
            else
            {
                return SqlBulkOperation.Merge(context, entities, tableInfo, operationType, progress);
            }
        }

        public static Task<IEnumerable<OperationStats>> ExecuteAsync<T>(DbContext context, IList<T> entities, OperationType operationType, BulkConfig bulkConfig, Action<decimal> progress) where T : class
        {
            TableInfo tableInfo = TableInfo.CreateInstance(context, entities, operationType, bulkConfig);

            if (operationType == OperationType.Insert && !tableInfo.BulkConfig.SetOutputIdentity && !tableInfo.BulkConfig.GenerateStats)
            {
                return SqlBulkOperation.InsertAsync(context, entities, tableInfo, progress);
            }
            else
            {
                return SqlBulkOperation.MergeAsync(context, entities, tableInfo, operationType, progress);
            }
        }
    }
}
