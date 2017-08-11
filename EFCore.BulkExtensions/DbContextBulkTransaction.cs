using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.EntityFrameworkCore;

namespace EFCore.BulkExtensions
{
    internal static class DbContextBulkTransaction
    {
        public static void Execute<T>(DbContext context, IList<T> entities, OperationType operationType, BulkConfig bulkConfig, Action<decimal> progress) where T : class
        {
            TableInfo tableInfo = TableInfo.CreateInstance(context, entities, operationType, bulkConfig);

            if (operationType == OperationType.Insert && !tableInfo.BulkConfig.SetOutputIdentity)
            {
                SqlBulkOperation.Insert<T>(context, entities, tableInfo, progress);
            }
            else
            {
                SqlBulkOperation.Merge<T>(context, entities, tableInfo, operationType, progress);
            }
        }

        public static Task ExecuteAsync<T>(DbContext context, IList<T> entities, OperationType operationType, BulkConfig bulkConfig, Action<decimal> progress) where T : class
        {
            TableInfo tableInfo = TableInfo.CreateInstance(context, entities, operationType, bulkConfig);

            if (operationType == OperationType.Insert && !tableInfo.BulkConfig.SetOutputIdentity)
            {
                return SqlBulkOperation.InsertAsync<T>(context, entities, tableInfo, progress);
            }
            else
            {
                return SqlBulkOperation.MergeAsync<T>(context, entities, tableInfo, operationType, progress);
            }
        }
    }
}
