﻿using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.EntityFrameworkCore;

namespace EFCore.BulkExtensions
{
    internal static class DbContextBulkTransaction
    {
        public static void Execute<T>(DbContext context, IList<T> entities, OperationType operationType, BulkConfig bulkConfig, Action<decimal> progress, Dictionary<string, object> sqlParameter = null) where T : class
        {
            if (operationType != OperationType.Truncate && entities.Count == 0)
            {
                return;
            }

            if (sqlParameter != null && sqlParameter.Count > 1)
            {
                throw new  SqlProviderNotSupportedException("sql parameter - only one allowed");
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
                SqlBulkOperation.Merge(context, entities, tableInfo, operationType, progress, sqlParameter);
            }
        }

        public static Task ExecuteAsync<T>(DbContext context, IList<T> entities, OperationType operationType, BulkConfig bulkConfig, Action<decimal> progress, CancellationToken cancellationToken, Dictionary<string, object> sqlParameter = null) where T : class
        {
            if (operationType != OperationType.Truncate && entities.Count == 0)
            {
                return Task.CompletedTask;
            }
            if (sqlParameter != null && sqlParameter.Count > 1)
            {
                throw new SqlProviderNotSupportedException("sql parameter - only one allowed");
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
                return SqlBulkOperation.TruncateAsync(context, tableInfo);
            }
            else
            {
                return SqlBulkOperation.MergeAsync(context, entities, tableInfo, operationType, progress, cancellationToken, sqlParameter);
            }
        }
    }
}
