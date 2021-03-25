using EFCore.BulkExtensions.SqlAdapters;
using Microsoft.EntityFrameworkCore;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace EFCore.BulkExtensions
{
    public enum OperationType
    {
        Insert,
        InsertOrUpdate,
        InsertOrUpdateDelete,
        Update,
        Delete,
        Read,
        Truncate
    }

    internal static class SqlBulkOperation
    {
        internal static string ColumnMappingExceptionMessage => "The given ColumnMapping does not match up with any column in the source or destination";

        public static void Insert<T>(DbContext context, Type type, IList<T> entities, TableInfo tableInfo, Action<decimal> progress)
        {
            var adapter = SqlAdaptersMapping.CreateBulkOperationsAdapter(context);
            adapter.Insert(context,type,entities,tableInfo,progress);
        }

        public static async Task InsertAsync<T>(DbContext context, Type type, IList<T> entities, TableInfo tableInfo, Action<decimal> progress, CancellationToken cancellationToken)
        {
            var adapter = SqlAdaptersMapping.CreateBulkOperationsAdapter(context);
            await adapter.InsertAsync(context,type,entities,tableInfo,progress,cancellationToken);
        }

        public static void Merge<T>(DbContext context, Type type, IList<T> entities, TableInfo tableInfo, OperationType operationType, Action<decimal> progress) where T : class
        {
            var adapter = SqlAdaptersMapping.CreateBulkOperationsAdapter(context);
            adapter.Merge(context,type,entities,tableInfo,operationType,progress);
        }

        public static async Task MergeAsync<T>(DbContext context, Type type, IList<T> entities, TableInfo tableInfo, OperationType operationType, Action<decimal> progress, CancellationToken cancellationToken) where T : class
        {
            var adapter = SqlAdaptersMapping.CreateBulkOperationsAdapter(context);
            await adapter.MergeAsync(context,type,entities,tableInfo,operationType, progress,cancellationToken);
        }

        public static void Read<T>(DbContext context, Type type, IList<T> entities, TableInfo tableInfo, Action<decimal> progress) where T : class
        {
            var dropTempTableIfExists = tableInfo.BulkConfig.UseTempDB;

            if (dropTempTableIfExists)
            {
                context.Database.ExecuteSqlRaw(SqlQueryBuilder.DropTable(tableInfo.FullTempTableName, tableInfo.BulkConfig.UseTempDB));
            }

            var adapter = SqlAdaptersMapping.CreateBulkOperationsAdapter(context);
            adapter.Read(context,type,entities,tableInfo,progress);
        }

        public static async Task ReadAsync<T>(DbContext context, Type type, IList<T> entities, TableInfo tableInfo, Action<decimal> progress, CancellationToken cancellationToken) where T : class
        {
           
            var dropTempTableIfExists = tableInfo.BulkConfig.UseTempDB;

            if (dropTempTableIfExists)
            {
                await context.Database.ExecuteSqlRawAsync(SqlQueryBuilder.DropTable(tableInfo.FullTempTableName, tableInfo.BulkConfig.UseTempDB), cancellationToken).ConfigureAwait(false);
            }

            var adapter = SqlAdaptersMapping.CreateBulkOperationsAdapter(context);
            await adapter.ReadAsync(context,type,entities,tableInfo,progress,cancellationToken);
        }

        public static void Truncate(DbContext context, TableInfo tableInfo)
        {
            var adapter = SqlAdaptersMapping.CreateBulkOperationsAdapter(context);
            adapter.Truncate(context,tableInfo);
        }

        public static async Task TruncateAsync(DbContext context, TableInfo tableInfo, CancellationToken cancellationToken)
        {
            var adapter = SqlAdaptersMapping.CreateBulkOperationsAdapter(context);
            await adapter.TruncateAsync(context,tableInfo);
        }
    }
}
