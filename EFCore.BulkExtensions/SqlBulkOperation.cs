using Microsoft.EntityFrameworkCore;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using EFCore.BulkExtensions.SqlAdapters;

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

        public static void Insert<T>(DbContext context, IList<T> entities, TableInfo tableInfo, Action<decimal> progress)
        {
            Insert<T>(context, typeof(T), entities, tableInfo, progress);
        }

        public static void Insert(DbContext context, Type type, IList<object> entities, TableInfo tableInfo, Action<decimal> progress)
        {
            Insert<object>(context, type, entities, tableInfo, progress);
        }

        private static void Insert<T>(DbContext context, Type type, IList<T> entities, TableInfo tableInfo, Action<decimal> progress)
        {
            var adapter = SqlAdaptersMapping.CreateBulkOperationsAdapter(context);
            adapter.Insert(context,type,entities,tableInfo,progress);
        }

        public static async Task InsertAsync<T>(DbContext context, IList<T> entities, TableInfo tableInfo, Action<decimal> progress, CancellationToken cancellationToken)
        {
            await InsertAsync<T>(context, typeof(T), entities, tableInfo, progress, cancellationToken).ConfigureAwait(false);
        }

        public static async Task InsertAsync(DbContext context, Type type, IList<object> entities, TableInfo tableInfo, Action<decimal> progress, CancellationToken cancellationToken)
        {
            await InsertAsync<object>(context, type, entities, tableInfo, progress, cancellationToken).ConfigureAwait(false);
        }

        private static async Task InsertAsync<T>(DbContext context, Type type, IList<T> entities, TableInfo tableInfo, Action<decimal> progress, CancellationToken cancellationToken)
        {
            var adapter = SqlAdaptersMapping.CreateBulkOperationsAdapter(context);
            await adapter.InsertAsync(context,type,entities,tableInfo,progress,cancellationToken);
        }

        public static void Merge<T>(DbContext context, IList<T> entities, TableInfo tableInfo, OperationType operationType, Action<decimal> progress) where T : class
        {
            Merge<T>(context, typeof(T), entities, tableInfo, operationType, progress);
        }

        public static void Merge(DbContext context, Type type, IList<object> entities, TableInfo tableInfo, OperationType operationType, Action<decimal> progress)
        {
            Merge<object>(context, type, entities, tableInfo, operationType, progress);
        }

        private static void Merge<T>(DbContext context, Type type, IList<T> entities, TableInfo tableInfo, OperationType operationType, Action<decimal> progress) where T : class
        {
            var adapter = SqlAdaptersMapping.CreateBulkOperationsAdapter(context);
            adapter.Merge(context,type,entities,tableInfo,operationType,progress);
        }

        public static async Task MergeAsync<T>(DbContext context, IList<T> entities, TableInfo tableInfo, OperationType operationType, Action<decimal> progress, CancellationToken cancellationToken) where T : class
        {
            await MergeAsync<T>(context, typeof(T), entities, tableInfo, operationType, progress, cancellationToken).ConfigureAwait(false);
        }

        public static async Task MergeAsync(DbContext context, Type type, IList<object> entities, TableInfo tableInfo, OperationType operationType, Action<decimal> progress, CancellationToken cancellationToken)
        {
            await MergeAsync<object>(context, type, entities, tableInfo, operationType, progress, cancellationToken).ConfigureAwait(false);
        }

        private static async Task MergeAsync<T>(DbContext context, Type type, IList<T> entities, TableInfo tableInfo, OperationType operationType, Action<decimal> progress, CancellationToken cancellationToken) where T : class
        {
            var adapter = SqlAdaptersMapping.CreateBulkOperationsAdapter(context);
            await adapter.MergeAsync(context,type,entities,tableInfo,operationType, progress,cancellationToken);
        }

        public static void Read<T>(DbContext context, IList<T> entities, TableInfo tableInfo, Action<decimal> progress) where T : class
        {
            Read<T>(context, typeof(T), entities, tableInfo, progress);
        }

        public static void Read(DbContext context, Type type, IList<object> entities, TableInfo tableInfo, Action<decimal> progress)
        {
            Read<object>(context, type, entities, tableInfo, progress);
        }

        private static void Read<T>(DbContext context, Type type, IList<T> entities, TableInfo tableInfo, Action<decimal> progress) where T : class
        {
            var dropTempTableIfExists = tableInfo.BulkConfig.UseTempDB;

            if (dropTempTableIfExists)
            {
                context.Database.ExecuteSqlRaw(SqlQueryBuilder.DropTable(tableInfo.FullTempTableName, tableInfo.BulkConfig.UseTempDB));
            }

            var adapter = SqlAdaptersMapping.CreateBulkOperationsAdapter(context);
            adapter.Read(context,type,entities,tableInfo,progress);
        }

        public static async Task ReadAsync<T>(DbContext context, IList<T> entities, TableInfo tableInfo, Action<decimal> progress, CancellationToken cancellationToken) where T : class
        {
            await ReadAsync<T>(context, typeof(T), entities, tableInfo, progress, cancellationToken).ConfigureAwait(false);
        }

        public static async Task ReadAsync(DbContext context, Type type, IList<object> entities, TableInfo tableInfo, Action<decimal> progress, CancellationToken cancellationToken)
        {
            await ReadAsync<object>(context, type, entities, tableInfo, progress, cancellationToken).ConfigureAwait(false);
        }

        private static async Task ReadAsync<T>(DbContext context, Type type, IList<T> entities, TableInfo tableInfo, Action<decimal> progress, CancellationToken cancellationToken) where T : class
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
