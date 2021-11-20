using EFCore.BulkExtensions.SqlAdapters;
using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;

namespace EFCore.BulkExtensions.SQLAdapters.PostgreSql
{
    public class PostgreSqlAdapter : ISqlOperationsAdapter
    {
        #region Methods
        // Insert
        public void Insert<T>(DbContext context, Type type, IList<T> entities, TableInfo tableInfo, Action<decimal> progress)
        {
            InsertAsync(context, type, entities, tableInfo, progress, CancellationToken.None, isAsync: false).GetAwaiter().GetResult();
        }

        public async Task InsertAsync<T>(DbContext context, Type type, IList<T> entities, TableInfo tableInfo, Action<decimal> progress, CancellationToken cancellationToken)
        {
            await InsertAsync(context, type, entities, tableInfo, progress, cancellationToken, isAsync: true).ConfigureAwait(false);
        }
        protected async Task InsertAsync<T>(DbContext context, Type type, IList<T> entities, TableInfo tableInfo, Action<decimal> progress, CancellationToken cancellationToken, bool isAsync)
        {
            throw new NotImplementedException();
            /*
            tableInfo.CheckToSetIdentityForPreserveOrder(entities);
            if (isAsync)
            {
                await context.Database.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
            }
            else
            {
                context.Database.OpenConnection();
            }
            var connection = context.GetUnderlyingConnection(tableInfo.BulkConfig);

            try
            {
                var transaction = context.Database.CurrentTransaction;

                using var sqlBulkCopy = GetSqlBulkCopy((SqlConnection)connection, transaction, tableInfo.BulkConfig);
                bool setColumnMapping = false;
                tableInfo.SetSqlBulkCopyConfig(sqlBulkCopy, entities, setColumnMapping, progress);
                try
                {
                    var dataTable = GetDataTable(context, type, entities, sqlBulkCopy, tableInfo);
                    if (isAsync)
                    {
                        await sqlBulkCopy.WriteToServerAsync(dataTable, cancellationToken).ConfigureAwait(false);
                    }
                    else
                    {
                        sqlBulkCopy.WriteToServer(dataTable);
                    }
                }
                catch (InvalidOperationException ex)
                {
                    if (ex.Message.Contains(BulkExceptionMessage.ColumnMappingNotMatch))
                    {
                        bool tableExist = isAsync ? await tableInfo.CheckTableExistAsync(context, tableInfo, cancellationToken, isAsync: true).ConfigureAwait(false)
                                                        : tableInfo.CheckTableExistAsync(context, tableInfo, cancellationToken, isAsync: false).GetAwaiter().GetResult();

                        if (!tableExist)
                        {
                            var sqlCreateTableCopy = SqlQueryBuilder.CreateTableCopy(tableInfo.FullTableName, tableInfo.FullTempTableName, tableInfo);
                            var sqlDropTable = SqlQueryBuilder.DropTable(tableInfo.FullTempTableName, tableInfo.BulkConfig.UseTempDB);

                            if (isAsync)
                            {
                                await context.Database.ExecuteSqlRawAsync(sqlCreateTableCopy, cancellationToken).ConfigureAwait(false);
                                await context.Database.ExecuteSqlRawAsync(sqlDropTable, cancellationToken).ConfigureAwait(false);
                            }
                            else
                            {
                                context.Database.ExecuteSqlRaw(sqlCreateTableCopy);
                                context.Database.ExecuteSqlRaw(sqlDropTable);
                            }
                        }
                    }
                    throw;
                }
            }
            finally
            {
                if (isAsync)
                {
                    await context.Database.CloseConnectionAsync().ConfigureAwait(false);
                }
                else
                {
                    context.Database.CloseConnection();
                }
            }
            if (!tableInfo.CreatedOutputTable)
            {
                tableInfo.CheckToSetIdentityForPreserveOrder(entities, reset: true);
            }
            */
        }

        // Merge
        public void Merge<T>(DbContext context, Type type, IList<T> entities, TableInfo tableInfo, OperationType operationType, Action<decimal> progress) where T : class
        {
            MergeAsync(context, type, entities, tableInfo, operationType, progress, CancellationToken.None, isAsync: false).GetAwaiter().GetResult();
        }

        public async Task MergeAsync<T>(DbContext context, Type type, IList<T> entities, TableInfo tableInfo, OperationType operationType, Action<decimal> progress, CancellationToken cancellationToken) where T : class
        {
            await MergeAsync(context, type, entities, tableInfo, operationType, progress, cancellationToken, isAsync: true).ConfigureAwait(false);
        }

        protected async Task MergeAsync<T>(DbContext context, Type type, IList<T> entities, TableInfo tableInfo, OperationType operationType, Action<decimal> progress, CancellationToken cancellationToken, bool isAsync) where T : class
        {
            throw new NotImplementedException();
        }

        // Read
        public void Read<T>(DbContext context, Type type, IList<T> entities, TableInfo tableInfo, Action<decimal> progress) where T : class
        {
            ReadAsync(context, type, entities, tableInfo, progress, CancellationToken.None, isAsync: false).GetAwaiter().GetResult();
        }

        public async Task ReadAsync<T>(DbContext context, Type type, IList<T> entities, TableInfo tableInfo, Action<decimal> progress, CancellationToken cancellationToken) where T : class
        {
            await ReadAsync(context, type, entities, tableInfo, progress, cancellationToken, isAsync: true).ConfigureAwait(false);
        }

        protected async Task ReadAsync<T>(DbContext context, Type type, IList<T> entities, TableInfo tableInfo, Action<decimal> progress, CancellationToken cancellationToken, bool isAsync) where T : class
        {
            throw new NotImplementedException();
        }

        // Truncate
        public void Truncate(DbContext context, TableInfo tableInfo)
        {
            throw new NotImplementedException();
            //var sqlTruncateTable = SqlQueryBuilder.TruncateTable(tableInfo.FullTableName);
            //context.Database.ExecuteSqlRaw(sqlTruncateTable);
        }

        public async Task TruncateAsync(DbContext context, TableInfo tableInfo, CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
            //var sqlTruncateTable = SqlQueryBuilder.TruncateTable(tableInfo.FullTableName);
            //await context.Database.ExecuteSqlRawAsync(sqlTruncateTable, cancellationToken).ConfigureAwait(false);
        }
        #endregion
    }
}
