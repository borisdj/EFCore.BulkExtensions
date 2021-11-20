using EFCore.BulkExtensions.SqlAdapters;
using EFCore.BulkExtensions.SQLAdapters.SQLite;
using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;
using Npgsql;
using NpgsqlTypes;
using System;
using System.Collections.Generic;
using System.Data;
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
            NpgsqlConnection connection = tableInfo.NpgsqlConnection;
            bool closeConnectionInternally = false;
            if (connection == null)
            {
                (connection, closeConnectionInternally) =
                    isAsync ? await OpenAndGetNpgsqlConnectionAsync(context, tableInfo.BulkConfig, cancellationToken).ConfigureAwait(false)
                            : OpenAndGetNpgsqlConnection(context, tableInfo.BulkConfig);
            }
            
            try
            {
                var sqlCopy = SqlQueryBuilderPostgreSql.InsertIntoTable(tableInfo, OperationType.Insert);

                using (var writer = connection.BeginBinaryImport(sqlCopy))
                {
                    foreach (var entity in entities)
                    {
                        writer.StartRow();
                        var propertiesNames = tableInfo.PropertyColumnNamesDict.Where(a => a.Value != tableInfo.IdentityColumnName).Select(a => a.Key).ToList();
                        foreach (var propertyName in propertiesNames)
                        {
                            var propertyValue = tableInfo.FastPropertyDict.ContainsKey(propertyName) ? tableInfo.FastPropertyDict[propertyName].Get(entity) : null;
                            //var isDecimalType = tableInfo.FastPropertyDict[propertyName].Property.PropertyType == typeof(decimal);
                            //if (isDecimalType)
                            //    writer.Write(propertyValue, NpgsqlDbType.Numeric);
                            //else
                                writer.Write(propertyValue);
                        }
                    }
                    writer.Complete();
                }
            }
            finally
            {
                if (closeConnectionInternally)
                {
                    //connection.Close();
                    if (isAsync)
                    {
                        await connection.CloseAsync();
                        //await context.Database.CloseConnectionAsync().ConfigureAwait(false);
                    }
                    else
                    {
                        connection.Close();
                        //context.Database.CloseConnection();
                    }
                }
            }
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
            var sqlTruncateTable = SqlQueryBuilderPostgreSql.TruncateTable(tableInfo.FullTableName);
            context.Database.ExecuteSqlRaw(sqlTruncateTable);
        }

        public async Task TruncateAsync(DbContext context, TableInfo tableInfo, CancellationToken cancellationToken)
        {
            var sqlTruncateTable = SqlQueryBuilderPostgreSql.TruncateTable(tableInfo.FullTableName);
            await context.Database.ExecuteSqlRawAsync(sqlTruncateTable, cancellationToken).ConfigureAwait(false);
        }
        #endregion

        #region Connection
        internal static async Task<(NpgsqlConnection, bool)> OpenAndGetNpgsqlConnectionAsync(DbContext context, BulkConfig bulkConfig, CancellationToken cancellationToken)
        {
            bool closeConnectionInternally = false;
            var npgsqlConnection = (NpgsqlConnection)context.Database.GetDbConnection();
            if (npgsqlConnection.State != ConnectionState.Open)
            {
                await npgsqlConnection.OpenAsync(cancellationToken).ConfigureAwait(false);
                closeConnectionInternally = true;
            }
            return (npgsqlConnection, closeConnectionInternally);

            //await context.Database.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
            //return (NpgsqlConnection)context.Database.GetDbConnection();
        }

        internal static (NpgsqlConnection, bool) OpenAndGetNpgsqlConnection(DbContext context, BulkConfig bulkConfig)
        {
            bool closeConnectionInternally = false;
            var npgsqlConnection = (NpgsqlConnection)context.Database.GetDbConnection();
            if (npgsqlConnection.State != ConnectionState.Open)
            {
                npgsqlConnection.Open();
                closeConnectionInternally = true;
            }
            return (npgsqlConnection, closeConnectionInternally);

            //context.Database.OpenConnection();
            //return (NpgsqlConnection)context.Database.GetDbConnection();

        }
        #endregion

        internal static NpgsqlCommand GetCommand<T>(DbContext context, Type type, IList<T> entities, TableInfo tableInfo, NpgsqlConnection connection, NpgsqlTransaction transaction)
        {
            NpgsqlCommand command = connection.CreateCommand();
            command.Transaction = transaction;

            var operationType = tableInfo.BulkConfig.OperationType;

            switch (operationType)
            {
                case OperationType.Insert:
                    command.CommandText = SqlQueryBuilderSqlite.InsertIntoTable(tableInfo, OperationType.Insert);
                    break;
                /*case OperationType.InsertOrUpdate:
                    command.CommandText = SqlQueryBuilderSqlite.InsertIntoTable(tableInfo, OperationType.InsertOrUpdate);
                    break;
                case OperationType.InsertOrUpdateDelete:
                    throw new NotSupportedException("'BulkInsertOrUpdateDelete' not supported for Sqlite. Sqlite has only UPSERT statement (analog for MERGE WHEN MATCHED) but no functionality for: 'WHEN NOT MATCHED BY SOURCE THEN DELETE'." +
                                                    " Another way to achieve this is to BulkRead existing data from DB, split list into sublists and call separately Bulk methods for Insert, Update, Delete.");
                case OperationType.Update:
                    command.CommandText = SqlQueryBuilderSqlite.UpdateSetTable(tableInfo);
                    break;
                case OperationType.Delete:
                    command.CommandText = SqlQueryBuilderSqlite.DeleteFromTable(tableInfo);
                    break;*/
            }

            /*var shadowProperties = tableInfo.ShadowProperties;
            foreach (var shadowProperty in shadowProperties)
            {
                var parameter = new SqliteParameter($"@{shadowProperty}", typeof(string));
                command.Parameters.Add(parameter);
            }*/

            command.Prepare(); // Not Required but called for efficiency (prepared should be little faster)
            return command;
        }
    }
}
