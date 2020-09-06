using FastMember;
using Microsoft.Data.SqlClient;
using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;

namespace EFCore.BulkExtensions
{
    public enum DbServer
    {
        SqlServer,
        MySql,
        PostrgeSql,
        Sqlite,
    }

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

    public class SqlProviderNotSupportedException : NotSupportedException
    {
        public SqlProviderNotSupportedException(string providerName, string message = null) : base($"Provider {providerName} not supported. Only SQL Server and SQLite are Currently supported. {message}") { }
    }

    internal static class SqlBulkOperation
    {
        internal static string ColumnMappingExceptionMessage => "The given ColumnMapping does not match up with any column in the source or destination";

        #region MainOps
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
            string providerName = context.Database.ProviderName; // "Microsoft.EntityFrameworkCore.*****"
            // -- SQL Server --
            if (providerName.EndsWith(DbServer.SqlServer.ToString()))
            {
                var connection = OpenAndGetSqlConnection(context, tableInfo.BulkConfig);
                try
                {
                    var transaction = context.Database.CurrentTransaction;

                    using (var sqlBulkCopy = GetSqlBulkCopy((SqlConnection)connection, transaction, tableInfo.BulkConfig))
                    {
                        bool useFastMember = tableInfo.HasOwnedTypes == false                      // With OwnedTypes DataTable is used since library FastMember can not (https://github.com/mgravell/fast-member/issues/21)
                                             && tableInfo.ColumnNameContainsSquareBracket == false // FastMember does not support escaped columnNames  ] -> ]]
                                             && tableInfo.ShadowProperties.Count == 0              // With Shadow prop. Discriminator (TPH inheritance) also not used because FastMember is slow for Update (https://github.com/borisdj/EFCore.BulkExtensions/pull/17)
                                             && !tableInfo.ConvertibleProperties.Any()             // With ConvertibleProperties FastMember is slow as well
                                             && !tableInfo.HasAbstractList                         // AbstractList not working with FastMember
                                             && !tableInfo.BulkConfig.UseOnlyDataTable;
                        bool setColumnMapping = useFastMember;
                        tableInfo.SetSqlBulkCopyConfig(sqlBulkCopy, entities, setColumnMapping, progress);
                        try
                        {
                            if (useFastMember)
                            {
                                using (var reader = ObjectReaderEx.Create(type, entities, tableInfo.ShadowProperties, tableInfo.ConvertibleProperties, context, tableInfo.PropertyColumnNamesDict.Keys.ToArray()))
                                {
                                    sqlBulkCopy.WriteToServer(reader);
                                }
                            }
                            else
                            {
                                var dataTable = GetDataTable(context, type, entities, sqlBulkCopy, tableInfo);
                                sqlBulkCopy.WriteToServer(dataTable);
                            }
                        }
                        catch (InvalidOperationException ex)
                        {
                            if (ex.Message.Contains(ColumnMappingExceptionMessage))
                            {
                                if (!tableInfo.CheckTableExist(context, tableInfo))
                                {
                                    context.Database.ExecuteSqlRaw(SqlQueryBuilder.CreateTableCopy(tableInfo.FullTableName, tableInfo.FullTempTableName, tableInfo)); // Will throw Exception specify missing db column: Invalid column name ''
                                    context.Database.ExecuteSqlRaw(SqlQueryBuilder.DropTable(tableInfo.FullTempTableName, tableInfo.BulkConfig.UseTempDB));
                                }
                            }
                            throw ex;
                        }
                    }
                }
                finally
                {
                    context.Database.CloseConnection();
                }
            }
            // -- SQLite --
            else if (providerName.EndsWith(DbServer.Sqlite.ToString()))
            {
                var connection = OpenAndGetSqliteConnection(context, tableInfo.BulkConfig);

                try
                {
                    var transaction = tableInfo.BulkConfig.SqliteTransaction ?? connection.BeginTransaction();
                    try
                    {
                        var command = GetSqliteCommand(context, type, entities, tableInfo, connection, transaction);

                        type = tableInfo.HasAbstractList ? entities[0].GetType() : type;
                        var typeAccessor = TypeAccessor.Create(type, true);
                        int rowsCopied = 0;
                        foreach (var item in entities)
                        {
                            LoadSqliteValues(tableInfo, typeAccessor, item, command);
                            command.ExecuteNonQuery();
                            SetProgress(ref rowsCopied, entities.Count, tableInfo.BulkConfig, progress);
                        }
                    }
                    finally
                    {
                        if (tableInfo.BulkConfig.SqliteTransaction == null)
                        {
                            transaction.Commit();
                            transaction.Dispose();
                        }
                    }
                }
                finally
                {
                    context.Database.CloseConnection();
                }
            }
            else
            {
                throw new SqlProviderNotSupportedException(providerName);
            }
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
            string providerName = context.Database.ProviderName; // "Microsoft.EntityFrameworkCore.*****"
            // -- SQL Server --
            if (providerName.EndsWith(DbServer.SqlServer.ToString()))
            {
                var connection = await OpenAndGetSqlConnectionAsync(context, tableInfo.BulkConfig, cancellationToken).ConfigureAwait(false);
                try
                {
                    var transaction = context.Database.CurrentTransaction;

                    using (var sqlBulkCopy = GetSqlBulkCopy((SqlConnection)connection, transaction, tableInfo.BulkConfig))
                    {
                        bool useFastMember = tableInfo.HasOwnedTypes == false
                                             && tableInfo.ColumnNameContainsSquareBracket == false
                                             && tableInfo.ShadowProperties.Count == 0
                                             && !tableInfo.ConvertibleProperties.Any()
                                             && !tableInfo.HasAbstractList
                                             && !tableInfo.BulkConfig.UseOnlyDataTable;
                        bool setColumnMapping = useFastMember;
                        tableInfo.SetSqlBulkCopyConfig(sqlBulkCopy, entities, setColumnMapping, progress);
                        try
                        {
                            if (useFastMember)
                            {
                                using (var reader = ObjectReaderEx.Create(type, entities, tableInfo.ShadowProperties, tableInfo.ConvertibleProperties, context, tableInfo.PropertyColumnNamesDict.Keys.ToArray()))
                                {
                                    await sqlBulkCopy.WriteToServerAsync(reader, cancellationToken).ConfigureAwait(false);
                                }
                            }
                            else
                            {
                                var dataTable = GetDataTable(context, type, entities, sqlBulkCopy, tableInfo);
                                await sqlBulkCopy.WriteToServerAsync(dataTable, cancellationToken).ConfigureAwait(false);
                            }
                        }
                        catch (InvalidOperationException ex)
                        {
                            if (ex.Message.Contains(ColumnMappingExceptionMessage))
                            {
                                if (!await tableInfo.CheckTableExistAsync(context, tableInfo, cancellationToken).ConfigureAwait(false))
                                {
                                    await context.Database.ExecuteSqlRawAsync(SqlQueryBuilder.CreateTableCopy(tableInfo.FullTableName, tableInfo.FullTempTableName, tableInfo), cancellationToken).ConfigureAwait(false);
                                    await context.Database.ExecuteSqlRawAsync(SqlQueryBuilder.DropTable(tableInfo.FullTempTableName, tableInfo.BulkConfig.UseTempDB), cancellationToken).ConfigureAwait(false);
                                }
                            }
                            throw ex;
                        }
                    }
                }
                finally
                {
                    await context.Database.CloseConnectionAsync().ConfigureAwait(false);
                }
            }
            // -- SQLite --
            else if (providerName.EndsWith(DbServer.Sqlite.ToString()))
            {
                var connection = await OpenAndGetSqliteConnectionAsync(context, tableInfo.BulkConfig, cancellationToken).ConfigureAwait(false);

                try
                {
                    var transaction = tableInfo.BulkConfig.SqliteTransaction ?? connection.BeginTransaction();

                    try
                    {
                        var command = GetSqliteCommand(context, type, entities, tableInfo, connection, transaction);

                        type = tableInfo.HasAbstractList ? entities[0].GetType() : type;
                        var typeAccessor = TypeAccessor.Create(type, true);
                        int rowsCopied = 0;

                        foreach (var item in entities)
                        {
                            LoadSqliteValues(tableInfo, typeAccessor, item, command);
                            await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
                            SetProgress(ref rowsCopied, entities.Count, tableInfo.BulkConfig, progress);
                        }
                    }
                    finally
                    {
                        if (tableInfo.BulkConfig.SqliteTransaction == null)
                        {
                            transaction.Commit();
                            transaction.Dispose();
                        }
                    }
                }
                finally
                {
                    await context.Database.CloseConnectionAsync().ConfigureAwait(false);
                }
            }
            else
            {
                throw new SqlProviderNotSupportedException(providerName);
            }
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
            string providerName = context.Database.ProviderName;
            // -- SQL Server --
            if (providerName.EndsWith(DbServer.SqlServer.ToString()))
            {
                tableInfo.InsertToTempTable = true;
                tableInfo.CheckHasIdentity(context);

                var dropTempTableIfExists = tableInfo.BulkConfig.UseTempDB;

                if (dropTempTableIfExists)
                {
                    context.Database.ExecuteSqlRaw(SqlQueryBuilder.DropTable(tableInfo.FullTempTableName, tableInfo.BulkConfig.UseTempDB));
                }

                context.Database.ExecuteSqlRaw(SqlQueryBuilder.CreateTableCopy(tableInfo.FullTableName, tableInfo.FullTempTableName, tableInfo));
                if (tableInfo.CreatedOutputTable)
                {
                    context.Database.ExecuteSqlRaw(SqlQueryBuilder.CreateTableCopy(tableInfo.FullTableName, tableInfo.FullTempOutputTableName, tableInfo, true));
                    if (tableInfo.TimeStampColumnName != null)
                    {
                        context.Database.ExecuteSqlRaw(SqlQueryBuilder.AddColumn(tableInfo.FullTempOutputTableName, tableInfo.TimeStampColumnName, tableInfo.TimeStampOutColumnType));
                    }
                }

                bool keepIdentity = tableInfo.BulkConfig.SqlBulkCopyOptions.HasFlag(SqlBulkCopyOptions.KeepIdentity);
                try
                {
                    Insert(context, type, entities, tableInfo, progress);

                    if (keepIdentity && tableInfo.HasIdentity)
                    {
                        context.Database.OpenConnection();
                        context.Database.ExecuteSqlRaw(SqlQueryBuilder.SetIdentityInsert(tableInfo.FullTableName, true));
                    }

                    context.Database.ExecuteSqlRaw(SqlQueryBuilder.MergeTable(tableInfo, operationType));

                    if (tableInfo.CreatedOutputTable)
                    {
                        tableInfo.LoadOutputData(context, type, entities);
                    }
                }
                finally
                {
                    if (!tableInfo.BulkConfig.UseTempDB)
                    {
                        if (tableInfo.CreatedOutputTable)
                        {
                            context.Database.ExecuteSqlRaw(SqlQueryBuilder.DropTable(tableInfo.FullTempOutputTableName, tableInfo.BulkConfig.UseTempDB));
                        }
                        context.Database.ExecuteSqlRaw(SqlQueryBuilder.DropTable(tableInfo.FullTempTableName, tableInfo.BulkConfig.UseTempDB));
                    }

                    if (keepIdentity && tableInfo.HasIdentity)
                    {
                        context.Database.ExecuteSqlRaw(SqlQueryBuilder.SetIdentityInsert(tableInfo.FullTableName, false));
                        context.Database.CloseConnection();
                    }
                }
            }
            // -- SQLite --
            else if (providerName.EndsWith(DbServer.Sqlite.ToString()))
            {
                var connection = OpenAndGetSqliteConnection(context, tableInfo.BulkConfig);

                try
                {
                    var transaction = tableInfo.BulkConfig.SqliteTransaction ?? connection.BeginTransaction();
                    try
                    {
                        var command = GetSqliteCommand(context, type, entities, tableInfo, connection, transaction);

                        type = tableInfo.HasAbstractList ? entities[0].GetType() : type;
                        var typeAccessor = TypeAccessor.Create(type, true);
                        int rowsCopied = 0;
                        foreach (var item in entities)
                        {
                            LoadSqliteValues(tableInfo, typeAccessor, item, command);
                            command.ExecuteNonQuery();
                            SetProgress(ref rowsCopied, entities.Count, tableInfo.BulkConfig, progress);
                        }

                        if (operationType != OperationType.Delete && tableInfo.BulkConfig.SetOutputIdentity && tableInfo.IdentityColumnName != null)
                        {
                            command.CommandText = SqlQueryBuilderSqlite.SelectLastInsertRowId();
                            long lastRowIdScalar = (long)command.ExecuteScalar();
                            var identityPropertyInteger = false;
                            var identityPropertyUnsigned = false;
                            var identityPropertyByte = false;
                            var identityPropertyShort = false;

                            var accessor = TypeAccessor.Create(type, true);
                            string identityPropertyName = tableInfo.IdentityColumnName;

                            if (accessor.GetMembers().FirstOrDefault(x => x.Name == identityPropertyName)?.Type == typeof(ulong))
                            {
                                identityPropertyUnsigned = true;
                            }
                            else if (accessor.GetMembers().FirstOrDefault(x => x.Name == identityPropertyName)?.Type == typeof(uint))
                            {
                                identityPropertyInteger = true;
                                identityPropertyUnsigned = true;
                            }
                            else if (accessor.GetMembers().FirstOrDefault(x => x.Name == identityPropertyName)?.Type == typeof(int))
                            {
                                identityPropertyInteger = true;
                            }
                            else if (accessor.GetMembers().FirstOrDefault(x => x.Name == identityPropertyName)?.Type == typeof(ushort))
                            {
                                identityPropertyShort = true;
                                identityPropertyUnsigned = true;
                            }
                            else if (accessor.GetMembers().FirstOrDefault(x => x.Name == identityPropertyName)?.Type == typeof(short))
                            {
                                identityPropertyShort = true;
                            }
                            else if (accessor.GetMembers().FirstOrDefault(x => x.Name == identityPropertyName)?.Type == typeof(byte))
                            {
                                identityPropertyByte = true;
                                identityPropertyUnsigned = true;
                            }
                            else if (accessor.GetMembers().FirstOrDefault(x => x.Name == identityPropertyName)?.Type == typeof(sbyte))
                            {
                                identityPropertyByte = true;
                            }
                            
                            for (int i = entities.Count - 1; i >= 0; i--)
                            {
                                if (identityPropertyByte)
                                {
                                    if(identityPropertyUnsigned)
                                        accessor[entities[i], identityPropertyName] = (byte)lastRowIdScalar;
                                    else
                                        accessor[entities[i], identityPropertyName] = (sbyte)lastRowIdScalar;
                                }
                                else if (identityPropertyShort)
                                {
                                    if(identityPropertyUnsigned)
                                        accessor[entities[i], identityPropertyName] = (ushort)lastRowIdScalar;
                                    else
                                        accessor[entities[i], identityPropertyName] = (short)lastRowIdScalar;
                                }
                                else if (identityPropertyInteger)
                                {
                                    if(identityPropertyUnsigned)
                                        accessor[entities[i], identityPropertyName] = (uint)lastRowIdScalar;
                                    else
                                        accessor[entities[i], identityPropertyName] = (int)lastRowIdScalar;
                                }
                                else
                                {
                                    if(identityPropertyUnsigned)
                                        accessor[entities[i], identityPropertyName] = (ulong)lastRowIdScalar;
                                    else
                                        accessor[entities[i], identityPropertyName] = lastRowIdScalar;
                                }

                                lastRowIdScalar--;
                            }
                        }
                    }
                    finally
                    {
                        if (tableInfo.BulkConfig.SqliteTransaction == null)
                        {
                            transaction.Commit();
                            transaction.Dispose();
                        }
                    }
                }
                finally
                {
                    context.Database.CloseConnection();
                }
            }
            else
            {
                throw new SqlProviderNotSupportedException(providerName);
            }
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
            string providerName = context.Database.ProviderName;
            // -- SQL Server --
            if (providerName.EndsWith(DbServer.SqlServer.ToString()))
            {
                tableInfo.InsertToTempTable = true;
                await tableInfo.CheckHasIdentityAsync(context, cancellationToken).ConfigureAwait(false);

                var dropTempTableIfExists = tableInfo.BulkConfig.UseTempDB;

                if (dropTempTableIfExists)
                {
                    await context.Database.ExecuteSqlRawAsync(SqlQueryBuilder.DropTable(tableInfo.FullTempTableName, tableInfo.BulkConfig.UseTempDB)).ConfigureAwait(false);
                }

                await context.Database.ExecuteSqlRawAsync(SqlQueryBuilder.CreateTableCopy(tableInfo.FullTableName, tableInfo.FullTempTableName, tableInfo), cancellationToken).ConfigureAwait(false);
                if (tableInfo.CreatedOutputTable)
                {
                    await context.Database.ExecuteSqlRawAsync(SqlQueryBuilder.CreateTableCopy(tableInfo.FullTableName, tableInfo.FullTempOutputTableName, tableInfo, true), cancellationToken).ConfigureAwait(false);
                    if (tableInfo.TimeStampColumnName != null)
                    {
                        await context.Database.ExecuteSqlRawAsync(SqlQueryBuilder.AddColumn(tableInfo.FullTempOutputTableName, tableInfo.TimeStampColumnName, tableInfo.TimeStampOutColumnType), cancellationToken).ConfigureAwait(false);
                    }
                }

                bool keepIdentity = tableInfo.BulkConfig.SqlBulkCopyOptions.HasFlag(SqlBulkCopyOptions.KeepIdentity);
                try
                {
                    await InsertAsync(context, type, entities, tableInfo, progress, cancellationToken).ConfigureAwait(false);

                    if (keepIdentity && tableInfo.HasIdentity)
                    {
                        await context.Database.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
                        await context.Database.ExecuteSqlRawAsync(SqlQueryBuilder.SetIdentityInsert(tableInfo.FullTableName, true), cancellationToken).ConfigureAwait(false);
                    }

                    await context.Database.ExecuteSqlRawAsync(SqlQueryBuilder.MergeTable(tableInfo, operationType), cancellationToken).ConfigureAwait(false);

                    if (tableInfo.CreatedOutputTable)
                    {
                        await tableInfo.LoadOutputDataAsync(context, type, entities, cancellationToken).ConfigureAwait(false);
                    }
                }
                finally
                {
                    if (!tableInfo.BulkConfig.UseTempDB)
                    {
                        if (tableInfo.CreatedOutputTable)
                        {
                            await context.Database.ExecuteSqlRawAsync(SqlQueryBuilder.DropTable(tableInfo.FullTempOutputTableName, tableInfo.BulkConfig.UseTempDB), cancellationToken).ConfigureAwait(false);
                        }
                        await context.Database.ExecuteSqlRawAsync(SqlQueryBuilder.DropTable(tableInfo.FullTempTableName, tableInfo.BulkConfig.UseTempDB), cancellationToken).ConfigureAwait(false);
                    }

                    if (keepIdentity && tableInfo.HasIdentity)
                    {
                        await context.Database.ExecuteSqlRawAsync(SqlQueryBuilder.SetIdentityInsert(tableInfo.FullTableName, false), cancellationToken).ConfigureAwait(false);
                        context.Database.CloseConnection();
                    }
                }
            }
            // -- SQLite --
            else if (providerName.EndsWith(DbServer.Sqlite.ToString()))
            {
                var connection = await OpenAndGetSqliteConnectionAsync(context, tableInfo.BulkConfig, cancellationToken).ConfigureAwait(false);

                try
                {
                    var transaction = tableInfo.BulkConfig.SqliteTransaction ?? connection.BeginTransaction();

                    try
                    {
                        var command = GetSqliteCommand(context, type, entities, tableInfo, connection, transaction);

                        type = tableInfo.HasAbstractList ? entities[0].GetType() : type;
                        var typeAccessor = TypeAccessor.Create(type, true);
                        int rowsCopied = 0;

                        foreach (var item in entities)
                        {
                            LoadSqliteValues(tableInfo, typeAccessor, item, command);
                            await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
                            SetProgress(ref rowsCopied, entities.Count, tableInfo.BulkConfig, progress);
                        }

                        if (operationType != OperationType.Delete && tableInfo.BulkConfig.SetOutputIdentity && tableInfo.IdentityColumnName != null)
                        {
                            command.CommandText = SqlQueryBuilderSqlite.SelectLastInsertRowId();
                            long lastRowIdScalar = (long)await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
                            var identityPropertyInteger = false;
                            var identityPropertyUnsigned = false;
                            var identityPropertyByte = false;
                            var identityPropertyShort = false;
                            var accessor = TypeAccessor.Create(type, true);
                            string identityPropertyName = tableInfo.PropertyColumnNamesDict.SingleOrDefault(a => a.Value == tableInfo.IdentityColumnName).Key;
                            if (accessor.GetMembers().FirstOrDefault(x => x.Name == identityPropertyName)?.Type == typeof(ulong))
                            {
                                identityPropertyUnsigned = true;
                            }
                            else if (accessor.GetMembers().FirstOrDefault(x => x.Name == identityPropertyName)?.Type == typeof(uint))
                            {
                                identityPropertyInteger = true;
                                identityPropertyUnsigned = true;
                            }
                            else if (accessor.GetMembers().FirstOrDefault(x => x.Name == identityPropertyName)?.Type == typeof(int))
                            {
                                identityPropertyInteger = true;
                            }
                            else if (accessor.GetMembers().FirstOrDefault(x => x.Name == identityPropertyName)?.Type == typeof(ushort))
                            {
                                identityPropertyShort = true;
                                identityPropertyUnsigned = true;
                            }
                            else if (accessor.GetMembers().FirstOrDefault(x => x.Name == identityPropertyName)?.Type == typeof(short))
                            {
                                identityPropertyShort = true;
                            }
                            else if (accessor.GetMembers().FirstOrDefault(x => x.Name == identityPropertyName)?.Type == typeof(byte))
                            {
                                identityPropertyByte = true;
                                identityPropertyUnsigned = true;
                            }
                            else if (accessor.GetMembers().FirstOrDefault(x => x.Name == identityPropertyName)?.Type == typeof(sbyte))
                            {
                                identityPropertyByte = true;
                            }
                            for (int i = entities.Count - 1; i >= 0; i--)                            
                            {
                                if (identityPropertyByte)
                                {
                                    if(identityPropertyUnsigned)
                                        accessor[entities[i], identityPropertyName] = (byte)lastRowIdScalar;
                                    else
                                        accessor[entities[i], identityPropertyName] = (sbyte)lastRowIdScalar;
                                }
                                else if (identityPropertyShort)
                                {
                                    if(identityPropertyUnsigned)
                                        accessor[entities[i], identityPropertyName] = (ushort)lastRowIdScalar;
                                    else
                                        accessor[entities[i], identityPropertyName] = (short)lastRowIdScalar;
                                }
                                else if (identityPropertyInteger)
                                {
                                    if(identityPropertyUnsigned)
                                        accessor[entities[i], identityPropertyName] = (uint)lastRowIdScalar;
                                    else
                                        accessor[entities[i], identityPropertyName] = (int)lastRowIdScalar;
                                }
                                else
                                {
                                    if(identityPropertyUnsigned)
                                        accessor[entities[i], identityPropertyName] = (ulong)lastRowIdScalar;
                                    else
                                        accessor[entities[i], identityPropertyName] = lastRowIdScalar;
                                }

                                lastRowIdScalar--;
                            }
                        }
                    }
                    finally
                    {
                        if (tableInfo.BulkConfig.SqliteTransaction == null)
                        {
                            transaction.Commit();
                            transaction.Dispose();
                        }
                    }
                }
                finally
                {
                    await context.Database.CloseConnectionAsync().ConfigureAwait(false);
                }
            }
            else
            {
                throw new SqlProviderNotSupportedException(providerName);
            }
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
            Dictionary<string, string> previousPropertyColumnNamesDict = tableInfo.ConfigureBulkReadTableInfo(context);

            var dropTempTableIfExists = tableInfo.BulkConfig.UseTempDB;

            if (dropTempTableIfExists)
            {
                context.Database.ExecuteSqlRaw(SqlQueryBuilder.DropTable(tableInfo.FullTempTableName, tableInfo.BulkConfig.UseTempDB));
            }

            string providerName = context.Database.ProviderName;
            // -- SQL Server --
            if (providerName.EndsWith(DbServer.SqlServer.ToString()))
            {
                context.Database.ExecuteSqlRaw(SqlQueryBuilder.CreateTableCopy(tableInfo.FullTableName, tableInfo.FullTempTableName, tableInfo));
                try
                {
                    Insert(context, type, entities, tableInfo, progress);

                    tableInfo.PropertyColumnNamesDict = tableInfo.OutputPropertyColumnNamesDict;

                    var sqlQuery = SqlQueryBuilder.SelectJoinTable(tableInfo);

                    tableInfo.PropertyColumnNamesDict = previousPropertyColumnNamesDict;

                    List<T> existingEntities;
                    if (typeof(T) == type)
                    {
                        Expression<Func<DbContext, IQueryable<T>>> expression = tableInfo.GetQueryExpression<T>(sqlQuery, false);
                        var compiled = EF.CompileQuery(expression); // instead using Compiled queries
                        existingEntities = compiled(context).ToList();
                    }
                    else
                    {
                        Expression<Func<DbContext, IEnumerable>> expression = tableInfo.GetQueryExpression(type, sqlQuery, false);
                        var compiled = EF.CompileQuery(expression); // instead using Compiled queries
                        existingEntities = compiled(context).Cast<T>().ToList();
                    }

                    tableInfo.UpdateReadEntities(type, entities, existingEntities);
                }
                finally
                {
                    if (!tableInfo.BulkConfig.UseTempDB)
                        context.Database.ExecuteSqlRaw(SqlQueryBuilder.DropTable(tableInfo.FullTempTableName, tableInfo.BulkConfig.UseTempDB));
                }
            }
            // -- Sqlite --
            else if (providerName.EndsWith(DbServer.Sqlite.ToString()))
            {
                throw new NotImplementedException();
            }
            else
            {
                throw new SqlProviderNotSupportedException(providerName);
            }
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
            Dictionary<string, string> previousPropertyColumnNamesDict = tableInfo.ConfigureBulkReadTableInfo(context);

            var dropTempTableIfExists = tableInfo.BulkConfig.UseTempDB;

            if (dropTempTableIfExists)
            {
                await context.Database.ExecuteSqlRawAsync(SqlQueryBuilder.DropTable(tableInfo.FullTempTableName, tableInfo.BulkConfig.UseTempDB)).ConfigureAwait(false);
            }

            string providerName = context.Database.ProviderName;
            // -- SQL Server --
            if (providerName.EndsWith(DbServer.SqlServer.ToString()))
            {
                await context.Database.ExecuteSqlRawAsync(SqlQueryBuilder.CreateTableCopy(tableInfo.FullTableName, tableInfo.FullTempTableName, tableInfo), cancellationToken).ConfigureAwait(false);
                try
                {
                    await InsertAsync(context, type, entities, tableInfo, progress, cancellationToken).ConfigureAwait(false);

                    tableInfo.PropertyColumnNamesDict = previousPropertyColumnNamesDict;

                    var sqlQuery = SqlQueryBuilder.SelectJoinTable(tableInfo);

                    tableInfo.PropertyColumnNamesDict = previousPropertyColumnNamesDict;

                    List<T> existingEntities;
                    if (typeof(T) == type)
                    {
                        Expression<Func<DbContext, IQueryable<T>>> expression = tableInfo.GetQueryExpression<T>(sqlQuery, false);
                        var compiled = EF.CompileQuery(expression); // instead using Compiled queries
                        existingEntities = compiled(context).ToList();
                    }
                    else
                    {
                        Expression<Func<DbContext, IEnumerable>> expression = tableInfo.GetQueryExpression(type, sqlQuery, false);
                        var compiled = EF.CompileQuery(expression); // instead using Compiled queries
                        existingEntities = compiled(context).Cast<T>().ToList();
                    }

                    tableInfo.UpdateReadEntities(type, entities, existingEntities);
                }
                finally
                {
                    if (!tableInfo.BulkConfig.UseTempDB)
                        await context.Database.ExecuteSqlRawAsync(SqlQueryBuilder.DropTable(tableInfo.FullTempTableName, tableInfo.BulkConfig.UseTempDB), cancellationToken).ConfigureAwait(false);
                }
            }
            // -- Sqlite --
            else if (providerName.EndsWith(DbServer.Sqlite.ToString()))
            {
                throw new NotImplementedException();
            }
            else
            {
                throw new SqlProviderNotSupportedException(providerName);
            }
        }

        public static void Truncate(DbContext context, TableInfo tableInfo)
        {
            string providerName = context.Database.ProviderName;
            // -- SQL Server --
            if (providerName.EndsWith(DbServer.SqlServer.ToString()))
            {
                context.Database.ExecuteSqlRaw(SqlQueryBuilder.TruncateTable(tableInfo.FullTableName));

            }
            // -- Sqlite --
            else if (providerName.EndsWith(DbServer.Sqlite.ToString()))
            {
                context.Database.ExecuteSqlRaw(SqlQueryBuilder.DeleteTable(tableInfo.FullTableName));
            }
            else
            {
                throw new SqlProviderNotSupportedException(providerName);
            }
        }

        public static async Task TruncateAsync(DbContext context, TableInfo tableInfo)
        {
            string providerName = context.Database.ProviderName;
            // -- SQL Server --
            if (providerName.EndsWith(DbServer.SqlServer.ToString()))
            {
                await context.Database.ExecuteSqlRawAsync(SqlQueryBuilder.TruncateTable(tableInfo.FullTableName));

            }
            // -- Sqlite --
            else if (providerName.EndsWith(DbServer.Sqlite.ToString()))
            {
                context.Database.ExecuteSqlRaw(SqlQueryBuilder.DeleteTable(tableInfo.FullTableName));
            }
            else
            {
                throw new SqlProviderNotSupportedException(providerName);
            }
        }

        private static void SetProgress(ref int rowsCopied, int entitiesCount, BulkConfig bulkConfig, Action<decimal> progress)
        {
            if (progress != null && bulkConfig.NotifyAfter != null && bulkConfig.NotifyAfter != 0)
            {
                rowsCopied++;

                if (rowsCopied == entitiesCount || rowsCopied % bulkConfig.NotifyAfter == 0)
                {
                    progress.Invoke(GetProgress(entitiesCount, rowsCopied));
                }
            }
        }

        internal static decimal GetProgress(int entitiesCount, long rowsCopied)
        {
            return (decimal)(Math.Floor(rowsCopied * 10000D / entitiesCount) / 10000);
        }
        #endregion

        #region DataTable
        internal static DataTable GetDataTable<T>(DbContext context, Type type, IList<T> entities, SqlBulkCopy sqlBulkCopy, TableInfo tableInfo)
        {
            var dataTable = new DataTable();
            var columnsDict = new Dictionary<string, object>();
            var ownedEntitiesMappedProperties = new HashSet<string>();

            type = tableInfo.HasAbstractList ? entities[0].GetType() : type;
            var entityType = context.Model.FindEntityType(type);
            var entityPropertiesDict = entityType.GetProperties().Where(a => tableInfo.PropertyColumnNamesDict.ContainsKey(a.Name)).ToDictionary(a => a.Name, a => a);
            var entityNavigationOwnedDict = entityType.GetNavigations().Where(a => a.GetTargetType().IsOwned()).ToDictionary(a => a.Name, a => a);
            var properties = type.GetProperties();
            var discriminatorColumn = tableInfo.ShadowProperties.Count == 0 ? null : tableInfo.ShadowProperties.ElementAt(0);

            foreach (var property in properties)
            {
                if (entityPropertiesDict.ContainsKey(property.Name))
                {
                    var propertyEntityType = entityPropertiesDict[property.Name];
                    string columnName = propertyEntityType.GetColumnName();

                    var isConvertible = tableInfo.ConvertibleProperties.ContainsKey(columnName);
                    var propertyType = isConvertible ? tableInfo.ConvertibleProperties[columnName].ProviderClrType : property.PropertyType;

                    var underlyingType = Nullable.GetUnderlyingType(propertyType);
                    if (underlyingType != null)
                    {
                        propertyType = underlyingType;
                    }

                    dataTable.Columns.Add(columnName, propertyType);
                    columnsDict.Add(property.Name, null);
                }
                else if (entityNavigationOwnedDict.ContainsKey(property.Name)) // isOWned
                {
                    Type navOwnedType = type.Assembly.GetType(property.PropertyType.FullName);

                    var ownedEntityType = context.Model.FindEntityType(property.PropertyType);
                    if (ownedEntityType == null)
                    {
                        ownedEntityType = context.Model.GetEntityTypes().SingleOrDefault(a => a.DefiningNavigationName == property.Name && a.DefiningEntityType.Name == entityType.Name);
                    }
                    var ownedEntityProperties = ownedEntityType.GetProperties().ToList();
                    var ownedEntityPropertyNameColumnNameDict = new Dictionary<string, string>();

                    foreach (var ownedEntityProperty in ownedEntityProperties)
                    {
                        if (!ownedEntityProperty.IsPrimaryKey())
                        {
                            string columnName = ownedEntityProperty.GetColumnName();
                            if (tableInfo.PropertyColumnNamesDict.ContainsValue(columnName))
                            {
                                ownedEntityPropertyNameColumnNameDict.Add(ownedEntityProperty.Name, columnName);
                                ownedEntitiesMappedProperties.Add(property.Name + "_" + ownedEntityProperty.Name);
                            }
                        }
                    }

                    var innerProperties = property.PropertyType.GetProperties();
                    if (!tableInfo.LoadOnlyPKColumn)
                    {
                        foreach (var innerProperty in innerProperties)
                        {
                            if (ownedEntityPropertyNameColumnNameDict.ContainsKey(innerProperty.Name))
                            {
                                string columnName = ownedEntityPropertyNameColumnNameDict[innerProperty.Name];
                                var ownedPropertyType = Nullable.GetUnderlyingType(innerProperty.PropertyType) ?? innerProperty.PropertyType;
                                dataTable.Columns.Add(columnName, ownedPropertyType);
                                columnsDict.Add(property.Name + "_" + innerProperty.Name, null);
                            }
                        }
                    }
                }
            }
            if (discriminatorColumn != null)
            {
                dataTable.Columns.Add(discriminatorColumn, typeof(string));
                columnsDict.Add(discriminatorColumn, type.Name);
            }

            foreach (var entity in entities)
            {
                foreach (var property in properties)
                {
                    var propertyValue = property.GetValue(entity, null);

                    if (entityPropertiesDict.ContainsKey(property.Name))
                    {
                        string columnName = entityPropertiesDict[property.Name].GetColumnName();
                        if (tableInfo.ConvertibleProperties.ContainsKey(columnName))
                        {
                            propertyValue = tableInfo.ConvertibleProperties[columnName].ConvertToProvider.Invoke(propertyValue);
                        }
                    }

                    if (entityPropertiesDict.ContainsKey(property.Name))
                    {
                        columnsDict[property.Name] = propertyValue;
                    }
                    else if (entityNavigationOwnedDict.ContainsKey(property.Name) && !tableInfo.LoadOnlyPKColumn)
                    {
                        var ownedProperties = property.PropertyType.GetProperties().Where(a => ownedEntitiesMappedProperties.Contains(property.Name + "_" + a.Name));
                        foreach (var ownedProperty in ownedProperties)
                        {
                            columnsDict[property.Name + "_" + ownedProperty.Name] = propertyValue == null ? null : ownedProperty.GetValue(propertyValue, null);
                        }
                    }
                }
                var record = columnsDict.Values.ToArray();
                dataTable.Rows.Add(record);
            }

            foreach (DataColumn item in dataTable.Columns)  //Add mapping
            {
                sqlBulkCopy.ColumnMappings.Add(item.ColumnName, item.ColumnName);
            }
            return dataTable;
        }
        #endregion

        #region SqliteData
        internal static SqliteCommand GetSqliteCommand<T>(DbContext context, Type type, IList<T> entities, TableInfo tableInfo, SqliteConnection connection, SqliteTransaction transaction)
        {
            SqliteCommand command = connection.CreateCommand();
            command.Transaction = transaction;

            OperationType operationType = tableInfo.BulkConfig.OperationType;

            if (operationType == OperationType.Insert)
            {
                command.CommandText = SqlQueryBuilderSqlite.InsertIntoTable(tableInfo, OperationType.Insert);
            }
            else if (operationType == OperationType.InsertOrUpdate)
            {
                command.CommandText = SqlQueryBuilderSqlite.InsertIntoTable(tableInfo, OperationType.InsertOrUpdate);
            }
            else if (operationType == OperationType.InsertOrUpdateDelete)
            {
                throw new NotSupportedException("Sqlite supports only UPSERT(analog for MERGE WHEN MATCHED) but does not have functionality to do: 'WHEN NOT MATCHED BY SOURCE THEN DELETE'" +
                                                "What can be done is to read all Data, find rows that are not is input List, then with those do the BulkDelete.");
            }
            else if (operationType == OperationType.Update)
            {
                command.CommandText = SqlQueryBuilderSqlite.UpdateSetTable(tableInfo);
            }
            else if (operationType == OperationType.Delete)
            {
                command.CommandText = SqlQueryBuilderSqlite.DeleteFromTable(tableInfo);
            }

            type = tableInfo.HasAbstractList ? entities[0].GetType() : type;
            var entityType = context.Model.FindEntityType(type);
            var entityPropertiesDict = entityType.GetProperties().Where(a => tableInfo.PropertyColumnNamesDict.ContainsKey(a.Name)).ToDictionary(a => a.Name, a => a);
            var properties = type.GetProperties();

            foreach (var property in properties)
            {
                if (entityPropertiesDict.ContainsKey(property.Name))
                {
                    var propertyEntityType = entityPropertiesDict[property.Name];
                    string columnName = propertyEntityType.GetColumnName();
                    var propertyType = Nullable.GetUnderlyingType(property.PropertyType) ?? property.PropertyType;

                    /*var sqliteType = SqliteType.Text; // "String" || "Decimal" || "DateTime"
                    if (propertyType.Name == "Int16" || propertyType.Name == "Int32" || propertyType.Name == "Int64")
                        sqliteType = SqliteType.Integer;
                    if (propertyType.Name == "Float" || propertyType.Name == "Double")
                        sqliteType = SqliteType.Real;
                    if (propertyType.Name == "Guid" )
                        sqliteType = SqliteType.Blob; */

                    var parameter = new SqliteParameter($"@{columnName}", propertyType); // ,sqliteType // ,null
                    command.Parameters.Add(parameter);
                }
            }

            var shadowProperties = tableInfo.ShadowProperties;
            foreach (var shadowProperty in shadowProperties)
            {
                var parameter = new SqliteParameter($"@{shadowProperty}", typeof(string));
                command.Parameters.Add(parameter);
            }

            command.Prepare(); // Not Required (check if same efficiency when removed)
            return command;
        }

        internal static void LoadSqliteValues<T>(TableInfo tableInfo, TypeAccessor typeAccessor, T entity, SqliteCommand command)
        {
            var PropertyColumnsDict = tableInfo.PropertyColumnNamesDict;
            foreach (var propertyColumn in PropertyColumnsDict)
            {
                object value;
                if (!tableInfo.ShadowProperties.Contains(propertyColumn.Key))
                {
                    if (propertyColumn.Key.Contains(".")) // ToDo: change IF clause to check for NavigationProperties, optimise, integrate with same code segment from LoadData method
                    {
                        var subProperties = propertyColumn.Key.Split('.');
                        var subPropertiesLevel1 = typeAccessor[entity, subProperties[0]];

                        var propertyType = Nullable.GetUnderlyingType(subPropertiesLevel1.GetType()) ?? subPropertiesLevel1.GetType();
                        if (!command.Parameters.Contains("@" + propertyColumn.Value))
                        {
                            var parameter = new SqliteParameter($"@{propertyColumn.Value}", propertyType);
                            command.Parameters.Add(parameter);
                        }

                        if (subPropertiesLevel1 == null)
                            value = null;
                        else
                            value = subPropertiesLevel1.GetType().GetProperty(subProperties[1]).GetValue(subPropertiesLevel1);
                    }
                    else
                    {
                        value = typeAccessor[entity, propertyColumn.Key];
                    }
                }
                else // IsShadowProperty
                {
                    value = entity.GetType().Name;
                }

                if (tableInfo.ConvertibleProperties.ContainsKey(propertyColumn.Key))
                {
                    value = tableInfo.ConvertibleProperties[propertyColumn.Key].ConvertToProvider.Invoke(value);
                }

                command.Parameters[$"@{propertyColumn.Value}"].Value = value ?? DBNull.Value;
            }
        }
        #endregion

        #region Connection
        internal static DbConnection OpenAndGetSqlConnection(DbContext context, BulkConfig config)
        {
            context.Database.OpenConnection();

            return context.GetUnderlyingConnection(config);
        }

        internal static async Task<DbConnection> OpenAndGetSqlConnectionAsync(DbContext context, BulkConfig config, CancellationToken cancellationToken)
        {
            await context.Database.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);

            return context.GetUnderlyingConnection(config);
        }

        internal static SqliteConnection OpenAndGetSqliteConnection(DbContext context, BulkConfig bulkConfig)
        {
            context.Database.OpenConnection();

            return bulkConfig.SqliteConnection ?? (SqliteConnection)context.Database.GetDbConnection();
        }

        internal static async Task<SqliteConnection> OpenAndGetSqliteConnectionAsync(DbContext context, BulkConfig bulkConfig, CancellationToken cancellationToken)
        {
            await context.Database.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
            return bulkConfig.SqliteConnection ?? (SqliteConnection)context.Database.GetDbConnection();
        }

        private static SqlBulkCopy GetSqlBulkCopy(SqlConnection sqlConnection, IDbContextTransaction transaction, BulkConfig config)
        {
            var sqlBulkCopyOptions = config.SqlBulkCopyOptions;
            if (transaction == null)
            {
                return new SqlBulkCopy(sqlConnection, sqlBulkCopyOptions, null);
            }
            else
            {
                var sqlTransaction = (SqlTransaction)transaction.GetUnderlyingTransaction(config);
                return new SqlBulkCopy(sqlConnection, sqlBulkCopyOptions, sqlTransaction);
            }
        }
        #endregion
    }
}
