using EFCore.BulkExtensions.SqlAdapters;
using EFCore.BulkExtensions.SQLAdapters.SQLServer;
using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;

namespace EFCore.BulkExtensions.SQLAdapters.SQLite;
/// <inheritdoc/>
public class SqliteOperationsAdapter : ISqlOperationsAdapter
{
    /// <inheritdoc/>
    #region Methods
    // Insert
    public void Insert<T>(DbContext context, Type type, IList<T> entities, TableInfo tableInfo, Action<decimal>? progress)
    {
        InsertAsync(context, type, entities, tableInfo, progress, CancellationToken.None, isAsync: false).GetAwaiter().GetResult();
    }


    /// <inheritdoc/>
    public async Task InsertAsync<T>(DbContext context, Type type, IList<T> entities, TableInfo tableInfo, Action<decimal>? progress, CancellationToken cancellationToken)
    {
        await InsertAsync(context, type, entities, tableInfo, progress, cancellationToken, isAsync: true).ConfigureAwait(false);
    }
    
    /// <inheritdoc/>
    public async Task InsertAsync<T>(DbContext context, Type type, IList<T> entities, TableInfo tableInfo, Action<decimal>? progress, CancellationToken cancellationToken, bool isAsync)

    /// <inheritdoc/>
    public async Task InsertAsync<T>(DbContext context, Type type, IList<T> entities, TableInfo tableInfo, Action<decimal> progress, CancellationToken cancellationToken, bool isAsync)
    {
        SqliteConnection? connection = tableInfo.SqliteConnection;
        if (connection == null)
        {
            connection = isAsync ? await OpenAndGetSqliteConnectionAsync(context, cancellationToken).ConfigureAwait(false)
                                 : OpenAndGetSqliteConnection(context);
        }
        bool doExplicitCommit = false;

        try
        {
            if (context.Database.CurrentTransaction == null)
            {
                //context.Database.UseTransaction(connection.BeginTransaction());
                doExplicitCommit = true;
            }

            SqliteTransaction? transaction = tableInfo.SqliteTransaction;
            if (transaction == null)
            {
                var dbTransaction = doExplicitCommit ? connection.BeginTransaction()
                                                     : context.Database.CurrentTransaction?.GetUnderlyingTransaction(tableInfo.BulkConfig);

                transaction = (SqliteTransaction?)dbTransaction;
            }
            else
            {
                doExplicitCommit = false;
            }

            var command = GetSqliteCommand(context, type, entities, tableInfo, connection, transaction);

            type = tableInfo.HasAbstractList ? entities[0]!.GetType() : type;
            int rowsCopied = 0;

            foreach (var item in entities)
            {
                LoadSqliteValues(tableInfo, item, command, context);
                if (isAsync)
                {
                    await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
                }
                else
                {
                    command.ExecuteNonQuery();
                }
                ProgressHelper.SetProgress(ref rowsCopied, entities.Count, tableInfo.BulkConfig, progress);
            }
            if (doExplicitCommit)
            {
                transaction?.Commit();
            }
        }
        finally
        {
            if (doExplicitCommit)
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
        }
    }

    // Merge
    /// <inheritdoc/>
    public void Merge<T>(DbContext context, Type type, IList<T> entities, TableInfo tableInfo, OperationType operationType, Action<decimal>? progress) where T : class
    {
        MergeAsync(context, type, entities, tableInfo, operationType, progress, CancellationToken.None, isAsync: false).GetAwaiter().GetResult();
    }

    /// <inheritdoc/>
    public async Task MergeAsync<T>(DbContext context, Type type, IList<T> entities, TableInfo tableInfo, OperationType operationType, Action<decimal>? progress, CancellationToken cancellationToken) where T : class
    {
        await MergeAsync(context, type, entities, tableInfo, operationType, progress, cancellationToken, isAsync: true);
    }

    /// <inheritdoc/>
    protected async Task MergeAsync<T>(DbContext context, Type type, IList<T> entities, TableInfo tableInfo, OperationType operationType, Action<decimal>? progress, CancellationToken cancellationToken, bool isAsync) where T : class
    {
        SqliteConnection connection = isAsync ? await OpenAndGetSqliteConnectionAsync(context, cancellationToken).ConfigureAwait(false)
                                                    : OpenAndGetSqliteConnection(context);
        bool doExplicitCommit = false;

        try
        {
            if (context.Database.CurrentTransaction == null)
            {
                //context.Database.UseTransaction(connection.BeginTransaction());
                doExplicitCommit = true;
            }
            var dbTransaction = doExplicitCommit ? connection.BeginTransaction()
                                                 : context.Database.CurrentTransaction?.GetUnderlyingTransaction(tableInfo.BulkConfig);
            var transaction = (SqliteTransaction?)dbTransaction;

            var command = GetSqliteCommand(context, type, entities, tableInfo, connection, transaction);

            type = tableInfo.HasAbstractList ? entities[0].GetType() : type;
            int rowsCopied = 0;

            foreach (var item in entities)
            {
                LoadSqliteValues(tableInfo, item, command, context);
                if (isAsync)
                {
                    await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
                }
                else
                {
                    command.ExecuteNonQuery();
                }
                ProgressHelper.SetProgress(ref rowsCopied, entities.Count, tableInfo.BulkConfig, progress);
            }

            if (operationType == OperationType.Insert && tableInfo.BulkConfig.SetOutputIdentity && tableInfo.IdentityColumnName != null) // For Sqlite Identity can be set by Db only with pure Insert method
            {
                command.CommandText = SqlQueryBuilderSqlite.SelectLastInsertRowId();

                object? lastRowIdScalar = isAsync ? await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false)
                                                       : command.ExecuteScalar();

                SetIdentityForOutput(entities, tableInfo, lastRowIdScalar);
            }

            if (doExplicitCommit)
            {
                transaction?.Commit();
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
    }

    // Read
    /// <inheritdoc/>
    public void Read<T>(DbContext context, Type type, IList<T> entities, TableInfo tableInfo, Action<decimal>? progress) where T : class
    {
        ReadAsync(context, type, entities, tableInfo, progress, CancellationToken.None, isAsync: false).GetAwaiter().GetResult();
    }

    /// <inheritdoc/>
    public async Task ReadAsync<T>(DbContext context, Type type, IList<T> entities, TableInfo tableInfo, Action<decimal>? progress, CancellationToken cancellationToken) where T : class
    {
        await ReadAsync(context, type, entities, tableInfo, progress, cancellationToken, isAsync: true).ConfigureAwait(false);
    }

    /// <inheritdoc/>
    protected async Task ReadAsync<T>(DbContext context, Type type, IList<T> entities, TableInfo tableInfo, Action<decimal>? progress, CancellationToken cancellationToken, bool isAsync) where T : class
    {
        SqliteConnection connection = isAsync ? await OpenAndGetSqliteConnectionAsync(context, cancellationToken).ConfigureAwait(false)
                                                    : OpenAndGetSqliteConnection(context);
        bool doExplicitCommit = false;
        SqliteTransaction? transaction = null;

        try
        {
            if (context.Database.CurrentTransaction == null)
            {
                //context.Database.UseTransaction(connection.BeginTransaction());
                doExplicitCommit = true;
            }

            transaction = doExplicitCommit ? connection.BeginTransaction()
                                           : (SqliteTransaction?)context.Database.CurrentTransaction?.GetUnderlyingTransaction(tableInfo.BulkConfig);

            SqliteCommand command = connection.CreateCommand();
            command.Transaction = transaction;

            // CREATE
            command.CommandText = SqlQueryBuilderSqlite.CreateTableCopy(tableInfo.FullTableName, tableInfo.FullTempTableName);
            if (isAsync)
            {
                await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            }
            else
            {
                command.ExecuteNonQuery();
            }

            tableInfo.BulkConfig.OperationType = OperationType.Insert;
            tableInfo.InsertToTempTable = true;
            tableInfo.SqliteConnection = connection;
            tableInfo.SqliteTransaction = transaction;
            // INSERT
            if (isAsync)
            {
                await InsertAsync(context, type, entities, tableInfo, progress, cancellationToken, isAsync: true).ConfigureAwait(false);
            }
            else
            {
                InsertAsync(context, type, entities, tableInfo, progress, cancellationToken, isAsync: false).GetAwaiter().GetResult();
            }

            // JOIN
            List<T> existingEntities;
            var sqlSelectJoinTable = SqlQueryBuilder.SelectJoinTable(tableInfo);
            Expression<Func<DbContext, IQueryable<T>>> expression = tableInfo.GetQueryExpression<T>(sqlSelectJoinTable, false);
            var compiled = EF.CompileQuery(expression); // instead using Compiled queries
            existingEntities = compiled(context).ToList();

            tableInfo.UpdateReadEntities(entities, existingEntities, context);

            // DROP
            command.CommandText = SqlQueryBuilderSqlite.DropTable(tableInfo.FullTempTableName);
            if (isAsync)
            {
                await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            }
            else
            {
                command.ExecuteNonQuery();
            }

            if (doExplicitCommit)
            {
                transaction?.Commit();
            }
        }
        finally
        {
            if (doExplicitCommit)
            {
                if (isAsync)
                {
                    if (transaction is not null)
                    {
                        await transaction.DisposeAsync();
                    }

                    await context.Database.CloseConnectionAsync().ConfigureAwait(false);
                }
                else
                {
                    transaction?.Dispose();
                    context.Database.CloseConnection();
                }
            }
        }
    }

    /// <inheritdoc/>
    public void Truncate(DbContext context, TableInfo tableInfo)
    {
        string sql = SqlQueryBuilder.DeleteTable(tableInfo.FullTableName);
        context.Database.ExecuteSqlRaw(sql);
    }

    /// <inheritdoc/>
    public async Task TruncateAsync(DbContext context, TableInfo tableInfo, CancellationToken cancellationToken)
    {
        string sql = SqlQueryBuilder.DeleteTable(tableInfo.FullTableName);
        await context.Database.ExecuteSqlRawAsync(sql, cancellationToken).ConfigureAwait(false);
    }
    #endregion

    #region Connection
    internal static async Task<SqliteConnection> OpenAndGetSqliteConnectionAsync(DbContext context, CancellationToken cancellationToken)
    {
        await context.Database.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        return (SqliteConnection)context.Database.GetDbConnection();
    }

    internal static SqliteConnection OpenAndGetSqliteConnection(DbContext context)
    {
        context.Database.OpenConnection();

        return (SqliteConnection)context.Database.GetDbConnection();
    }
    #endregion

    #region SqliteData
    internal static SqliteCommand GetSqliteCommand<T>(DbContext context, Type? type, IList<T> entities, TableInfo tableInfo, SqliteConnection connection, SqliteTransaction? transaction)
    {
        SqliteCommand command = connection.CreateCommand();
        command.Transaction = transaction;

        var operationType = tableInfo.BulkConfig.OperationType;

        switch (operationType)
        {
            case OperationType.Insert:
                command.CommandText = SqlQueryBuilderSqlite.InsertIntoTable(tableInfo, OperationType.Insert);
                break;
            case OperationType.InsertOrUpdate:
                command.CommandText = SqlQueryBuilderSqlite.InsertIntoTable(tableInfo, OperationType.InsertOrUpdate);
                break;
            case OperationType.InsertOrUpdateOrDelete:
                throw new NotSupportedException("'BulkInsertOrUpdateDelete' not supported for Sqlite. Sqlite has only UPSERT statement (analog for MERGE WHEN MATCHED) but no functionality for: 'WHEN NOT MATCHED BY SOURCE THEN DELETE'." +
                                                " Another way to achieve this is to BulkRead existing data from DB, split list into sublists and call separately Bulk methods for Insert, Update, Delete.");
            case OperationType.Update:
                command.CommandText = SqlQueryBuilderSqlite.UpdateSetTable(tableInfo);
                break;
            case OperationType.Delete:
                command.CommandText = SqlQueryBuilderSqlite.DeleteFromTable(tableInfo);
                break;
        }

        type = tableInfo.HasAbstractList ? entities[0]?.GetType() : type;
        if (type is null)
        {
            throw new ArgumentException("Unable to determine entity type");
        }
        var entityType = context.Model.FindEntityType(type);
        var entityPropertiesDict = entityType?.GetProperties().Where(a => tableInfo.PropertyColumnNamesDict.ContainsKey(a.Name)).ToDictionary(a => a.Name, a => a);
        var properties = type.GetProperties(BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic);

        foreach (var property in properties)
        {
            if (entityPropertiesDict?.ContainsKey(property.Name) ?? false)
            {
                var propertyEntityType = entityPropertiesDict[property.Name];
                string? columnName = propertyEntityType.GetColumnName(tableInfo.ObjectIdentifier);
                var propertyType = Nullable.GetUnderlyingType(property.PropertyType) ?? property.PropertyType;

                //SqliteType(CpropertyType.Name): Text(String, Decimal, DateTime); Integer(Int16, Int32, Int64) Real(Float, Double) Blob(Guid)
                var parameter = new SqliteParameter($"@{property.Name}", propertyType); // ,sqliteType // ,null //()
                command.Parameters.Add(parameter);
            }
        }

        var shadowProperties = tableInfo.ShadowProperties;
        foreach (var shadowProperty in shadowProperties)
        {
            var parameter = new SqliteParameter($"@{shadowProperty}", typeof(string));
            command.Parameters.Add(parameter);
        }

        command.Prepare(); // Not Required but called for efficiency (prepared should be little faster)
        return command;
    }

    internal static void LoadSqliteValues<T>(TableInfo tableInfo, T? entity, SqliteCommand command, DbContext dbContext)
    {
        var propertyColumnsDict = tableInfo.PropertyColumnNamesDict;
        foreach (var propertyColumn in propertyColumnsDict)
        {
            var isShadowProperty = tableInfo.ShadowProperties.Contains(propertyColumn.Key);
            string parameterName = propertyColumn.Key.Replace(".", "_");
            object? value;
            if (!isShadowProperty)
            {
                if (propertyColumn.Key.Contains('.')) // ToDo: change IF clause to check for NavigationProperties, optimise, integrate with same code segment from LoadData method
                {
                    var ownedPropertyNameList = propertyColumn.Key.Split('.');
                    var ownedPropertyName = ownedPropertyNameList[0];
                    var subPropertyName = ownedPropertyNameList[1];
                    var ownedFastProperty = tableInfo.FastPropertyDict[ownedPropertyName];
                    var ownedProperty = ownedFastProperty.Property;

                    var propertyType = Nullable.GetUnderlyingType(ownedProperty.GetType()) ?? ownedProperty.GetType();
                    if (!command.Parameters.Contains("@" + parameterName))
                    {
                        var parameter = new SqliteParameter($"@{parameterName}", propertyType);
                        command.Parameters.Add(parameter);
                    }

                    if (ownedProperty == null)
                    {
                        value = null;
                    }
                    else
                    {
                        var ownedPropertyValue = entity == null ? null : tableInfo.FastPropertyDict[ownedPropertyName].Get(entity);
                        var subPropertyFullName = $"{ownedPropertyName}_{subPropertyName}";
                        value = ownedPropertyValue == null ? null : tableInfo.FastPropertyDict[subPropertyFullName]?.Get(ownedPropertyValue);
                    }
                }
                else
                {
                    value = entity is null ? null : tableInfo.FastPropertyDict[propertyColumn.Key].Get(entity);
                }
            }
            else
            {
                if (tableInfo.BulkConfig.EnableShadowProperties)
                {
                    if (tableInfo.BulkConfig.ShadowPropertyValue == null)
                    {
                        value = entity is null ? null : dbContext.Entry(entity).Property(propertyColumn.Key).CurrentValue; // Get the shadow property value
                    }
                    else
                    {
                        value = entity is null ? null : tableInfo.BulkConfig.ShadowPropertyValue(entity, propertyColumn.Key);
                    }


                }
                else
                {
                    value = entity is null ? null : dbContext.Entry(entity).Metadata.GetDiscriminatorValue(); // Set the value for the discriminator column
                }
            }

            if (tableInfo.ConvertibleColumnConverterDict.ContainsKey(propertyColumn.Key) && value != DBNull.Value)
            {
                value = tableInfo.ConvertibleColumnConverterDict[propertyColumn.Key].ConvertToProvider.Invoke(value);
            }

            command.Parameters[$"@{parameterName}"].Value = value ?? DBNull.Value;
        }
    }

    /// <inheritdoc/>
    public void SetIdentityForOutput<T>(IList<T> entities, TableInfo tableInfo, object? lastRowIdScalar)
    {
        long counter = (long?)lastRowIdScalar ?? 0;

        string identityPropertyName = tableInfo.PropertyColumnNamesDict.SingleOrDefault(a => a.Value == tableInfo.IdentityColumnName).Key;
        FastProperty identityFastProperty = tableInfo.FastPropertyDict[identityPropertyName];

        string idTypeName = identityFastProperty.Property.PropertyType.Name;
        object? idValue = null;
        for (int i = entities.Count - 1; i >= 0; i--)
        {
            idValue = idTypeName switch
            {
                "Int64" => counter, // long is default
                "UInt64" => (ulong)counter,
                "Int32" => (int)counter,
                "UInt32" => (uint)counter,
                "Int16" => (short)counter,
                "UInt16" => (ushort)counter,
                "Byte" => (byte)counter,
                "SByte" => (sbyte)counter,
                _ => counter,
            };
            if (entities[i] is not null)
            {
                identityFastProperty.Set(entities[i]!, idValue);
            }

            counter--;
        }
    }
    #endregion
}
