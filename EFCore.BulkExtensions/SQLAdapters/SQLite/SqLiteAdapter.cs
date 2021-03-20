using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using EFCore.BulkExtensions.SqlAdapters;
using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;

namespace EFCore.BulkExtensions.SQLAdapters.SQLite
{
    public class SqLiteOperationsAdapter: ISqlOperationsAdapter
    {
        public void Insert<T>(DbContext context, Type type, IList<T> entities, TableInfo tableInfo, Action<decimal> progress)
        {
            var connection = OpenAndGetSqliteConnection(context, tableInfo.BulkConfig);
            bool doExplicitCommit = false;

            try
            {
                if (context.Database.CurrentTransaction == null)
                {
                    //context.Database.UseTransaction(connection.BeginTransaction());
                    doExplicitCommit = true;
                }
                var transaction = (SqliteTransaction)(context.Database.CurrentTransaction == null ?
                    connection.BeginTransaction() :
                    context.Database.CurrentTransaction.GetUnderlyingTransaction(tableInfo.BulkConfig));

                var command = GetSqliteCommand(context, type, entities, tableInfo, connection, transaction);

                type = tableInfo.HasAbstractList ? entities[0].GetType() : type;
                int rowsCopied = 0;
                foreach (var item in entities)
                {
                    LoadSqliteValues(tableInfo, item, command, context);
                    command.ExecuteNonQuery();
                    ProgressHelper.SetProgress(ref rowsCopied, entities.Count, tableInfo.BulkConfig, progress);
                }
                if (doExplicitCommit)
                {
                    transaction.Commit();
                }
            }
            finally
            {
                context.Database.CloseConnection();
            }
        }

        public async  Task InsertAsync<T>(DbContext context, Type type, IList<T> entities, TableInfo tableInfo, Action<decimal> progress, CancellationToken cancellationToken)
        {
            var connection = await OpenAndGetSqliteConnectionAsync(context, tableInfo.BulkConfig, cancellationToken).ConfigureAwait(false);
            bool doExplicitCommit = false;

            try
            {
                if (context.Database.CurrentTransaction == null)
                {
                    //context.Database.UseTransaction(connection.BeginTransaction());
                    doExplicitCommit = true;
                }
                var transaction = (SqliteTransaction)(context.Database.CurrentTransaction == null ?
                    connection.BeginTransaction() :
                    context.Database.CurrentTransaction.GetUnderlyingTransaction(tableInfo.BulkConfig));

                var command = GetSqliteCommand(context, type, entities, tableInfo, connection, transaction);

                type = tableInfo.HasAbstractList ? entities[0].GetType() : type;
                int rowsCopied = 0;

                foreach (var item in entities)
                {
                    LoadSqliteValues(tableInfo, item, command, context);
                    await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
                    ProgressHelper.SetProgress(ref rowsCopied, entities.Count, tableInfo.BulkConfig, progress);
                }
                if (doExplicitCommit)
                {
                    transaction.Commit();
                }
            }
            finally
            {
                await context.Database.CloseConnectionAsync().ConfigureAwait(false);
            }
        }

        public void Merge<T>(DbContext context, Type type, IList<T> entities, TableInfo tableInfo, OperationType operationType, Action<decimal> progress) where T : class
        {
            var connection = OpenAndGetSqliteConnection(context, tableInfo.BulkConfig);
            bool doExplicitCommit = false;

            try
            {
                if (context.Database.CurrentTransaction == null)
                {
                    //context.Database.UseTransaction(connection.BeginTransaction());
                    doExplicitCommit = true;
                }
                var transaction = (SqliteTransaction)(context.Database.CurrentTransaction == null ?
                                                        connection.BeginTransaction() :
                                                        context.Database.CurrentTransaction.GetUnderlyingTransaction(tableInfo.BulkConfig));

                var command = GetSqliteCommand(context, type, entities, tableInfo, connection, transaction);

                type = tableInfo.HasAbstractList ? entities[0].GetType() : type;
                int rowsCopied = 0;
                foreach (var item in entities)
                {
                    LoadSqliteValues(tableInfo, item, command, context);
                    command.ExecuteNonQuery();
                    ProgressHelper.SetProgress(ref rowsCopied, entities.Count, tableInfo.BulkConfig, progress);
                }

                if (operationType != OperationType.Delete && tableInfo.BulkConfig.SetOutputIdentity && tableInfo.IdentityColumnName != null)
                {
                    string identityPropertyName = tableInfo.PrimaryKeysPropertyColumnNameDict.SingleOrDefault(a => a.Value == tableInfo.IdentityColumnName).Key;
                    command.CommandText = SqlQueryBuilderSqlite.SelectLastInsertRowId();
                    long lastRowIdScalar = (long)command.ExecuteScalar();
                    var identityPropertyInteger = false;
                    var identityPropertyUnsigned = false;
                    var identityPropertyByte = false;
                    var identityPropertyShort = false;

                    if (tableInfo.FastPropertyDict[identityPropertyName].Property.PropertyType == typeof(ulong))
                    {
                        identityPropertyUnsigned = true;
                    }
                    else if (tableInfo.FastPropertyDict[identityPropertyName].Property.PropertyType == typeof(uint))
                    {
                        identityPropertyInteger = true;
                        identityPropertyUnsigned = true;
                    }
                    else if (tableInfo.FastPropertyDict[identityPropertyName].Property.PropertyType == typeof(int))
                    {
                        identityPropertyInteger = true;
                    }
                    else if (tableInfo.FastPropertyDict[identityPropertyName].Property.PropertyType == typeof(ushort))
                    {
                        identityPropertyShort = true;
                        identityPropertyUnsigned = true;
                    }
                    else if (tableInfo.FastPropertyDict[identityPropertyName].Property.PropertyType == typeof(short))
                    {
                        identityPropertyShort = true;
                    }
                    else if (tableInfo.FastPropertyDict[identityPropertyName].Property.PropertyType == typeof(byte))
                    {
                        identityPropertyByte = true;
                        identityPropertyUnsigned = true;
                    }
                    else if (tableInfo.FastPropertyDict[identityPropertyName].Property.PropertyType == typeof(sbyte))
                    {
                        identityPropertyByte = true;
                    }

                    for (int i = entities.Count - 1; i >= 0; i--)
                    {
                        if (identityPropertyByte)
                        {
                            if (identityPropertyUnsigned)
                                tableInfo.FastPropertyDict[identityPropertyName].Set(entities[i], (byte)lastRowIdScalar);
                            else
                                tableInfo.FastPropertyDict[identityPropertyName].Set(entities[i], (sbyte)lastRowIdScalar);
                        }
                        else if (identityPropertyShort)
                        {
                            if (identityPropertyUnsigned)
                                tableInfo.FastPropertyDict[identityPropertyName].Set(entities[i], (ushort)lastRowIdScalar);
                            else
                                tableInfo.FastPropertyDict[identityPropertyName].Set(entities[i], (short)lastRowIdScalar);
                        }
                        else if (identityPropertyInteger)
                        {
                            if (identityPropertyUnsigned)
                                tableInfo.FastPropertyDict[identityPropertyName].Set(entities[i], (uint)lastRowIdScalar);
                            else
                                tableInfo.FastPropertyDict[identityPropertyName].Set(entities[i], (int)lastRowIdScalar);
                        }
                        else
                        {
                            if (identityPropertyUnsigned)
                                tableInfo.FastPropertyDict[identityPropertyName].Set(entities[i], (ulong)lastRowIdScalar);
                            else
                                tableInfo.FastPropertyDict[identityPropertyName].Set(entities[i], lastRowIdScalar);
                        }

                        lastRowIdScalar--;
                    }
                }
                if (doExplicitCommit)
                {
                    transaction.Commit();
                }
            }
            finally
            {
                context.Database.CloseConnection();
            }
        }

        public async Task MergeAsync<T>(DbContext context, Type type, IList<T> entities, TableInfo tableInfo, OperationType operationType, Action<decimal> progress, CancellationToken cancellationToken) where T : class
        {
            var connection = await OpenAndGetSqliteConnectionAsync(context, tableInfo.BulkConfig, cancellationToken).ConfigureAwait(false);
            bool doExplicitCommit = false;

            try
            {
                if (context.Database.CurrentTransaction == null)
                {
                    //context.Database.UseTransaction(connection.BeginTransaction());
                    doExplicitCommit = true;
                }
                var transaction = (SqliteTransaction)(context.Database.CurrentTransaction == null ?
                                                        connection.BeginTransaction() :
                                                        context.Database.CurrentTransaction.GetUnderlyingTransaction(tableInfo.BulkConfig));

                var command = GetSqliteCommand(context, type, entities, tableInfo, connection, transaction);

                type = tableInfo.HasAbstractList ? entities[0].GetType() : type;
                int rowsCopied = 0;

                foreach (var item in entities)
                {
                    LoadSqliteValues(tableInfo, item, command, context);
                    await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
                    ProgressHelper.SetProgress(ref rowsCopied, entities.Count, tableInfo.BulkConfig, progress);
                }

                if (operationType != OperationType.Delete && tableInfo.BulkConfig.SetOutputIdentity && tableInfo.IdentityColumnName != null)
                {
                    command.CommandText = SqlQueryBuilderSqlite.SelectLastInsertRowId();
                    long lastRowIdScalar = (long)await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
                    string identityPropertyName = tableInfo.PropertyColumnNamesDict.SingleOrDefault(a => a.Value == tableInfo.IdentityColumnName).Key;

                    var identityPropertyInteger = false;
                    var identityPropertyUnsigned = false;
                    var identityPropertyByte = false;
                    var identityPropertyShort = false;

                    if (tableInfo.FastPropertyDict[identityPropertyName].Property.PropertyType == typeof(ulong))
                    {
                        identityPropertyUnsigned = true;
                    }
                    else if (tableInfo.FastPropertyDict[identityPropertyName].Property.PropertyType == typeof(uint))
                    {
                        identityPropertyInteger = true;
                        identityPropertyUnsigned = true;
                    }
                    else if (tableInfo.FastPropertyDict[identityPropertyName].Property.PropertyType == typeof(int))
                    {
                        identityPropertyInteger = true;
                    }
                    else if (tableInfo.FastPropertyDict[identityPropertyName].Property.PropertyType == typeof(ushort))
                    {
                        identityPropertyShort = true;
                        identityPropertyUnsigned = true;
                    }
                    else if (tableInfo.FastPropertyDict[identityPropertyName].Property.PropertyType == typeof(short))
                    {
                        identityPropertyShort = true;
                    }
                    else if (tableInfo.FastPropertyDict[identityPropertyName].Property.PropertyType == typeof(byte))
                    {
                        identityPropertyByte = true;
                        identityPropertyUnsigned = true;
                    }
                    else if (tableInfo.FastPropertyDict[identityPropertyName].Property.PropertyType == typeof(sbyte))
                    {
                        identityPropertyByte = true;
                    }

                    for (int i = entities.Count - 1; i >= 0; i--)
                    {
                        if (identityPropertyByte)
                        {
                            if (identityPropertyUnsigned)
                                tableInfo.FastPropertyDict[identityPropertyName].Set(entities[i], (byte)lastRowIdScalar);
                            else
                                tableInfo.FastPropertyDict[identityPropertyName].Set(entities[i], (sbyte)lastRowIdScalar);
                        }
                        else if (identityPropertyShort)
                        {
                            if (identityPropertyUnsigned)
                                tableInfo.FastPropertyDict[identityPropertyName].Set(entities[i], (ushort)lastRowIdScalar);
                            else
                                tableInfo.FastPropertyDict[identityPropertyName].Set(entities[i], (short)lastRowIdScalar);
                        }
                        else if (identityPropertyInteger)
                        {
                            if (identityPropertyUnsigned)
                                tableInfo.FastPropertyDict[identityPropertyName].Set(entities[i], (uint)lastRowIdScalar);
                            else
                                tableInfo.FastPropertyDict[identityPropertyName].Set(entities[i], (int)lastRowIdScalar);
                        }
                        else
                        {
                            if (identityPropertyUnsigned)
                                tableInfo.FastPropertyDict[identityPropertyName].Set(entities[i], (ulong)lastRowIdScalar);
                            else
                                tableInfo.FastPropertyDict[identityPropertyName].Set(entities[i], lastRowIdScalar);
                        }

                        lastRowIdScalar--;
                    }
                }
                if (doExplicitCommit)
                {
                    transaction.Commit();
                }
            }
            finally
            {
                await context.Database.CloseConnectionAsync().ConfigureAwait(false);
            }
        }

        public void Read<T>(DbContext context, Type type, IList<T> entities, TableInfo tableInfo, Action<decimal> progress) where T : class
        {
            throw new NotImplementedException();
        }

        public Task ReadAsync<T>(DbContext context, Type type, IList<T> entities, TableInfo tableInfo, Action<decimal> progress,
            CancellationToken cancellationToken) where T : class
        {
            throw new NotImplementedException();
        }

        public void Truncate(DbContext context, TableInfo tableInfo)
        {
            string sql = SqlQueryBuilder.DeleteTable(tableInfo.FullTableName);
            context.Database.ExecuteSqlRaw(sql);
        }

        public async Task TruncateAsync(DbContext context, TableInfo tableInfo)
        {
            string sql = SqlQueryBuilder.DeleteTable(tableInfo.FullTableName);
            await context.Database.ExecuteSqlRawAsync(sql);
        }
        
          #region SqliteData
        internal static SqliteCommand GetSqliteCommand<T>(DbContext context, Type type, IList<T> entities, TableInfo tableInfo, SqliteConnection connection, SqliteTransaction transaction)
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
                case OperationType.InsertOrUpdateDelete:
                    throw new NotSupportedException("Sqlite supports only UPSERT(analog for MERGE WHEN MATCHED) but does not have functionality to do: 'WHEN NOT MATCHED BY SOURCE THEN DELETE'" +
                                                    "What can be done is to read all Data, find rows that are not in input List, then with those do the BulkDelete.");
                case OperationType.Update:
                    command.CommandText = SqlQueryBuilderSqlite.UpdateSetTable(tableInfo);
                    break;
                case OperationType.Delete:
                    command.CommandText = SqlQueryBuilderSqlite.DeleteFromTable(tableInfo);
                    break;
            }

            type = tableInfo.HasAbstractList ? entities[0].GetType() : type;
            var entityType = context.Model.FindEntityType(type);
            var entityPropertiesDict = entityType.GetProperties().Where(a => tableInfo.PropertyColumnNamesDict.ContainsKey(a.Name)).ToDictionary(a => a.Name, a => a);
            var properties = type.GetProperties(BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic);

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

                    var parameter = new SqliteParameter($"@{property.Name}", propertyType); // ,sqliteType // ,null
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

        internal static void LoadSqliteValues<T>(TableInfo tableInfo, T entity, SqliteCommand command, DbContext dbContext)
        {
            var propertyColumnsDict = tableInfo.PropertyColumnNamesDict;
            foreach (var propertyColumn in propertyColumnsDict)
            {
                var isShadowProperty = tableInfo.ShadowProperties.Contains(propertyColumn.Key);
                string parameterName = propertyColumn.Key.Replace(".", "_");
                object value;
                if (!isShadowProperty)
                {
                    if (propertyColumn.Key.Contains(".")) // ToDo: change IF clause to check for NavigationProperties, optimise, integrate with same code segment from LoadData method
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
                        value = tableInfo.FastPropertyDict[propertyColumn.Key].Get(entity);
                    }
                }
                else
                {
                    if (tableInfo.BulkConfig.EnableShadowProperties)
                    {
                        // Get the shadow property value
                        value = dbContext.Entry(entity).Property(propertyColumn.Key).CurrentValue;
                    }
                    else
                    {
                        // Set the value for the discriminator column
                        value = entity.GetType().Name;
                    }
                }

                if (tableInfo.ConvertibleProperties.ContainsKey(propertyColumn.Key) && value != DBNull.Value)
                {
                    value = tableInfo.ConvertibleProperties[propertyColumn.Key].ConvertToProvider.Invoke(value);
                }

                command.Parameters[$"@{parameterName}"].Value = value ?? DBNull.Value;
            }
        }
        
        internal static async Task<SqliteConnection> OpenAndGetSqliteConnectionAsync(DbContext context, BulkConfig bulkConfig, CancellationToken cancellationToken)
        {
            await context.Database.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
            return (SqliteConnection)context.Database.GetDbConnection();
        }
        
        internal static SqliteConnection OpenAndGetSqliteConnection(DbContext context, BulkConfig bulkConfig)
        {
            context.Database.OpenConnection();

            return (SqliteConnection)context.Database.GetDbConnection();
        }
        #endregion
    }
}
