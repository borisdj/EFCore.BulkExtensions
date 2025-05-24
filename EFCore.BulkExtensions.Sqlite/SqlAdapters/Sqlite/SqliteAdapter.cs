using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using NetTopologySuite.Geometries;
using NetTopologySuite.IO;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;

namespace EFCore.BulkExtensions.SqlAdapters.Sqlite;
/// <inheritdoc/>
public class SqliteAdapter : ISqlOperationsAdapter
{
    /// <inheritdoc/>
    #region Methods
    // Insert
    public void Insert<T>(BulkContext context, Type type, IEnumerable<T> entities, TableInfo tableInfo, Action<decimal>? progress)
    {
        InsertAsync(context, type, entities, tableInfo, progress, isAsync: false, CancellationToken.None).GetAwaiter().GetResult();
    }

    /// <inheritdoc/>
    public async Task InsertAsync<T>(BulkContext context, Type type, IEnumerable<T> entities, TableInfo tableInfo, Action<decimal>? progress, CancellationToken cancellationToken)
    {
        await InsertAsync(context, type, entities, tableInfo, progress, isAsync: true, cancellationToken).ConfigureAwait(false);
    }
    
    /// <inheritdoc/>
    public static async Task InsertAsync<T>(BulkContext context, Type type, IEnumerable<T> entities, TableInfo tableInfo, Action<decimal>? progress, bool isAsync, CancellationToken cancellationToken)
    {
        var dbContext = context.DbContext;
        SqliteConnection? connection = (SqliteConnection?)context.DbConnection;
        if (connection == null)
        {
            connection = isAsync ? await OpenAndGetSqliteConnectionAsync(dbContext, cancellationToken).ConfigureAwait(false)
                                 : OpenAndGetSqliteConnection(dbContext);
        }
        bool doExplicitCommit = false;

        try
        {
            if (dbContext.Database.CurrentTransaction == null)
            {
                //context.Database.UseTransaction(connection.BeginTransaction());
                doExplicitCommit = true;
            }

            SqliteTransaction? transaction = (SqliteTransaction?)context.DbTransaction;
            if (transaction == null)
            {
                var dbTransaction = doExplicitCommit ? connection.BeginTransaction()
                                                     : dbContext.Database.CurrentTransaction?.GetUnderlyingTransaction(tableInfo.BulkConfig);

                transaction = (SqliteTransaction?)dbTransaction;
            }
            else
            {
                doExplicitCommit = false;
            }

            var command = GetSqliteCommand(dbContext, type, entities, tableInfo, connection, transaction);

            type = tableInfo.HasAbstractList ? entities.ElementAt(0)!.GetType() : type;
            int rowsCopied = 0;

            foreach (var item in entities)
            {
                LoadSqliteValues(tableInfo, item, command, dbContext);
                if (isAsync)
                {
                    await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
                }
                else
                {
                    command.ExecuteNonQuery();
                }
                ProgressHelper.SetProgress(ref rowsCopied, entities.Count(), tableInfo.BulkConfig, progress);
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
                    await dbContext.Database.CloseConnectionAsync().ConfigureAwait(false);
                }
                else
                {
                    dbContext.Database.CloseConnection();
                }
            }
        }
    }

    // Merge
    /// <inheritdoc/>
    public void Merge<T>(BulkContext context, Type type, IEnumerable<T> entities, TableInfo tableInfo, OperationType operationType, Action<decimal>? progress) where T : class
    {
        MergeAsync(context, type, entities, tableInfo, operationType, progress, isAsync: false, CancellationToken.None).GetAwaiter().GetResult();
    }

    /// <inheritdoc/>
    public async Task MergeAsync<T>(BulkContext context, Type type, IEnumerable<T> entities, TableInfo tableInfo, OperationType operationType, Action<decimal>? progress, CancellationToken cancellationToken) where T : class
    {
        await MergeAsync(context, type, entities, tableInfo, operationType, progress, isAsync: true, cancellationToken).ConfigureAwait(false);
    }
    
    /// <inheritdoc/>
    protected static async Task MergeAsync<T>(BulkContext context, Type type, IEnumerable<T> entities, TableInfo tableInfo, OperationType operationType, Action<decimal>? progress, bool isAsync, CancellationToken cancellationToken) where T : class
    {
        var dbContext = context.DbContext;
        SqliteConnection connection = isAsync ? await OpenAndGetSqliteConnectionAsync(dbContext, cancellationToken).ConfigureAwait(false)
                                                    : OpenAndGetSqliteConnection(dbContext);
        bool doExplicitCommit = false;

        try
        {
            if (dbContext.Database.CurrentTransaction == null)
            {
                //context.Database.UseTransaction(connection.BeginTransaction());
                doExplicitCommit = true;
            }
            var dbTransaction = doExplicitCommit ? connection.BeginTransaction()
                                                 : dbContext.Database.CurrentTransaction?.GetUnderlyingTransaction(tableInfo.BulkConfig);
            var transaction = (SqliteTransaction?)dbTransaction;

            var command = GetSqliteCommand(dbContext, type, entities, tableInfo, connection, transaction);

            type = tableInfo.HasAbstractList ? entities.ElementAt(0).GetType() : type;
            int rowsCopied = 0;

            foreach (var item in entities)
            {
                LoadSqliteValues(tableInfo, item, command, dbContext);
                if (isAsync)
                {
                    await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
                }
                else
                {
                    command.ExecuteNonQuery();
                }
                ProgressHelper.SetProgress(ref rowsCopied, entities.Count(), tableInfo.BulkConfig, progress);
            }

            if (tableInfo.BulkConfig.SetOutputIdentity && tableInfo.IdentityColumnName != null) // For Sqlite Identity can be set by Db only with pure Insert method
            {
                if (operationType == OperationType.Insert)
                {
                    command.CommandText = SqliteQueryBuilder.SelectLastInsertRowId();

                    object? lastRowIdScalar = isAsync ? await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false)
                                                            : command.ExecuteScalar();

                    SetIdentityForOutput(entities, tableInfo, lastRowIdScalar);
                }
            }

            if (tableInfo.BulkConfig.CustomSqlPostProcess != null)
            {
                if (isAsync)
                {
                    await dbContext.Database.ExecuteSqlRawAsync(tableInfo.BulkConfig.CustomSqlPostProcess, cancellationToken).ConfigureAwait(false);
                }
                else
                {
                    dbContext.Database.ExecuteSqlRaw(tableInfo.BulkConfig.CustomSqlPostProcess);
                }
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
                await dbContext.Database.CloseConnectionAsync().ConfigureAwait(false);
            }
            else
            {
                dbContext.Database.CloseConnection();
            }
        }
    }

    // Read
    /// <inheritdoc/>
    public void Read<T>(BulkContext context, Type type, IEnumerable<T> entities, TableInfo tableInfo, Action<decimal>? progress) where T : class
    {
        ReadAsync(context, type, entities, tableInfo, progress, isAsync: false, CancellationToken.None).GetAwaiter().GetResult();
    }

    /// <inheritdoc/>
    public async Task ReadAsync<T>(BulkContext context, Type type, IEnumerable<T> entities, TableInfo tableInfo, Action<decimal>? progress, CancellationToken cancellationToken) where T : class
    {
        await ReadAsync(context, type, entities, tableInfo, progress, isAsync: true, cancellationToken).ConfigureAwait(false);
    }
    
    /// <inheritdoc/>
    protected static async Task ReadAsync<T>(BulkContext context, Type type, IEnumerable<T> entities, TableInfo tableInfo, Action<decimal>? progress, bool isAsync, CancellationToken cancellationToken) where T : class
    {
        var dbContext = context.DbContext;
        SqliteConnection connection = isAsync ? await OpenAndGetSqliteConnectionAsync(dbContext, cancellationToken).ConfigureAwait(false)
                                                    : OpenAndGetSqliteConnection(dbContext);
        bool doExplicitCommit = false;
        bool tempTableCreated = false;
        SqliteTransaction? transaction = null;
        SqliteCommand? command = null;
        try
        {
            if (dbContext.Database.CurrentTransaction == null)
            {
                //context.Database.UseTransaction(connection.BeginTransaction());
                doExplicitCommit = true;
            }

            transaction = doExplicitCommit ? connection.BeginTransaction()
                                           : (SqliteTransaction?)dbContext.Database.CurrentTransaction?.GetUnderlyingTransaction(tableInfo.BulkConfig);

            command = connection.CreateCommand();
            command.Transaction = transaction;

            // CREATE
            command.CommandText = SqliteQueryBuilder.CreateTableCopy(tableInfo.FullTableName, tableInfo.FullTempTableName);
            if (isAsync)
            {
                await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            }
            else
            {
                command.ExecuteNonQuery();
            }
            tempTableCreated = true;

            tableInfo.BulkConfig.OperationType = OperationType.Insert;
            tableInfo.InsertToTempTable = true;
            context.DbConnection = connection;
            context.DbTransaction = transaction;
            // INSERT
            if (isAsync)
            {
                await InsertAsync(context, type, entities, tableInfo, progress, isAsync: true, cancellationToken).ConfigureAwait(false);
            }
            else
            {
                InsertAsync(context, type, entities, tableInfo, progress, isAsync: false, cancellationToken).GetAwaiter().GetResult();
            }

            // JOIN
            List<T> existingEntities;
            var sqlSelectJoinTable = SqlQueryBuilder.SelectJoinTable(tableInfo);
            Expression<Func<DbContext, IQueryable<T>>> expression = tableInfo.GetQueryExpression<T>(sqlSelectJoinTable, false);
            var compiled = EF.CompileQuery(expression); // instead using Compiled queries
            existingEntities = compiled(dbContext).ToList();

            if (tableInfo.BulkConfig.ReplaceReadEntities)
            {
                tableInfo.ReplaceReadEntities(entities, existingEntities);
            }
            else
            {
                tableInfo.UpdateReadEntities(entities, existingEntities, dbContext);
            }
        }
        finally
        {
            if (tempTableCreated && command != null)
            {
                command.CommandText = SqliteQueryBuilder.DropTable(tableInfo.FullTempTableName);
                if (isAsync)
                {
                    await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
                }
                else
                {
                    command.ExecuteNonQuery();
                }
            }

            if (doExplicitCommit)
            {
                transaction?.Commit();

                if (isAsync)
                {
                    if (transaction is not null)
                    {
                        await transaction.DisposeAsync().ConfigureAwait(false);
                    }

                    await dbContext.Database.CloseConnectionAsync().ConfigureAwait(false);
                }
                else
                {
                    transaction?.Dispose();
                    dbContext.Database.CloseConnection();
                }
            }
        }
    }

    /// <inheritdoc/>
    public void Truncate(BulkContext context, TableInfo tableInfo)
    {
        string sql = SqlQueryBuilder.DeleteTable(tableInfo.FullTableName);
        context.DbContext.Database.ExecuteSqlRaw(sql);
    }

    /// <inheritdoc/>
    public async Task TruncateAsync(BulkContext context, TableInfo tableInfo, CancellationToken cancellationToken)
    {
        string sql = SqlQueryBuilder.DeleteTable(tableInfo.FullTableName);
        await context.DbContext.Database.ExecuteSqlRawAsync(sql, cancellationToken).ConfigureAwait(false);
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
    internal static SqliteCommand GetSqliteCommand<T>(DbContext context, Type? type, IEnumerable<T> entities, TableInfo tableInfo, SqliteConnection connection, SqliteTransaction? transaction)
    {
        SqliteCommand command = connection.CreateCommand();
        command.Transaction = transaction;

        var operationType = tableInfo.BulkConfig.OperationType;

        switch (operationType)
        {
            case OperationType.Insert:
                command.CommandText = SqliteQueryBuilder.InsertIntoTable(tableInfo, OperationType.Insert);
                break;
            case OperationType.InsertOrUpdate:
                command.CommandText = SqliteQueryBuilder.InsertIntoTable(tableInfo, OperationType.InsertOrUpdate);
                break;
            case OperationType.InsertOrUpdateOrDelete:
                throw new NotSupportedException("'BulkInsertOrUpdateDelete' not supported for Sqlite. Sqlite has only UPSERT statement (analog for MERGE WHEN MATCHED) but no functionality for: 'WHEN NOT MATCHED BY SOURCE THEN DELETE'." +
                                                " Another way to achieve this is to BulkRead existing data from DB, split list into sublists and call separately Bulk methods for Insert, Update, Delete.");
            case OperationType.Update:
                command.CommandText = SqliteQueryBuilder.UpdateSetTable(tableInfo);
                break;
            case OperationType.Delete:
                command.CommandText = SqliteQueryBuilder.DeleteFromTable(tableInfo);
                break;
        }

        type = tableInfo.HasAbstractList ? entities.ElementAt(0)?.GetType() : type;
        if (type is null)
        {
            throw new ArgumentException("Unable to determine entity type");
        }
        var entityType = context.Model.FindEntityType(type);
        var entityPropertiesDict = entityType?.GetProperties().Where(a => tableInfo.PropertyColumnNamesDict.ContainsKey(a.Name)).ToDictionary(a => a.Name, a => a);
        var properties = type.GetProperties(BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic);

        var entityShadowFkPropertiesDict = entityType?.GetProperties().Where(
            a => a.IsShadowProperty() &&
                 a.IsForeignKey() &&
                 a.GetContainingForeignKeys().FirstOrDefault()?.DependentToPrincipal?.Name != null)
           .ToDictionary(x => x.GetContainingForeignKeys()?.First()?.DependentToPrincipal?.Name ?? string.Empty, a => a);

        foreach (var property in properties)
        {
            IProperty? propertyEntityType = null;
            if (entityPropertiesDict?.ContainsKey(property.Name) ?? false)
            {
                propertyEntityType = entityPropertiesDict[property.Name];
            }
            else if (entityShadowFkPropertiesDict?.ContainsKey(property.Name) ?? false)
            {
                propertyEntityType = entityShadowFkPropertiesDict[property.Name];
            }

            if (propertyEntityType != null)
            {
                string? propertyName = propertyEntityType.Name;
                var propertyType = Nullable.GetUnderlyingType(property.PropertyType) ?? property.PropertyType;

                //SqliteType(CpropertyType.Name): Text(String, Decimal, DateTime); Integer(Int16, Int32, Int64) Real(Float, Double) Blob(Guid)
                var parameter = new SqliteParameter($"@{propertyName}", propertyType); // ,sqliteType // ,null //()
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
            object? value = null;
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
                else if (tableInfo.FastPropertyDict.ContainsKey(propertyColumn.Key))
                {
                    value = entity is null ? null : tableInfo.FastPropertyDict[propertyColumn.Key].Get(entity);
                }
                else if (tableInfo.ColumnToPropertyDictionary.ContainsKey(propertyColumn.Key))
                {
                    var property = tableInfo.ColumnToPropertyDictionary[propertyColumn.Key];

                    if (property.IsShadowProperty() && property.IsForeignKey())
                    {
                        var foreignKey = property.GetContainingForeignKeys().FirstOrDefault();
                        var principalNavigation = foreignKey?.DependentToPrincipal;
                        var pkPropertyName = foreignKey?.PrincipalKey.Properties.FirstOrDefault()?.Name;
                        if (principalNavigation is not null && pkPropertyName is not null)
                        {
                            pkPropertyName = principalNavigation.Name + "_" + pkPropertyName;
                            var fkPropertyValue = entity == null ? null : tableInfo.FastPropertyDict[principalNavigation.Name].Get(entity);
                            value = fkPropertyValue == null ? null : tableInfo.FastPropertyDict[pkPropertyName]?.Get(fkPropertyValue);
                        }
                    }
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

            if (tableInfo.ConvertibleColumnConverterDict.ContainsKey(propertyColumn.Value) && value != DBNull.Value)
            {
                value = tableInfo.ConvertibleColumnConverterDict[propertyColumn.Value].ConvertToProvider.Invoke(value);
            }

            var param = command.Parameters[$"@{parameterName}"];

            string columnName = propertyColumn.Value;
            string typeName = tableInfo.ColumnNamesTypesDict[columnName];

            if (value is Geometry geo) // spatial types
            {
                param.Value = CreateWriter(typeName).Write(geo);
            }
            else
            {
                param.Value = value ?? DBNull.Value;
            }
        }
    }
    
    /// <inheritdoc/>
    public static void SetIdentityForOutput<T>(IEnumerable<T> entities, TableInfo tableInfo, object? lastRowIdScalar)
    {
        long counter = (long?)lastRowIdScalar ?? 0;

        string identityPropertyName = tableInfo.PropertyColumnNamesDict.SingleOrDefault(a => a.Value == tableInfo.IdentityColumnName).Key;
        FastProperty identityFastProperty = tableInfo.FastPropertyDict[identityPropertyName];

        string idTypeName = identityFastProperty.Property.PropertyType.Name;
        object? idValue = null;
        for (int i = entities.Count() - 1; i >= 0; i--)
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
            if (entities.ElementAt(i) is not null)
            {
                identityFastProperty.Set(entities.ElementAt(i)!, idValue);
            }

            counter--;
        }
    }

    private static GaiaGeoWriter CreateWriter(string storeType)
    {
        Ordinates handleOrdinates;

        switch (storeType.ToUpperInvariant())
        {
            case "POINT":
            case "LINESTRING":
            case "POLYGON":
            case "MULTIPOINT":
            case "MULTILINESTRING":
            case "MULTIPOLYGON":
            case "GEOMETRYCOLLECTION":
            case "GEOMETRY":
                handleOrdinates = Ordinates.XY;
                break;

            case "POINTZ":
            case "LINESTRINGZ":
            case "POLYGONZ":
            case "MULTIPOINTZ":
            case "MULTILINESTRINGZ":
            case "MULTIPOLYGONZ":
            case "GEOMETRYCOLLECTIONZ":
            case "GEOMETRYZ":
                handleOrdinates = Ordinates.XYZ;
                break;

            case "POINTM":
            case "LINESTRINGM":
            case "POLYGONM":
            case "MULTIPOINTM":
            case "MULTILINESTRINGM":
            case "MULTIPOLYGONM":
            case "GEOMETRYCOLLECTIONM":
            case "GEOMETRYM":
                handleOrdinates = Ordinates.XYM;
                break;

            case "POINTZM":
            case "LINESTRINGZM":
            case "POLYGONZM":
            case "MULTIPOINTZM":
            case "MULTILINESTRINGZM":
            case "MULTIPOLYGONZM":
            case "GEOMETRYCOLLECTIONZM":
            case "GEOMETRYZM":
                handleOrdinates = Ordinates.XYZM;
                break;

            default:
                throw new ArgumentException("Invalid GeometryType", nameof(storeType));
        }

        return new GaiaGeoWriter { HandleOrdinates = handleOrdinates };
    }
    #endregion
}
