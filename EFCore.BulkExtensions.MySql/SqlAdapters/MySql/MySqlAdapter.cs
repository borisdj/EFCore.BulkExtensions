﻿using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;
using MySqlConnector;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using NetTopologySuite.Geometries;

namespace EFCore.BulkExtensions.SqlAdapters.MySql;

/// <inheritdoc/>
public class MySqlAdapter : ISqlOperationsAdapter
{
    private MySqlQueryBuilder ProviderSqlQueryBuilder => new MySqlQueryBuilder();

    /// <inheritdoc/>
    #region Methods
    // Insert
    public void Insert<T>(BulkContext context, Type type, IEnumerable<T> entities, TableInfo tableInfo, Action<decimal>? progress)
    {
        InsertAsync(context, type, entities, tableInfo, progress, isAsync: false, CancellationToken.None).GetAwaiter().GetResult();
    }

    /// <inheritdoc/>
    public async Task InsertAsync<T>(BulkContext context, Type type, IEnumerable<T> entities, TableInfo tableInfo, Action<decimal>? progress,
        CancellationToken cancellationToken)
    {
        await InsertAsync(context, type, entities, tableInfo, progress, isAsync: true, CancellationToken.None).ConfigureAwait(false);
    }

    /// <inheritdoc/>
    protected static async Task InsertAsync<T>(BulkContext context, Type type, IEnumerable<T> entities, TableInfo tableInfo, Action<decimal>? progress, 
        bool isAsync, CancellationToken cancellationToken)
    {
        var dbContext = context.DbContext;
        tableInfo.CheckToSetIdentityForPreserveOrder(tableInfo, entities);
        if (isAsync)
        {
            await dbContext.Database.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        }
        else
        {
            dbContext.Database.OpenConnection();
        }
        var connection = dbContext.GetUnderlyingConnection(tableInfo.BulkConfig);
        try
        {
            var transaction = dbContext.Database.CurrentTransaction;
            var mySqlBulkCopy = GetMySqlBulkCopy((MySqlConnection)connection, transaction, tableInfo.BulkConfig);

            SetMySqlBulkCopyConfig(mySqlBulkCopy, tableInfo);

            var dataTable = GetDataTable(context, type, entities, mySqlBulkCopy, tableInfo);
            IDataReader? dataReader = tableInfo.BulkConfig.DataReader;

            if (isAsync)
            {
                if(dataReader == null)
                    await mySqlBulkCopy.WriteToServerAsync(dataTable, cancellationToken).ConfigureAwait(false);
                else
                    await mySqlBulkCopy.WriteToServerAsync(dataReader, cancellationToken).ConfigureAwait(false);
            }
            else
            {
                if (dataReader == null)
                    mySqlBulkCopy.WriteToServer(dataTable);
                else
                    mySqlBulkCopy.WriteToServer(dataReader);
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
        if (!tableInfo.CreateOutputTable)
        {
            tableInfo.CheckToSetIdentityForPreserveOrder(tableInfo, entities, reset: true);
        }
    }
    /// <inheritdoc/>
    public void Merge<T>(BulkContext context, Type type, IEnumerable<T> entities, TableInfo tableInfo, OperationType operationType, Action<decimal>? progress) 
        where T : class
    {
        MergeAsync(context, type, entities, tableInfo, operationType, progress, isAsync: false, CancellationToken.None).GetAwaiter().GetResult();
    }

    /// <inheritdoc/>
    public async Task MergeAsync<T>(BulkContext context, Type type, IEnumerable<T> entities, TableInfo tableInfo, OperationType operationType, Action<decimal>? progress, 
        CancellationToken cancellationToken) where T : class
    {
        await MergeAsync(context, type, entities, tableInfo, operationType, progress, isAsync: true, CancellationToken.None).ConfigureAwait(false);
    }

    /// <inheritdoc/>
    protected async Task MergeAsync<T>(BulkContext context, Type type, IEnumerable<T> entities, TableInfo tableInfo, OperationType operationType, Action<decimal>? progress, 
        bool isAsync, CancellationToken cancellationToken) where T : class
    {
        bool tempTableCreated = false;
        bool outputTableCreated = false;
        bool uniqueConstrainCreated = false;
        bool hasExistingTransaction = false;
        var dbContext = context.DbContext;
        var transaction = dbContext.Database.CurrentTransaction;
        try
        {
            //Because of using temp table in case of update, we need to access created temp table in Insert method.
            hasExistingTransaction = dbContext.Database.CurrentTransaction != null;
            transaction ??= isAsync ? await dbContext.Database.BeginTransactionAsync(cancellationToken).ConfigureAwait(false)
                                    : dbContext.Database.BeginTransaction();

            if (tableInfo.BulkConfig.CustomSourceTableName == null)
            {
                tableInfo.InsertToTempTable = true;

                var sqlCreateTableCopy = MySqlQueryBuilder.CreateTableCopy(tableInfo.FullTableName, tableInfo.FullTempTableName, tableInfo.BulkConfig.UseTempDB);
                if (isAsync)
                {
                    await dbContext.Database.ExecuteSqlRawAsync(sqlCreateTableCopy, cancellationToken).ConfigureAwait(false);
                }
                else
                {
                    dbContext.Database.ExecuteSqlRaw(sqlCreateTableCopy);
                }
                tempTableCreated = true;
            }

            bool hasUniqueConstrain = false;
            string joinedEntityPK = string.Join("_", tableInfo.EntityPKPropertyColumnNameDict.Keys.ToList());
            string joinedPrimaryKeys = string.Join("_", tableInfo.PrimaryKeysPropertyColumnNameDict.Keys.ToList());
            if (joinedEntityPK == joinedPrimaryKeys)
            {
                hasUniqueConstrain = true; // Explicit Constrain not required for PK
            }
            else
            {
                (hasUniqueConstrain, bool connectionOpenedInternally) = 
                    await CheckHasExplicitUniqueConstrainAsync(dbContext, tableInfo, isAsync, cancellationToken).ConfigureAwait(false);
            }

            if (!hasUniqueConstrain)
            {
                string createUniqueConstrain = MySqlQueryBuilder.CreateUniqueConstrain(tableInfo);
                if (isAsync)
                {
                    await dbContext.Database.ExecuteSqlRawAsync(createUniqueConstrain, cancellationToken).ConfigureAwait(false);
                }
                else
                {
                    dbContext.Database.ExecuteSqlRaw(createUniqueConstrain);
                }
                uniqueConstrainCreated = true;
            }

            if (tableInfo.CreateOutputTable)
            {
                tableInfo.InsertToTempTable = true;
                var sqlCreateOutputTableCopy = MySqlQueryBuilder.CreateTableCopy(tableInfo.FullTableName,
                    tableInfo.FullTempOutputTableName, tableInfo.InsertToTempTable);
                if (isAsync)
                {
                    await dbContext.Database.ExecuteSqlRawAsync(sqlCreateOutputTableCopy, cancellationToken).ConfigureAwait(false);
                }
                else
                {
                    dbContext.Database.ExecuteSqlRaw(sqlCreateOutputTableCopy);
                }
                outputTableCreated = true;
            }

            if (tableInfo.BulkConfig.CustomSourceTableName == null)
            {
                if (isAsync)
                {
                    await InsertAsync(context, type, entities, tableInfo, progress, cancellationToken).ConfigureAwait(false);
                }
                else
                {
                    Insert(context, type, entities, tableInfo, progress);
                }
            }

            var sqlMergeTable = MySqlQueryBuilder.MergeTable<T>(tableInfo, operationType);
            if (isAsync)
            {
                await dbContext.Database.ExecuteSqlRawAsync(sqlMergeTable, cancellationToken).ConfigureAwait(false);
            }
            else
            {
                dbContext.Database.ExecuteSqlRaw(sqlMergeTable);
            }
            if (tableInfo.CreateOutputTable)
            {
                if (isAsync)
                {
                    await tableInfo.LoadOutputDataAsync(context, type, entities, tableInfo, isAsync: true, cancellationToken).ConfigureAwait(false);
                }
                else
                {
                    tableInfo.LoadOutputDataAsync(context, type, entities, tableInfo, isAsync: false, cancellationToken).GetAwaiter().GetResult();
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

            if (hasExistingTransaction == false && !tableInfo.BulkConfig.IncludeGraph)
            {
                if (isAsync)
                {
                    await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
                }
                else
                {
                    transaction.Commit();
                }
            }
        }
        finally
        {
            if (uniqueConstrainCreated)
            {
                string dropUniqueConstrain = MySqlQueryBuilder.DropUniqueConstrain(tableInfo);
                if (isAsync)
                {
                    await dbContext.Database.ExecuteSqlRawAsync(dropUniqueConstrain, cancellationToken)
                        .ConfigureAwait(false);
                }
                else
                {
                    dbContext.Database.ExecuteSqlRaw(dropUniqueConstrain);
                }
            }

            if (!tableInfo.BulkConfig.UseTempDB) // Temp tables are automatically dropped by the database
            {
                if (outputTableCreated)
                {
                    var sqlDropOutputTable = ProviderSqlQueryBuilder.DropTable(tableInfo.FullTempOutputTableName, tableInfo.BulkConfig.UseTempDB);
                    if (isAsync)
                    {
                        await dbContext.Database.ExecuteSqlRawAsync(sqlDropOutputTable, cancellationToken).ConfigureAwait(false);
                    }
                    else
                    {
                        dbContext.Database.ExecuteSqlRaw(sqlDropOutputTable);
                    }

                }
                if (tempTableCreated)
                {
                    var sqlDropTable = ProviderSqlQueryBuilder.DropTable(tableInfo.FullTempTableName, tableInfo.BulkConfig.UseTempDB);
                    if (isAsync)
                    {
                        await dbContext.Database.ExecuteSqlRawAsync(sqlDropTable, cancellationToken).ConfigureAwait(false);
                    }
                    else
                    {
                        dbContext.Database.ExecuteSqlRaw(sqlDropTable);
                    }
                }
                if (hasExistingTransaction == false && !tableInfo.BulkConfig.IncludeGraph && transaction != null)
                {
                    if (isAsync)
                    {
                        await transaction.DisposeAsync().ConfigureAwait(false);
                    }
                    else
                    {
                        transaction.Dispose();
                    }
                }
            }    
        }
        
        
    }
    /// <inheritdoc/>
    public void Read<T>(BulkContext context, Type type, IEnumerable<T> entities, TableInfo tableInfo, Action<decimal>? progress) where T : class
    {
        throw new NotImplementedException();
    }

    /// <inheritdoc/>
    public Task ReadAsync<T>(BulkContext context, Type type, IEnumerable<T> entities, TableInfo tableInfo, Action<decimal>? progress,
        CancellationToken cancellationToken) where T : class
    {
        throw new NotImplementedException();
    }

    /// <inheritdoc/>
    public void Truncate(BulkContext context, TableInfo tableInfo)
    {
        var sqlTruncateTable = ProviderSqlQueryBuilder.TruncateTable(tableInfo.FullTableName);
        context.DbContext.Database.ExecuteSqlRaw(sqlTruncateTable);
    }

    /// <inheritdoc/>
    public async Task TruncateAsync(BulkContext context, TableInfo tableInfo, CancellationToken cancellationToken)
    {
        var sqlTruncateTable = ProviderSqlQueryBuilder.TruncateTable(tableInfo.FullTableName);
        await context.DbContext.Database.ExecuteSqlRawAsync(sqlTruncateTable, cancellationToken).ConfigureAwait(false);
    }
    #endregion
    #region Connection

    internal static async Task<(DbConnection, bool)> OpenAndGetMySqlConnectionAsync(DbContext context, 
        bool isAsync, CancellationToken cancellationToken)
    {
        bool oonnectionOpenedInternally = false;
        var connection = context.Database.GetDbConnection();
        if (connection.State != ConnectionState.Open)
        {
            if (isAsync)
            {
                await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
            }
            else
            {
                connection.Open();
            }
            oonnectionOpenedInternally = true;
        }
        return (connection, oonnectionOpenedInternally);
    }

    internal static async Task<(bool, bool)> CheckHasExplicitUniqueConstrainAsync(DbContext context, TableInfo tableInfo, 
        bool isAsync, CancellationToken cancellationToken)
    {
        string countUniqueConstrain = MySqlQueryBuilder.HasUniqueConstrain(tableInfo);

        (DbConnection connection, bool connectionOpenedInternally) = await OpenAndGetMySqlConnectionAsync(context, isAsync, cancellationToken).ConfigureAwait(false);

        bool hasUniqueConstrain = false;
        using (var command = connection.CreateCommand())
        {
            command.CommandText = countUniqueConstrain;
            command.Transaction = context.Database.CurrentTransaction?.GetDbTransaction();

            if (isAsync)
            {
                using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
                if (reader.HasRows)
                {
                    while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                    {
                        Int64.TryParse(reader[0].ToString(), out long result);
                        hasUniqueConstrain = result > 0;
                    }
                }
            }
            else
            {
                using var reader = command.ExecuteReader();
                if (reader.HasRows)
                {
                    while (reader.Read())
                    {
                        Int64.TryParse(reader[0].ToString(), out long result);
                        hasUniqueConstrain = result > 0;
                    }
                }
            }
        }
        return (hasUniqueConstrain, connectionOpenedInternally);
    }

    private static MySqlBulkCopy GetMySqlBulkCopy(MySqlConnection mySqlConnection, IDbContextTransaction? transaction, BulkConfig config)
    {
        var mySqlTransaction = transaction == null ? null : (MySqlTransaction)transaction.GetUnderlyingTransaction(config);
        var mySqlBulkCopy = new MySqlBulkCopy(mySqlConnection, mySqlTransaction);
        
        return mySqlBulkCopy;
    }
    /// <summary>
    /// Supports <see cref="MySqlConnector.MySqlBulkCopy"/>
    /// </summary>
    /// <param name="mySqlBulkCopy"></param>
    /// <param name="tableInfo"></param>
    private static void SetMySqlBulkCopyConfig(MySqlBulkCopy mySqlBulkCopy, TableInfo tableInfo)
    {
        string destinationTable = tableInfo.InsertToTempTable ? tableInfo.FullTempTableName
                                                              : tableInfo.FullTableName;
        destinationTable = destinationTable.Replace("[", "")
                                           .Replace("]", "");
        mySqlBulkCopy.DestinationTableName = destinationTable;

        mySqlBulkCopy.NotifyAfter = tableInfo.BulkConfig.NotifyAfter ?? tableInfo.BulkConfig.BatchSize;
        mySqlBulkCopy.BulkCopyTimeout = tableInfo.BulkConfig.BulkCopyTimeout ?? mySqlBulkCopy.BulkCopyTimeout;

        mySqlBulkCopy.ConflictOption = tableInfo.BulkConfig.ConflictOption switch
        {
            ConflictOption.None => MySqlBulkLoaderConflictOption.None,
            ConflictOption.Replace => MySqlBulkLoaderConflictOption.Replace,
            ConflictOption.Ignore => MySqlBulkLoaderConflictOption.Ignore,
            _ => throw new InvalidEnumArgumentException(nameof(tableInfo.BulkConfig.ConflictOption))
        };
    }

    #endregion
    #region DataTable
    /// <summary>
    /// Supports <see cref="MySqlBulkCopy"/>
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <param name="context"></param>
    /// <param name="type"></param>
    /// <param name="entities"></param>
    /// <param name="mySqlBulkCopy"></param>
    /// <param name="tableInfo"></param>
    /// <returns></returns>
    public static DataTable GetDataTable<T>(BulkContext context, Type type, IEnumerable<T> entities, MySqlBulkCopy mySqlBulkCopy, TableInfo tableInfo)
    {
        DataTable dataTable = InnerGetDataTable(context, ref type, entities, tableInfo);

        int sourceOrdinal = 0;
        foreach (DataColumn item in dataTable.Columns)  //Add mapping
        {
            mySqlBulkCopy.ColumnMappings.Add(new MySqlBulkCopyColumnMapping(sourceOrdinal,item.ColumnName));
            sourceOrdinal++;
        }
        return dataTable;
    }

    /// <summary>
    /// Common logic for two versions of GetDataTable
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <param name="context"></param>
    /// <param name="type"></param>
    /// <param name="entities"></param>
    /// <param name="tableInfo"></param>
    /// <returns></returns>
    private static DataTable InnerGetDataTable<T>(BulkContext context, ref Type type, IEnumerable<T> entities, TableInfo tableInfo)
    {
        var dbContext = context.DbContext;
        var dataTable = new DataTable();
        var columnsDict = new Dictionary<string, object?>();
        var ownedEntitiesMappedProperties = new HashSet<string>();

        var databaseType = context.Server.Type;
        var isMySql = databaseType == SqlType.MySql;
        
        var objectIdentifier = tableInfo.ObjectIdentifier;
        type = tableInfo.HasAbstractList ? entities.ElementAt(0)!.GetType() : type;
        var entityType = dbContext.Model.FindEntityType(type) ?? throw new ArgumentException($"Unable to determine entity type from given type - {type.Name}");
        var entityTypeProperties = entityType.GetProperties();

        var entityPropertiesDict = entityTypeProperties.Where(a => tableInfo.PropertyColumnNamesDict.ContainsKey(a.Name) ||
                                                                   (tableInfo.BulkConfig.OperationType != OperationType.Read && a.Name == tableInfo.TimeStampPropertyName))
                                                       .ToDictionary(a => a.Name, a => a);

        var entityNavigationOwnedDict = entityType.GetNavigations().Where(a => a.TargetEntityType.IsOwned())
                                                                   .ToDictionary(a => a.Name, a => a);

        var entityShadowFkPropertiesDict = entityTypeProperties.Where(a => a.IsShadowProperty() &&
                                                                           a.IsForeignKey() &&
                                                                           a.GetContainingForeignKeys().FirstOrDefault()?.DependentToPrincipal?.Name != null)
                                                               .ToDictionary(x => x.GetContainingForeignKeys()?.First()?.DependentToPrincipal?.Name ?? string.Empty, a => a);

        var entityShadowFkPropertyColumnNamesDict = entityShadowFkPropertiesDict.ToDictionary(a => a.Key, a => a.Value.GetColumnName(objectIdentifier));
        var shadowPropertyColumnNamesDict = entityPropertiesDict.Where(a => a.Value.IsShadowProperty())
                                                                .ToDictionary(a => a.Key, a => a.Value.GetColumnName(objectIdentifier));

        var properties = type.GetProperties(BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic);
        var discriminatorColumn = GetDiscriminatorColumn(tableInfo);

        foreach (var property in properties)
        {
            var hasDefaultVauleOnInsert = tableInfo.BulkConfig.OperationType == OperationType.Insert
                && !tableInfo.BulkConfig.SetOutputIdentity
                && tableInfo.DefaultValueProperties.Contains(property.Name);

            if (entityPropertiesDict.ContainsKey(property.Name))
            {
                var propertyEntityType = entityPropertiesDict[property.Name];
                string columnName = propertyEntityType.GetColumnName(objectIdentifier) ?? string.Empty;

                var isConvertible = tableInfo.ConvertibleColumnConverterDict.ContainsKey(columnName);
                var propertyType = isConvertible ? tableInfo.ConvertibleColumnConverterDict[columnName].ProviderClrType
                                                 : property.PropertyType;

                var underlyingType = Nullable.GetUnderlyingType(propertyType);
                if (underlyingType != null)
                {
                    propertyType = underlyingType;
                }

                if (isMySql && (propertyType == typeof(Geometry) || propertyType.IsSubclassOf(typeof(Geometry))))
                {
                    propertyType = typeof(MySqlGeometry);
                    tableInfo.HasSpatialType = true;
                    if (tableInfo.BulkConfig.PropertiesToIncludeOnCompare != null || tableInfo.BulkConfig.PropertiesToIncludeOnCompare != null)
                    {
                        throw new InvalidOperationException("OnCompare properties Config can not be set for Entity with Spatial types like 'Geometry'");
                    }
                }

                if (!columnsDict.ContainsKey(property.Name) && !hasDefaultVauleOnInsert)
                {
                    dataTable.Columns.Add(columnName, propertyType);
                    columnsDict.Add(property.Name, null);
                }
            }
            else if (entityShadowFkPropertiesDict.ContainsKey(property.Name))
            {
                var fk = entityShadowFkPropertiesDict[property.Name];

                entityPropertiesDict.TryGetValue(fk.GetColumnName(objectIdentifier) ?? string.Empty, out var entityProperty);
                if (entityProperty == null) // BulkRead
                    continue;

                var columnName = entityProperty.GetColumnName(objectIdentifier);

                var isConvertible = tableInfo.ConvertibleColumnConverterDict.ContainsKey(columnName ?? string.Empty);
                var propertyType = isConvertible ? tableInfo.ConvertibleColumnConverterDict[columnName ?? string.Empty].ProviderClrType
                                                 : entityProperty.ClrType;

                var underlyingType = Nullable.GetUnderlyingType(propertyType);
                if (underlyingType != null)
                {
                    propertyType = underlyingType;
                }

                if (propertyType == typeof(Geometry) && isMySql)
                {
                    propertyType = typeof(byte[]);
                }

                if (columnName is not null && !(columnsDict.ContainsKey(columnName)) && !hasDefaultVauleOnInsert)
                {
                    dataTable.Columns.Add(columnName, propertyType);
                    columnsDict.Add(columnName, null);
                }
            }
            else if (entityNavigationOwnedDict.ContainsKey(property.Name)) // isOWned
            {
                //Type? navOwnedType = type.Assembly.GetType(property.PropertyType.FullName!); // was not used

                var ownedEntityType = dbContext.Model.FindEntityType(property.PropertyType);
                if (ownedEntityType == null)
                {
                    ownedEntityType = dbContext.Model.GetEntityTypes().SingleOrDefault(x => x.ClrType == property.PropertyType && x.Name.StartsWith(entityType.Name + "." + property.Name + "#"));
                }

                var ownedEntityProperties = ownedEntityType?.GetProperties().ToList() ?? new();
                var ownedEntityPropertyNameColumnNameDict = new Dictionary<string, string>();

                foreach (var ownedEntityProperty in ownedEntityProperties)
                {
                    if (!ownedEntityProperty.IsPrimaryKey())
                    {
                        string? columnName = ownedEntityProperty.GetColumnName(objectIdentifier);
                        if (columnName is not null && tableInfo.PropertyColumnNamesDict.ContainsValue(columnName))
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
                            var columnName = ownedEntityPropertyNameColumnNameDict[innerProperty.Name];
                            var propertyName = $"{property.Name}_{innerProperty.Name}";

                            if (tableInfo.ConvertibleColumnConverterDict.ContainsKey(propertyName))
                            {
                                var convertor = tableInfo.ConvertibleColumnConverterDict[propertyName];
                                var underlyingType = Nullable.GetUnderlyingType(convertor.ProviderClrType) ?? convertor.ProviderClrType;
                                dataTable.Columns.Add(columnName, underlyingType);
                            }
                            else
                            {
                                var ownedPropertyType = Nullable.GetUnderlyingType(innerProperty.PropertyType) ?? innerProperty.PropertyType;
                                dataTable.Columns.Add(columnName, ownedPropertyType);
                            }

                            columnsDict.Add(property.Name + "_" + innerProperty.Name, null);
                        }
                    }
                }
            }
        }

        if (tableInfo.BulkConfig.EnableShadowProperties)
        {
            foreach (var shadowProperty in entityPropertiesDict.Values.Where(a => a.IsShadowProperty()))
            {
                string? columnName = shadowProperty.GetColumnName(objectIdentifier);

                // If a model has an entity which has a relationship without an explicity defined FK, the data table will already contain the foreign key shadow property
                if (columnName is not null && dataTable.Columns.Contains(columnName))
                    continue;

                var isConvertible = columnName is not null && tableInfo.ConvertibleColumnConverterDict.ContainsKey(columnName);

                var propertyType = isConvertible
                    ? tableInfo.ConvertibleColumnConverterDict[columnName!].ProviderClrType
                    : shadowProperty.ClrType;

                var underlyingType = Nullable.GetUnderlyingType(propertyType);
                if (underlyingType != null)
                {
                    propertyType = underlyingType;
                }

                if (isMySql && (propertyType == typeof(Geometry) || propertyType.IsSubclassOf(typeof(Geometry))))
                {
                    propertyType = typeof(byte[]);
                }

                dataTable.Columns.Add(columnName, propertyType);
                columnsDict.Add(shadowProperty.Name, null);
            }
        }

        if (discriminatorColumn != null)
        {
            var discriminatorProperty = entityPropertiesDict[discriminatorColumn];

            dataTable.Columns.Add(discriminatorColumn, discriminatorProperty.ClrType);
            columnsDict.Add(discriminatorColumn, entityType.GetDiscriminatorValue());
        }
        bool hasConverterProperties = tableInfo.ConvertiblePropertyColumnDict.Count > 0;

        foreach (T entity in entities)
        {
            var propertiesToLoad = properties
                .Where(a => !tableInfo.AllNavigationsDictionary.ContainsKey(a.Name)
                            || entityShadowFkPropertiesDict.ContainsKey(a.Name)
                            || tableInfo.OwnedTypesDict.ContainsKey(a.Name)); // omit virtual Navigation (except Owned and ShadowNavig.) since it's Getter can cause unwanted Select-s from Db

            foreach (var property in propertiesToLoad)
            {
                object? propertyValue = tableInfo.FastPropertyDict.ContainsKey(property.Name)
                    ? tableInfo.FastPropertyDict[property.Name].Get(entity!)
                    : null;

                var hasDefaultVauleOnInsert = tableInfo.BulkConfig.OperationType == OperationType.Insert
                                           && !tableInfo.BulkConfig.SetOutputIdentity
                                           && tableInfo.DefaultValueProperties.Contains(property.Name);

                if (isMySql && tableInfo.BulkConfig.DateTime2PrecisionForceRound
                            && tableInfo.DateTime2PropertiesPrecisionLessThen7Dict.ContainsKey(property.Name))
                {
                    DateTime? dateTimePropertyValue = (DateTime?)propertyValue;

                    if (dateTimePropertyValue is not null)
                    {
                        int precision = tableInfo.DateTime2PropertiesPrecisionLessThen7Dict[property.Name];
                        int digitsToRemove = 7 - precision;
                        int powerOf10 = (int)Math.Pow(10, digitsToRemove);

                        long subsecondTicks = dateTimePropertyValue.Value!.Ticks % 10000000;
                        long ticksToRound = subsecondTicks + (subsecondTicks % 10 == 0 ? 1 : 0); // if ends with 0 add 1 tick to make sure rounding of value .5_zeros is rounded to Upper like SqlServer is doing, not to Even as Math.Round works
                        int roundedTicks = Convert.ToInt32(Math.Round((decimal)ticksToRound / powerOf10, 0)) * powerOf10;
                        dateTimePropertyValue = dateTimePropertyValue.Value!.AddTicks(-subsecondTicks).AddTicks(roundedTicks);

                        propertyValue = dateTimePropertyValue;
                    }
                }

                if (hasConverterProperties && tableInfo.ConvertiblePropertyColumnDict.ContainsKey(property.Name))
                {
                    string columnName = tableInfo.ConvertiblePropertyColumnDict[property.Name];
                    propertyValue = tableInfo.ConvertibleColumnConverterDict[columnName].ConvertToProvider.Invoke(propertyValue);
                }

                //TODO: Hamdling special types

                if (tableInfo.HasSpatialType && propertyValue is Geometry geometryValue)
                {
                    geometryValue.SRID = tableInfo.BulkConfig.SRID;
                    var wkb = geometryValue.ToBinary();
                    propertyValue = MySqlGeometry.FromWkb(geometryValue.SRID, wkb);
                }

                if (entityPropertiesDict.ContainsKey(property.Name) && !hasDefaultVauleOnInsert)
                {
                    columnsDict[property.Name] = propertyValue;
                }
                else if (entityShadowFkPropertiesDict.ContainsKey(property.Name))
                {
                    var foreignKeyShadowProperty = entityShadowFkPropertiesDict[property.Name];
                    var columnName = entityShadowFkPropertyColumnNamesDict[property.Name] ?? string.Empty;
                    if (!entityPropertiesDict.TryGetValue(columnName, out var entityProperty) || entityProperty is null)
                    {
                        continue; // BulkRead
                    };
                    columnsDict[columnName] = propertyValue != null ? foreignKeyShadowProperty.FindFirstPrincipal()?.PropertyInfo?.GetValue(propertyValue) // TODO Try to optimize
                                                                    : propertyValue;
                }
                else if (entityNavigationOwnedDict.ContainsKey(property.Name) && !tableInfo.LoadOnlyPKColumn)
                {
                    var ownedProperties = property.PropertyType.GetProperties().Where(a => ownedEntitiesMappedProperties.Contains(property.Name + "_" + a.Name));
                    foreach (var ownedProperty in ownedProperties)
                    {
                        var columnName = $"{property.Name}_{ownedProperty.Name}";
                        var ownedPropertyValue = propertyValue == null ? null : tableInfo.FastPropertyDict[columnName].Get(propertyValue);

                        if (tableInfo.ConvertibleColumnConverterDict.ContainsKey(columnName))
                        {
                            var converter = tableInfo.ConvertibleColumnConverterDict[columnName];
                            columnsDict[columnName] = ownedPropertyValue == null ? null : converter.ConvertToProvider.Invoke(ownedPropertyValue);
                        }
                        else
                        {
                            columnsDict[columnName] = ownedPropertyValue;
                        }
                    }
                }
            }

            if (tableInfo.BulkConfig.EnableShadowProperties)
            {
                foreach (var shadowPropertyName in shadowPropertyColumnNamesDict.Keys)
                {
                    var shadowProperty = entityPropertiesDict[shadowPropertyName];
                    var columnName = shadowPropertyColumnNamesDict[shadowPropertyName] ?? string.Empty;

                    var propertyValue = default(object);

                    if (tableInfo.BulkConfig.ShadowPropertyValue == null)
                    {
                        propertyValue = dbContext.Entry(entity!).Property(shadowPropertyName).CurrentValue;
                    }
                    else
                    {
                        propertyValue = tableInfo.BulkConfig.ShadowPropertyValue(entity!, shadowPropertyName);
                    }

                    if (tableInfo.ConvertibleColumnConverterDict.ContainsKey(columnName))
                    {
                        propertyValue = tableInfo.ConvertibleColumnConverterDict[columnName].ConvertToProvider.Invoke(propertyValue);
                    }

                    columnsDict[shadowPropertyName] = propertyValue;
                }
            }

            var record = columnsDict.Values.ToArray();
            dataTable.Rows.Add(record);
        }

        return dataTable;
    }

    private static string? GetDiscriminatorColumn(TableInfo tableInfo)
    {
        string? discriminatorColumn = null;
        if (!tableInfo.BulkConfig.EnableShadowProperties && tableInfo.ShadowProperties.Count > 0)
        {
            var stringColumns = tableInfo.ColumnNamesTypesDict.Where(a => a.Value.Contains("char")).Select(a => a.Key).ToList();
            discriminatorColumn = tableInfo.ShadowProperties.Where(a => stringColumns.Contains(a)).ElementAt(0);
        }
        return discriminatorColumn;
    }
    #endregion
}
