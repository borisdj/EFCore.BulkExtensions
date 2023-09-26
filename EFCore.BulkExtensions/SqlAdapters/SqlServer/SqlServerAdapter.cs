﻿using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Internal;
using Microsoft.EntityFrameworkCore.Storage;
using NetTopologySuite.Algorithm;
using NetTopologySuite.Geometries;
using NetTopologySuite.IO;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Data.Common;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Security.AccessControl;
using System.Threading;
using System.Threading.Tasks;

namespace EFCore.BulkExtensions.SqlAdapters.SqlServer;

/// <inheritdoc/>
public class SqlServerAdapter : ISqlOperationsAdapter
{
    private SqlServerQueryBuilder ProviderSqlQueryBuilder { get; set; } = new SqlServerQueryBuilder();

    /// <inheritdoc/>
    #region Methods
    // Insert
    public void Insert<T>(DbContext context, Type type, IEnumerable<T> entities, TableInfo tableInfo, Action<decimal>? progress)
    {
        InsertAsync(context, type, entities, tableInfo, progress, isAsync: false, CancellationToken.None).GetAwaiter().GetResult();
    }

    /// <inheritdoc/>
    public async Task InsertAsync<T>(DbContext context, Type type, IEnumerable<T> entities, TableInfo tableInfo, Action<decimal>? progress, 
        CancellationToken cancellationToken)
    {
        await InsertAsync(context, type, entities, tableInfo, progress, isAsync: true, cancellationToken).ConfigureAwait(false);
    }
    // Public Async and NonAsync are merged into single operation flow with protected method using arg: bool isAsync (keeps code DRY)
    // https://docs.microsoft.com/en-us/archive/msdn-magazine/2015/july/async-programming-brownfield-async-development#the-flag-argument-hack
    /// <inheritdoc/>
    protected static async Task InsertAsync<T>(DbContext context, Type type, IEnumerable<T> entities, TableInfo tableInfo, Action<decimal>? progress, 
        bool isAsync, CancellationToken cancellationToken)
    {
        tableInfo.CheckToSetIdentityForPreserveOrder(tableInfo, entities);
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
            SetSqlBulkCopyConfig(sqlBulkCopy, tableInfo, entities, setColumnMapping, progress);
            try
            {
                var dataTable = GetDataTable(context, type, entities, sqlBulkCopy, tableInfo);
                IDataReader? dataReader = tableInfo.BulkConfig.DataReader;

                if (isAsync)
                {
                    if(dataReader == null)
                        await sqlBulkCopy.WriteToServerAsync(dataTable, cancellationToken).ConfigureAwait(false);
                    else
                        await sqlBulkCopy.WriteToServerAsync(dataReader, cancellationToken).ConfigureAwait(false);
                }
                else
                {
                    if (dataReader == null)
                        sqlBulkCopy.WriteToServer(dataTable);
                    else
                        sqlBulkCopy.WriteToServer(dataReader);
                }
            }
            catch (Exception ex)
            {
                if (ex.Message.Contains(BulkExceptionMessage.ColumnMappingNotMatch))
                {
                    bool tableExist = isAsync ? await TableInfo.CheckTableExistAsync(context, tableInfo, isAsync: true, cancellationToken).ConfigureAwait(false)
                                                    : TableInfo.CheckTableExistAsync(context, tableInfo, isAsync: false, CancellationToken.None).GetAwaiter().GetResult();

                    if (!tableExist)
                    {
                        var sqlCreateTableCopy = new SqlServerQueryBuilder().CreateTableCopy(tableInfo.FullTableName, tableInfo.FullTempTableName, tableInfo);
                        var sqlDropTable = new SqlServerQueryBuilder().DropTable(tableInfo.FullTempTableName, tableInfo.BulkConfig.UseTempDB);

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
        if (!tableInfo.CreateOutputTable)
        {
            tableInfo.CheckToSetIdentityForPreserveOrder(tableInfo, entities, reset: true);
        }
    }

    /// <inheritdoc/>
    public void Merge<T>(DbContext context, Type type, IEnumerable<T> entities, TableInfo tableInfo, OperationType operationType, Action<decimal>? progress) where T : class
    {
        MergeAsync(context, type, entities, tableInfo, operationType, progress, isAsync: false, CancellationToken.None).GetAwaiter().GetResult();
    }

    /// <inheritdoc/>
    public async Task MergeAsync<T>(DbContext context, Type type, IEnumerable<T> entities, TableInfo tableInfo, OperationType operationType, Action<decimal>? progress, CancellationToken cancellationToken) where T : class
    {
        await MergeAsync(context, type, entities, tableInfo, operationType, progress, isAsync: true, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc/>
    protected async Task MergeAsync<T>(DbContext context, Type type, IEnumerable<T> entities, TableInfo tableInfo, OperationType operationType, Action<decimal>? progress, bool isAsync, CancellationToken cancellationToken) where T : class
    {
        bool tempTableCreated = false;
        bool outputTableCreated = false;
        bool identityInsertIsSet = false;
        bool keepIdentity = tableInfo.BulkConfig.SqlBulkCopyOptions.HasFlag(SqlBulkCopyOptions.KeepIdentity);
        try
        {
            var entityPropertyWithDefaultValue = entities.GetPropertiesWithDefaultValue(type, tableInfo);

            if (tableInfo.BulkConfig.CustomSourceTableName == null)
            {
                tableInfo.InsertToTempTable = true;

                var sqlCreateTableCopy = new SqlServerQueryBuilder().CreateTableCopy(tableInfo.FullTableName, tableInfo.FullTempTableName, tableInfo);
                if (isAsync)
                {
                    await context.Database.ExecuteSqlRawAsync(sqlCreateTableCopy, cancellationToken).ConfigureAwait(false);
                }
                else
                {
                    context.Database.ExecuteSqlRaw(sqlCreateTableCopy);
                }
                tempTableCreated = true;

                if (isAsync)
                {
                    await InsertAsync(context, type, entities, tableInfo, progress, cancellationToken).ConfigureAwait(false);
                }
                else
                {
                    Insert(context, type, entities, tableInfo, progress);
                }
            }

            if (tableInfo.CreateOutputTable)
            {
                var sqlCreateOutputTableCopy = new SqlServerQueryBuilder().CreateTableCopy(tableInfo.FullTableName, tableInfo.FullTempOutputTableName, tableInfo, true);
                if (isAsync)
                {
                    await context.Database.ExecuteSqlRawAsync(sqlCreateOutputTableCopy, cancellationToken).ConfigureAwait(false);
                }
                else
                {
                    context.Database.ExecuteSqlRaw(sqlCreateOutputTableCopy);
                }
                outputTableCreated = true;

                if (operationType == OperationType.InsertOrUpdateOrDelete)
                {
                    // Output returns all changes including Deleted rows with all NULL values, so if TempOutput.Id col not Nullable it breaks
                    var sqlAlterTableColumnsToNullable = SqlQueryBuilder.AlterTableColumnsToNullable(tableInfo.FullTempOutputTableName, tableInfo);
                    if (isAsync)
                    {
                        await context.Database.ExecuteSqlRawAsync(sqlAlterTableColumnsToNullable, cancellationToken).ConfigureAwait(false);
                    }
                    else
                    {
                        context.Database.ExecuteSqlRaw(sqlAlterTableColumnsToNullable);
                    }
                }
            }

            if (keepIdentity && tableInfo.HasIdentity)
            {
                var sqlSetIdentityInsertTrue = SqlQueryBuilder.SetIdentityInsert(tableInfo.FullTableName, true);
                if (isAsync)
                {
                    await context.Database.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
                    await context.Database.ExecuteSqlRawAsync(sqlSetIdentityInsertTrue, cancellationToken).ConfigureAwait(false);
                }
                else
                {
                    context.Database.OpenConnection();
                    context.Database.ExecuteSqlRaw(sqlSetIdentityInsertTrue);
                }
                identityInsertIsSet = true;
            }

            var (sql, parameters) = SqlQueryBuilder.MergeTable<T>(context, tableInfo, operationType, entityPropertyWithDefaultValue);
            if (isAsync)
            {
                await context.Database.ExecuteSqlRawAsync(sql, parameters, cancellationToken).ConfigureAwait(false);
            }
            else
            {
                context.Database.ExecuteSqlRaw(sql, parameters);
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
        }
        finally
        {
            if (!tableInfo.BulkConfig.UseTempDB) // When UseTempDB is set temp tables are automaticaly dropped by Db
            {
                if (outputTableCreated)
                {
                    var sqlDropOutputTable = new SqlServerQueryBuilder().DropTable(tableInfo.FullTempOutputTableName, tableInfo.BulkConfig.UseTempDB);
                    if (isAsync)
                    {
                        await context.Database.ExecuteSqlRawAsync(sqlDropOutputTable, cancellationToken).ConfigureAwait(false);
                    }
                    else
                    {
                        context.Database.ExecuteSqlRaw(sqlDropOutputTable);
                    }
                }
                if (tempTableCreated) // otherwise following lines execute the drop
                {
                    var sqlDropTable = new SqlServerQueryBuilder().DropTable(tableInfo.FullTempTableName, tableInfo.BulkConfig.UseTempDB);
                    if (isAsync)
                    {
                        await context.Database.ExecuteSqlRawAsync(sqlDropTable, cancellationToken).ConfigureAwait(false);
                    }
                    else
                    {
                        context.Database.ExecuteSqlRaw(sqlDropTable);
                    }
                }
            }

            if (identityInsertIsSet)
            {
                var sqlSetIdentityInsertFalse = SqlQueryBuilder.SetIdentityInsert(tableInfo.FullTableName, false);
                if (isAsync)
                {
                    await context.Database.ExecuteSqlRawAsync(sqlSetIdentityInsertFalse, cancellationToken).ConfigureAwait(false);
                }
                else
                {
                    context.Database.ExecuteSqlRaw(sqlSetIdentityInsertFalse);
                }
                context.Database.CloseConnection();
            }
        }
    }

    /// <inheritdoc/>
    public void Read<T>(DbContext context, Type type, IEnumerable<T> entities, TableInfo tableInfo, Action<decimal>? progress) where T : class
    {
        ReadAsync(context, type, entities, tableInfo, progress, isAsync: false, CancellationToken.None).GetAwaiter().GetResult();
    }

    /// <inheritdoc/>
    public async Task ReadAsync<T>(DbContext context, Type type, IEnumerable<T> entities, TableInfo tableInfo, Action<decimal>? progress, CancellationToken cancellationToken) where T : class
    {
        await ReadAsync(context, type, entities, tableInfo, progress, isAsync: true, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc/>
    protected async Task ReadAsync<T>(DbContext context, Type type, IEnumerable<T> entities, TableInfo tableInfo, Action<decimal>? progress, bool isAsync, CancellationToken cancellationToken) where T : class
    {
        bool tempTableCreated = false;
        try
        {
            Dictionary<string, string> previousPropertyColumnNamesDict = tableInfo.ConfigureBulkReadTableInfo();

            var sqlCreateTableCopy = new SqlServerQueryBuilder().CreateTableCopy(tableInfo.FullTableName, tableInfo.FullTempTableName, tableInfo);
            if (isAsync)
            {
                await context.Database.ExecuteSqlRawAsync(sqlCreateTableCopy, cancellationToken).ConfigureAwait(false);
            }
            else
            {
                context.Database.ExecuteSqlRaw(sqlCreateTableCopy);
            }
            tempTableCreated = true;

            if (isAsync)
            {
                await InsertAsync(context, type, entities, tableInfo, progress, cancellationToken).ConfigureAwait(false);
            }
            else
            {
                InsertAsync(context, type, entities, tableInfo, progress, isAsync: false, cancellationToken).GetAwaiter().GetResult();
            }

            tableInfo.PropertyColumnNamesDict = tableInfo.OutputPropertyColumnNamesDict;

            var sqlSelectJoinTable = SqlQueryBuilder.SelectJoinTable(tableInfo);

            tableInfo.PropertyColumnNamesDict = previousPropertyColumnNamesDict; // TODO Consider refactor and integrate with TimeStampPropertyName, also check for Calculated props.
                                                                                 // Output only PropertisToInclude and for getting Id with SetOutputIdentity
            if (tableInfo.TimeStampPropertyName != null && !tableInfo.PropertyColumnNamesDict.ContainsKey(tableInfo.TimeStampPropertyName) && tableInfo.TimeStampColumnName is not null)
            {
                tableInfo.PropertyColumnNamesDict.Add(tableInfo.TimeStampPropertyName, tableInfo.TimeStampColumnName);
            }

            List<T> existingEntities = tableInfo.LoadOutputEntities<T>(context, type, sqlSelectJoinTable);

            if (tableInfo.BulkConfig.ReplaceReadEntities)
            {
                tableInfo.ReplaceReadEntities(entities, existingEntities);
            }
            else
            {
                tableInfo.UpdateReadEntities(entities, existingEntities, context);
            }

            if (tableInfo.TimeStampPropertyName != null && !tableInfo.PropertyColumnNamesDict.ContainsKey(tableInfo.TimeStampPropertyName))
            {
                tableInfo.PropertyColumnNamesDict.Remove(tableInfo.TimeStampPropertyName);
            }
        }
        finally
        {
            if (!tableInfo.BulkConfig.UseTempDB)
            {
                if (tempTableCreated)
                {
                    var sqlDropTable = new SqlServerQueryBuilder().DropTable(tableInfo.FullTempTableName, tableInfo.BulkConfig.UseTempDB);
                    if (isAsync)
                    {
                        await context.Database.ExecuteSqlRawAsync(sqlDropTable, cancellationToken).ConfigureAwait(false);
                    }
                    else
                    {
                        context.Database.ExecuteSqlRaw(sqlDropTable);
                    }
                }
            }
        }
    }

    /// <inheritdoc/>
    public void Truncate(DbContext context, TableInfo tableInfo)
    {
        var sqlTruncateTable = new SqlServerQueryBuilder().TruncateTable(tableInfo.FullTableName);
        context.Database.ExecuteSqlRaw(sqlTruncateTable);
    }

    /// <inheritdoc/>
    public async Task TruncateAsync(DbContext context, TableInfo tableInfo, CancellationToken cancellationToken)
    {
        var sqlTruncateTable = new SqlServerQueryBuilder().TruncateTable(tableInfo.FullTableName);
        await context.Database.ExecuteSqlRawAsync(sqlTruncateTable, cancellationToken).ConfigureAwait(false);
    }
    #endregion

    #region Connection
    private static SqlBulkCopy GetSqlBulkCopy(SqlConnection sqlConnection, IDbContextTransaction? transaction, BulkConfig config)
    {
        var sqlTransaction = transaction == null ? null : (SqlTransaction)transaction.GetUnderlyingTransaction(config);
        var sqlBulkCopy = new SqlBulkCopy(sqlConnection, config.SqlBulkCopyOptions, sqlTransaction);
        if (config.SqlBulkCopyColumnOrderHints != null)
        {
            foreach(var hint in config.SqlBulkCopyColumnOrderHints)
                sqlBulkCopy.ColumnOrderHints.Add(hint);
        }
        return sqlBulkCopy;
    }
    /// <summary>
    /// Supports <see cref="SqlBulkCopy"/>
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <param name="sqlBulkCopy"></param>
    /// <param name="tableInfo"></param>
    /// <param name="entities"></param>
    /// <param name="setColumnMapping"></param>
    /// <param name="progress"></param>
    private static void SetSqlBulkCopyConfig<T>(SqlBulkCopy sqlBulkCopy, TableInfo tableInfo, IEnumerable<T> entities, bool setColumnMapping, Action<decimal>? progress)
    {
        sqlBulkCopy.DestinationTableName = tableInfo.InsertToTempTable ? tableInfo.FullTempTableName : tableInfo.FullTableName;
        sqlBulkCopy.BatchSize = tableInfo.BulkConfig.BatchSize;
        sqlBulkCopy.NotifyAfter = tableInfo.BulkConfig.NotifyAfter ?? tableInfo.BulkConfig.BatchSize;
        sqlBulkCopy.SqlRowsCopied += (sender, e) =>
        {
            progress?.Invoke(ProgressHelper.GetProgress(entities.Count(), e.RowsCopied)); // round to 4 decimal places
        };
        sqlBulkCopy.BulkCopyTimeout = tableInfo.BulkConfig.BulkCopyTimeout ?? sqlBulkCopy.BulkCopyTimeout;
        sqlBulkCopy.EnableStreaming = tableInfo.BulkConfig.EnableStreaming;

        if (setColumnMapping)
        {
            foreach (var element in tableInfo.PropertyColumnNamesDict)
            {
                sqlBulkCopy.ColumnMappings.Add(element.Key, element.Value);
            }
        }
    }

    #endregion

    #region DataTable
    /// <summary>
    /// Supports <see cref="SqlBulkCopy"/>
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <param name="context"></param>
    /// <param name="type"></param>
    /// <param name="entities"></param>
    /// <param name="sqlBulkCopy"></param>
    /// <param name="tableInfo"></param>
    /// <returns></returns>
    public static DataTable GetDataTable<T>(DbContext context, Type type, IEnumerable<T> entities, SqlBulkCopy sqlBulkCopy, TableInfo tableInfo)
    {
        DataTable dataTable = InnerGetDataTable(context, ref type, entities, tableInfo);

        foreach (DataColumn item in dataTable.Columns)  //Add mapping
        {
            sqlBulkCopy.ColumnMappings.Add(item.ColumnName, item.ColumnName);
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
    private static DataTable InnerGetDataTable<T>(DbContext context, ref Type type, IEnumerable<T> entities, TableInfo tableInfo)
    {
        var dataTable = new DataTable();
        var columnsDict = new Dictionary<string, object?>();
        var ownedEntitiesMappedProperties = new HashSet<string>();

        var databaseType = SqlAdaptersMapping.GetDatabaseType();
        var isSqlServer = databaseType == SqlType.SqlServer;
        var sqlServerBytesWriter = new SqlServerBytesWriter();

        var objectIdentifier = tableInfo.ObjectIdentifier;
        type = tableInfo.HasAbstractList ? entities.ElementAt(0)!.GetType() : type;
        var entityType = context.Model.FindEntityType(type) ?? throw new ArgumentException($"Unable to determine entity type from given type - {type.Name}");
        var entityTypeProperties = entityType.GetProperties();
        var entityPropertiesDict = entityTypeProperties.Where(a => tableInfo.PropertyColumnNamesDict.ContainsKey(a.Name) ||
                                                                   (tableInfo.BulkConfig.OperationType != OperationType.Read && a.Name == tableInfo.TimeStampPropertyName))
                                                       .ToDictionary(a => a.Name, a => a);
        var entityNavigationOwnedDict = entityType.GetNavigations().Where(a => a.TargetEntityType.IsOwned()).ToDictionary(a => a.Name, a => a);
        var entityShadowFkPropertiesDict = entityTypeProperties.Where(a => a.IsShadowProperty() &&
                                                                           a.IsForeignKey() &&
                                                                           a.GetContainingForeignKeys().FirstOrDefault()?.DependentToPrincipal?.Name != null)
                                                                     .ToDictionary(x => x.GetContainingForeignKeys()?.First()?.DependentToPrincipal?.Name ?? string.Empty, a => a);

        var entityShadowFkPropertyColumnNamesDict = entityShadowFkPropertiesDict
            .ToDictionary(a => a.Key, a => a.Value.GetColumnName(objectIdentifier));
        var shadowPropertyColumnNamesDict = entityPropertiesDict
            .Where(a => a.Value.IsShadowProperty()).ToDictionary(a => a.Key, a => a.Value.GetColumnName(objectIdentifier));

        var properties = type.GetProperties(BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic);
        var discriminatorColumn = GetDiscriminatorColumn(tableInfo);

        foreach (var property in properties)
        {
            var hasDefaultValueOnInsert = tableInfo.BulkConfig.OperationType == OperationType.Insert
                && !tableInfo.BulkConfig.SetOutputIdentity
                && tableInfo.DefaultValueProperties.Contains(property.Name);

            if (entityPropertiesDict.ContainsKey(property.Name))
            {
                var propertyEntityType = entityPropertiesDict[property.Name];
                string columnName = propertyEntityType.GetColumnName(objectIdentifier) ?? string.Empty;

                var isConvertible = tableInfo.ConvertibleColumnConverterDict.ContainsKey(columnName);
                var propertyType = isConvertible ? tableInfo.ConvertibleColumnConverterDict[columnName].ProviderClrType : property.PropertyType;

                var underlyingType = Nullable.GetUnderlyingType(propertyType);
                if (underlyingType != null)
                {
                    propertyType = underlyingType;
                }

                if (isSqlServer && (propertyType == typeof(Geometry) || propertyType.IsSubclassOf(typeof(Geometry))))
                {
                    propertyType = typeof(byte[]);
                    tableInfo.HasSpatialType = true;
                    if (tableInfo.BulkConfig.PropertiesToIncludeOnCompare != null || tableInfo.BulkConfig.PropertiesToIncludeOnCompare != null)
                    {
                        throw new InvalidOperationException("OnCompare properties Config can not be set for Entity with Spatial types like 'Geometry'");
                    }
                }
                if (isSqlServer && (propertyType == typeof(HierarchyId) || propertyType.IsSubclassOf(typeof(HierarchyId))))
                {
                    propertyType = typeof(byte[]);
                }

                // when TimeStamp has default value on Property (issue 958, test TimeStamp example with multiple DBs, second being Sqlite that does not have internal TS)
                var omitTimeStamp = property.Name == tableInfo.TimeStampPropertyName && tableInfo.BulkConfig.DoNotUpdateIfTimeStampChanged == false;

                if (!columnsDict.ContainsKey(property.Name) && !hasDefaultValueOnInsert && !omitTimeStamp)
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
                var propertyType = isConvertible ? tableInfo.ConvertibleColumnConverterDict[columnName ?? string.Empty].ProviderClrType : entityProperty.ClrType;

                var underlyingType = Nullable.GetUnderlyingType(propertyType);
                if (underlyingType != null)
                {
                    propertyType = underlyingType;
                }

                if (propertyType == typeof(Geometry) && isSqlServer)
                {
                    propertyType = typeof(byte[]);
                }

                if (propertyType == typeof(HierarchyId) && isSqlServer)
                {
                    propertyType = typeof(byte[]);
                }

                if (columnName is not null && !(columnsDict.ContainsKey(columnName)) && !hasDefaultValueOnInsert)
                {
                    dataTable.Columns.Add(columnName, propertyType);
                    columnsDict.Add(columnName, null);
                }
            }
            else if (entityNavigationOwnedDict.ContainsKey(property.Name) && !tableInfo.OwnedJsonTypesDict.ContainsKey(property.Name)) // isOWned
            {
                //Type? navOwnedType = type.Assembly.GetType(property.PropertyType.FullName!); // was not used

                var ownedEntityType = context.Model.FindEntityType(property.PropertyType);
                if (ownedEntityType == null)
                {
                    ownedEntityType = context.Model.GetEntityTypes().SingleOrDefault(x => x.ClrType == property.PropertyType && x.Name.StartsWith(entityType.Name + "." + property.Name + "#"));
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
                                
                                if (isSqlServer && (ownedPropertyType == typeof(Geometry) || ownedPropertyType.IsSubclassOf(typeof(Geometry))))
                                {
                                    ownedPropertyType = typeof(byte[]);
                                    tableInfo.HasSpatialType = true;
                                    if (tableInfo.BulkConfig.PropertiesToIncludeOnCompare != null || tableInfo.BulkConfig.PropertiesToIncludeOnCompare != null)
                                    {
                                        throw new InvalidOperationException("OnCompare properties Config can not be set for Entity with Spatial types like 'Geometry'");
                                    }
                                }

                                if (isSqlServer && (ownedPropertyType == typeof(HierarchyId) || ownedPropertyType.IsSubclassOf(typeof(HierarchyId))))
                                {
                                    ownedPropertyType = typeof(byte[]);
                                }

                                dataTable.Columns.Add(columnName, ownedPropertyType);
                            }
                            columnsDict.Add(property.Name + "_" + innerProperty.Name, null);
                        }
                    }
                }
            }
            else if (tableInfo.OwnedJsonTypesDict.ContainsKey(property.Name) && tableInfo.BulkConfig.OperationType != OperationType.Read) // isJson
            {
                dataTable.Columns.Add(property.Name, typeof(string));
                columnsDict.Add(property.Name, null);
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

                if (isSqlServer && (propertyType == typeof(Geometry) || propertyType.IsSubclassOf(typeof(Geometry))))
                {
                    propertyType = typeof(byte[]);
                }

                if (isSqlServer && (propertyType == typeof(HierarchyId) || propertyType.IsSubclassOf(typeof(HierarchyId))))
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
                            || tableInfo.OwnedTypesDict.ContainsKey(a.Name) // omit virtual Navigation (except Owned and ShadowNavig.) since it's Getter can cause unwanted Select-s from Db
                            || tableInfo.OwnedJsonTypesDict.ContainsKey(a.Name));

            foreach (var property in propertiesToLoad)
            {
                object? propertyValue = tableInfo.FastPropertyDict.ContainsKey(property.Name)
                    ? tableInfo.FastPropertyDict[property.Name].Get(entity!)
                    : null;

                var hasDefaultVauleOnInsert = tableInfo.BulkConfig.OperationType == OperationType.Insert
                    && !tableInfo.BulkConfig.SetOutputIdentity
                    && tableInfo.DefaultValueProperties.Contains(property.Name);

                if (tableInfo.BulkConfig.DateTime2PrecisionForceRound
                    && isSqlServer
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

                if (tableInfo.HasSpatialType && propertyValue is Geometry geometryValue)
                {
                    geometryValue.SRID = tableInfo.BulkConfig.SRID;

                    if (tableInfo.PropertyColumnNamesDict.ContainsKey(property.Name))
                    {
                        sqlServerBytesWriter.IsGeography = tableInfo.ColumnNamesTypesDict[tableInfo.PropertyColumnNamesDict[property.Name]] == "geography"; // "geography" type is default, otherwise it's "geometry" type
                    }

                    propertyValue = sqlServerBytesWriter.Write(geometryValue);
                }

                if (propertyValue is HierarchyId hierarchyValue && isSqlServer)
                {
                    using MemoryStream memStream = new();
                    using BinaryWriter binWriter = new(memStream);

                    //hierarchyValue.Write(binWriter); // removed as of EF8 (throws: Error CS1061  'HierarchyId' does not contain a definition for 'Write' and no accessible extension method 'Write' accepting a first argument of type 'HierarchyId' could be found.
                    propertyValue = memStream.ToArray();
                }
                var omitTimeStamp = property.Name == tableInfo.TimeStampPropertyName && tableInfo.BulkConfig.DoNotUpdateIfTimeStampChanged == false;
                if (entityPropertiesDict.ContainsKey(property.Name) && !hasDefaultVauleOnInsert && !omitTimeStamp)
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
                    columnsDict[columnName] = propertyValue == null
                        ? null
                        : foreignKeyShadowProperty.FindFirstPrincipal()?.PropertyInfo?.GetValue(propertyValue); // TODO Check if can be optimized
                }
                else if (entityNavigationOwnedDict.ContainsKey(property.Name) && !tableInfo.OwnedJsonTypesDict.ContainsKey(property.Name) && !tableInfo.LoadOnlyPKColumn)
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
                        else if(tableInfo.HasSpatialType && ownedPropertyValue is Geometry ownedGeometryValue)
                        {
                            ownedGeometryValue.SRID = tableInfo.BulkConfig.SRID;

                            if (tableInfo.PropertyColumnNamesDict.ContainsKey(property.Name))
                            {
                                sqlServerBytesWriter.IsGeography = tableInfo.ColumnNamesTypesDict[tableInfo.PropertyColumnNamesDict[property.Name]] == "geography"; // "geography" type is default, otherwise it's "geometry" type
                            }

                            columnsDict[columnName] = sqlServerBytesWriter.Write(ownedGeometryValue);
                        }
                        else
                        {
                            columnsDict[columnName] = ownedPropertyValue;
                        }
                    }
                }
                else if (tableInfo.OwnedJsonTypesDict.ContainsKey(property.Name) && !tableInfo.LoadOnlyPKColumn && tableInfo.BulkConfig.OperationType != OperationType.Read) // isJson
                {
                    var columnName = property.Name; // TODO if Diff
                    var jsonPropertyValue = tableInfo.FastPropertyDict[columnName].Get(entity!);
                    string jsonValue = System.Text.Json.JsonSerializer.Serialize(jsonPropertyValue);
                    columnsDict[columnName] = jsonValue;
                }
            }

            if (tableInfo.BulkConfig.EnableShadowProperties) // TODO change for regular Shadow props to work even without this config
            {
                foreach (var shadowPropertyName in shadowPropertyColumnNamesDict.Keys)
                {
                    var shadowProperty = entityPropertiesDict[shadowPropertyName];
                    var columnName = shadowPropertyColumnNamesDict[shadowPropertyName] ?? string.Empty;

                    var propertyValue = default(object);

                    if (tableInfo.BulkConfig.ShadowPropertyValue == null)
                    {
                        propertyValue = context.Entry(entity!).Property(shadowPropertyName).CurrentValue;
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
            var shadowProps = tableInfo.ShadowProperties.Where(a => stringColumns.Contains(a));

            if (shadowProps is not null && shadowProps.Any())
            {
                discriminatorColumn = shadowProps.ElementAt(0);
            }
        }
        return discriminatorColumn;
    }
    #endregion
}
