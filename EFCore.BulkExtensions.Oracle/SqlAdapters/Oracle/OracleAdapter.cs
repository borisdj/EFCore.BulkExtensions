using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;
using Oracle.ManagedDataAccess.Client;
using System.Data;
using System.Data.Common;
using System.Reflection;

namespace EFCore.BulkExtensions.SqlAdapters.Oracle;

/// <inheritdoc/>
public class OracleAdapter : ISqlOperationsAdapter
{
    private OracleQueryBuilder ProviderSqlQueryBuilder => new();

    /// <inheritdoc/>
    #region Methods
    // Insert
    public void Insert<T>(BulkContext context, Type type, IEnumerable<T> entities, TableInfo tableInfo, Action<decimal>? progress)
        => InsertAsync(context, type, entities, tableInfo, progress, isAsync: false, CancellationToken.None).GetAwaiter().GetResult();

    /// <inheritdoc/>
    public async Task InsertAsync<T>(BulkContext context, Type type, IEnumerable<T> entities, TableInfo tableInfo, Action<decimal>? progress,
        CancellationToken cancellationToken)
        => await InsertAsync(context, type, entities, tableInfo, progress, isAsync: true, CancellationToken.None).ConfigureAwait(false);

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
            var OracleBulkCopy = GetOracleBulkCopy((OracleConnection)connection, transaction, tableInfo.BulkConfig);

            SetOracleBulkCopyConfig(OracleBulkCopy, tableInfo);

            var dataTable = GetDataTable(dbContext, type, entities, OracleBulkCopy, tableInfo);
            IDataReader? dataReader = tableInfo.BulkConfig.DataReader;

            if (isAsync)
            {
                if (dataReader == null) 
                    await Task.Run(() => OracleBulkCopy.WriteToServer(dataTable), cancellationToken).ConfigureAwait(false);
                else
                    await Task.Run(() => OracleBulkCopy.WriteToServer(dataReader), cancellationToken).ConfigureAwait(false);

            }
            else
            {
                if (dataReader == null)
                    OracleBulkCopy.WriteToServer(dataTable);
                else
                    OracleBulkCopy.WriteToServer(dataReader);
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
        => MergeAsync(context, type, entities, tableInfo, operationType, progress, isAsync: false, CancellationToken.None).GetAwaiter().GetResult();

    /// <inheritdoc/>
    public async Task MergeAsync<T>(BulkContext context, Type type, IEnumerable<T> entities, TableInfo tableInfo, OperationType operationType, Action<decimal>? progress,
        CancellationToken cancellationToken) where T : class
        => await MergeAsync(context, type, entities, tableInfo, operationType, progress, isAsync: true, CancellationToken.None).ConfigureAwait(false);

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

                var sqlCreateTableCopy = OracleQueryBuilder.CreateTableCopy(tableInfo.FullTableName, tableInfo.FullTempTableName, tableInfo, tableInfo.BulkConfig.UseTempDB, operationType);
                await ExecuteSqlRawAsync(dbContext, isAsync, sqlCreateTableCopy, cancellationToken).ConfigureAwait(false);
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
                string createUniqueConstrain = OracleQueryBuilder.CreateUniqueConstrain(tableInfo);
                await ExecuteSqlRawAsync(dbContext, isAsync, createUniqueConstrain, cancellationToken).ConfigureAwait(false);
                uniqueConstrainCreated = true;
            }

            if (tableInfo.CreateOutputTable)
            {
                tableInfo.InsertToTempTable = true;
                var sqlCreateOutputTableCopy = OracleQueryBuilder.CreateTableCopy(tableInfo.FullTableName,
                    tableInfo.FullTempOutputTableName, tableInfo, tableInfo.InsertToTempTable, operationType);
                await ExecuteSqlRawAsync(dbContext, isAsync, sqlCreateOutputTableCopy, cancellationToken).ConfigureAwait(false);
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

            var sqlMergeTable = OracleQueryBuilder.MergeTable<T>(tableInfo, operationType);
            await ExecuteSqlRawAsync(dbContext, isAsync, sqlMergeTable, cancellationToken).ConfigureAwait(false);
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
                await ExecuteSqlRawAsync(dbContext, isAsync, tableInfo.BulkConfig.CustomSqlPostProcess, cancellationToken).ConfigureAwait(false);
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
                string dropUniqueConstrain = OracleQueryBuilder.DropUniqueConstrain(tableInfo);
                await ExecuteSqlRawAsync(dbContext, isAsync, dropUniqueConstrain, cancellationToken).ConfigureAwait(false);
            }

            if (!tableInfo.BulkConfig.UseTempDB) // Temp tables are automatically dropped by the database
            {
                if (outputTableCreated)
                {
                    var sqlDropOutputTable = ProviderSqlQueryBuilder.DropTable(tableInfo.FullTempOutputTableName, tableInfo.BulkConfig.UseTempDB);
                    await ExecuteSqlRawAsync(dbContext, isAsync, sqlDropOutputTable, cancellationToken).ConfigureAwait(false);
                }
                if (tempTableCreated)
                {
                    var sqlDropTable = ProviderSqlQueryBuilder.DropTable(tableInfo.FullTempTableName, tableInfo.BulkConfig.UseTempDB);
                    await ExecuteSqlRawAsync(dbContext, isAsync, sqlDropTable, cancellationToken).ConfigureAwait(false);
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
        _ = context.DbContext.Database.ExecuteSqlRaw(sqlTruncateTable);
    }

    /// <inheritdoc/>
    public async Task TruncateAsync(BulkContext context, TableInfo tableInfo, CancellationToken cancellationToken)
    {
        var sqlTruncateTable = ProviderSqlQueryBuilder.TruncateTable(tableInfo.FullTableName);
        _ = await context.DbContext.Database.ExecuteSqlRawAsync(sqlTruncateTable, cancellationToken).ConfigureAwait(false);
    }

    private async Task ExecuteSqlRawAsync(DbContext context, bool isAsync, string commandText, CancellationToken cancellationToken)
    {
        commandText = commandText.Replace("[", "").Replace("]", "");
        if (isAsync)
        {
            await context.Database.ExecuteSqlRawAsync(commandText, cancellationToken).ConfigureAwait(false);
        }
        else
        {
            context.Database.ExecuteSqlRaw(commandText);
        }
    }
    #endregion
    #region Connection

    internal static async Task<(DbConnection, bool)> OpenAndGetOracleConnectionAsync(DbContext context,
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
        string countUniqueConstrain = OracleQueryBuilder.HasUniqueConstrain(tableInfo);

        (DbConnection connection, bool connectionOpenedInternally) = await OpenAndGetOracleConnectionAsync(context, isAsync, cancellationToken).ConfigureAwait(false);

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
                        _ = Int64.TryParse(reader[0].ToString(), out long result);
                        hasUniqueConstrain = result == 1;
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
                        _ = Int64.TryParse(reader[0].ToString(), out long result);
                        hasUniqueConstrain = result == 1;
                    }
                }
            }
        }
        return (hasUniqueConstrain, connectionOpenedInternally);
    }

    private static OracleBulkCopy GetOracleBulkCopy(OracleConnection OracleConnection, IDbContextTransaction? transaction, BulkConfig config)
    {
        var OracleBulkCopy = new OracleBulkCopy(OracleConnection);

        return OracleBulkCopy;
    }
    /// <param name="OracleBulkCopy"></param>
    /// <param name="tableInfo"></param>
    private static void SetOracleBulkCopyConfig(OracleBulkCopy OracleBulkCopy, TableInfo tableInfo)
    {
        string destinationTable = tableInfo.InsertToTempTable ? tableInfo.FullTempTableName
                                                              : tableInfo.FullTableName;
        destinationTable = destinationTable.Replace("[", "")
                                           .Replace("]", "");
        OracleBulkCopy.DestinationTableName = destinationTable;

        OracleBulkCopy.NotifyAfter = tableInfo.BulkConfig.NotifyAfter ?? tableInfo.BulkConfig.BatchSize;
        OracleBulkCopy.BulkCopyTimeout = tableInfo.BulkConfig.BulkCopyTimeout ?? OracleBulkCopy.BulkCopyTimeout;
    }

    #endregion
    #region DataTable
    /// <summary>
    /// Supports <see cref="OracleBulkCopy"/>
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <param name="context"></param>
    /// <param name="type"></param>
    /// <param name="entities"></param>
    /// <param name="OracleBulkCopy"></param>
    /// <param name="tableInfo"></param>
    /// <returns></returns>
    public static DataTable GetDataTable<T>(DbContext context, Type type, IEnumerable<T> entities, OracleBulkCopy OracleBulkCopy, TableInfo tableInfo)
    {
        DataTable dataTable = InnerGetDataTable(context, ref type, entities, tableInfo);

        int sourceOrdinal = 0;
        foreach (DataColumn item in dataTable.Columns)  //Add mapping
        {
            OracleBulkCopy.ColumnMappings.Add(new OracleBulkCopyColumnMapping(sourceOrdinal, item.ColumnName));
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
    private static DataTable InnerGetDataTable<T>(DbContext context, ref Type type, IEnumerable<T> entities, TableInfo tableInfo)
    {
        var dataTable = new DataTable();
        var columnsDict = new Dictionary<string, object?>();
        var ownedEntitiesMappedProperties = new HashSet<string>();

        var objectIdentifier = tableInfo.ObjectIdentifier;
        type = tableInfo.HasAbstractList ? entities.ElementAt(0)!.GetType() : type;
        var entityType = context.Model.FindEntityType(type) ?? throw new ArgumentException($"Unable to determine entity type from given type - {type.Name}");
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

            if (entityPropertiesDict.TryGetValue(property.Name, out Microsoft.EntityFrameworkCore.Metadata.IProperty? propertyEntityType))
            {
                string columnName = propertyEntityType.GetColumnName(objectIdentifier) ?? string.Empty;

                var isConvertible = tableInfo.ConvertibleColumnConverterDict.ContainsKey(columnName);
                var propertyType = isConvertible ? tableInfo.ConvertibleColumnConverterDict[columnName].ProviderClrType
                                                 : property.PropertyType;

                var underlyingType = Nullable.GetUnderlyingType(propertyType);
                if (underlyingType != null)
                {
                    propertyType = underlyingType;
                }

                if (!columnsDict.ContainsKey(property.Name) && !hasDefaultVauleOnInsert)
                {
                    _ = dataTable.Columns.Add(columnName, propertyType);
                    columnsDict.Add(property.Name, null);
                }
            }
            else if (entityShadowFkPropertiesDict.TryGetValue(property.Name, out Microsoft.EntityFrameworkCore.Metadata.IProperty? fk))
            {
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

                if (columnName is not null && !(columnsDict.ContainsKey(columnName)) && !hasDefaultVauleOnInsert)
                {
                    dataTable.Columns.Add(columnName, propertyType);
                    columnsDict.Add(columnName, null);
                }
            }
            else if (entityNavigationOwnedDict.ContainsKey(property.Name)) // isOWned
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
                            _ = ownedEntitiesMappedProperties.Add(property.Name + "_" + ownedEntityProperty.Name);
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

                            if (tableInfo.ConvertibleColumnConverterDict.TryGetValue(propertyName, out Microsoft.EntityFrameworkCore.Storage.ValueConversion.ValueConverter? convertor))
                            {
                                var underlyingType = Nullable.GetUnderlyingType(convertor.ProviderClrType) ?? convertor.ProviderClrType;
                                _ = dataTable.Columns.Add(columnName, underlyingType);
                            }
                            else
                            {
                                var ownedPropertyType = Nullable.GetUnderlyingType(innerProperty.PropertyType) ?? innerProperty.PropertyType;
                                _ = dataTable.Columns.Add(columnName, ownedPropertyType);
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
                object? propertyValue = tableInfo.FastPropertyDict.TryGetValue(property.Name, out FastProperty? value) ? value.Get(entity!) : null;

                var hasDefaultVauleOnInsert = tableInfo.BulkConfig.OperationType == OperationType.Insert
                                           && !tableInfo.BulkConfig.SetOutputIdentity
                                           && tableInfo.DefaultValueProperties.Contains(property.Name);

                if (hasConverterProperties && tableInfo.ConvertiblePropertyColumnDict.ContainsKey(property.Name))
                {
                    string columnName = tableInfo.ConvertiblePropertyColumnDict[property.Name];
                    propertyValue = tableInfo.ConvertibleColumnConverterDict[columnName].ConvertToProvider.Invoke(propertyValue);
                }

                if (entityPropertiesDict.ContainsKey(property.Name) && !hasDefaultVauleOnInsert)
                {
                    columnsDict[property.Name] = propertyValue;
                }
                else if (entityShadowFkPropertiesDict.TryGetValue(property.Name, out Microsoft.EntityFrameworkCore.Metadata.IProperty? foreignKeyShadowProperty))
                {
                    var columnName = entityShadowFkPropertyColumnNamesDict[property.Name] ?? string.Empty;
                    if (!entityPropertiesDict.TryGetValue(columnName, out var entityProperty) || entityProperty is null)
                    {
                        continue; // BulkRead
                    };
                    columnsDict[columnName] = propertyValue != null ? 
                        foreignKeyShadowProperty.FindFirstPrincipal()?.PropertyInfo?.GetValue(propertyValue) // TODO Try to optimize
                        : propertyValue;
                }
                else if (entityNavigationOwnedDict.ContainsKey(property.Name) && !tableInfo.LoadOnlyPKColumn)
                {
                    var ownedProperties = property.PropertyType.GetProperties().Where(a => ownedEntitiesMappedProperties.Contains(property.Name + "_" + a.Name));
                    foreach (var ownedProperty in ownedProperties)
                    {
                        var columnName = $"{property.Name}_{ownedProperty.Name}";
                        var ownedPropertyValue = propertyValue == null ? null : tableInfo.FastPropertyDict[columnName].Get(propertyValue);

                        if (tableInfo.ConvertibleColumnConverterDict.TryGetValue(columnName, out Microsoft.EntityFrameworkCore.Storage.ValueConversion.ValueConverter? converter))
                        {
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
                        propertyValue = context.Entry(entity!).Property(shadowPropertyName).CurrentValue;
                    }
                    else
                    {
                        propertyValue = tableInfo.BulkConfig.ShadowPropertyValue(entity!, shadowPropertyName);
                    }

                    if (tableInfo.ConvertibleColumnConverterDict.TryGetValue(columnName, out Microsoft.EntityFrameworkCore.Storage.ValueConversion.ValueConverter? value))
                    {
                        propertyValue = value.ConvertToProvider.Invoke(propertyValue);
                    }

                    columnsDict[shadowPropertyName] = propertyValue;
                }
            }

            var record = columnsDict.Values.ToArray();
            _ = dataTable.Rows.Add(record);
        }

        return dataTable;
    }

    private static string? GetDiscriminatorColumn(TableInfo tableInfo)
    {
        string? discriminatorColumn = null;
        if (!tableInfo.BulkConfig.EnableShadowProperties && tableInfo.ShadowProperties.Count > 0)
        {
            var stringColumns = tableInfo.ColumnNamesTypesDict.Where(a => a.Value.Contains("char")).Select(a => a.Key).ToList();
            if (tableInfo.ShadowProperties.FirstOrDefault(a => stringColumns.Contains(a)) is { } c)
                discriminatorColumn = c;
        }
        return discriminatorColumn;
    }
    #endregion
}
