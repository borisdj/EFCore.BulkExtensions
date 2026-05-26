using GaussDB;
using Microsoft.EntityFrameworkCore;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace EFCore.BulkExtensions.SqlAdapters.GaussDB;

/// <inheritdoc/>
public class GaussDBAdapter : ISqlOperationsAdapter
{
    private GaussDBQueryBuilder ProviderSqlQueryBuilder => new();

    /// <inheritdoc/>
    public void Insert<T>(BulkContext context, Type type, IEnumerable<T> entities, TableInfo tableInfo, Action<decimal>? progress)
    {
        InsertAsync(context, entities, tableInfo, progress, isAsync: false, CancellationToken.None).GetAwaiter().GetResult();
    }

    /// <inheritdoc/>
    public async Task InsertAsync<T>(BulkContext context, Type type, IEnumerable<T> entities, TableInfo tableInfo, Action<decimal>? progress,
        CancellationToken cancellationToken)
    {
        await InsertAsync(context, entities, tableInfo, progress, isAsync: true, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc/>
    protected static async Task InsertAsync<T>(BulkContext context, IEnumerable<T> entities, TableInfo tableInfo, Action<decimal>? progress,
        bool isAsync, CancellationToken cancellationToken)
    {
        if (entities == null || !entities.Any())
        {
            return;
        }

        var dbContext = context.DbContext;
        var (connection, closeConnectionInternally) = await GetOrCreateConnection(context, isAsync, cancellationToken).ConfigureAwait(false);

        try
        {
            var operationType = tableInfo.InsertToTempTable ? OperationType.InsertOrUpdate : OperationType.Insert;
            var sqlCopy = GaussDBQueryBuilder.InsertIntoTable(tableInfo, operationType);

            using var writer = isAsync
                ? await connection.BeginBinaryImportAsync(sqlCopy, cancellationToken).ConfigureAwait(false)
                : connection.BeginBinaryImport(sqlCopy);

            var uniqueColumnName = tableInfo.PrimaryKeysPropertyColumnNameDict.Values.FirstOrDefault();
            var doKeepIdentity = tableInfo.BulkConfig.SqlBulkCopyOptions == SqlBulkCopyOptions.KeepIdentity;
            var propertiesColumnDict = ((tableInfo.InsertToTempTable || doKeepIdentity) && tableInfo.IdentityColumnName == uniqueColumnName)
                ? tableInfo.PropertyColumnNamesDict
                : tableInfo.PropertyColumnNamesDict.Where(a => a.Value != tableInfo.IdentityColumnName);
            var propertiesNames = propertiesColumnDict.Select(a => a.Key).ToList();
            var entitiesCopiedCount = 0;

            foreach (var entity in entities)
            {
                if (isAsync)
                {
                    await writer.StartRowAsync(cancellationToken).ConfigureAwait(false);
                }
                else
                {
                    writer.StartRow();
                }

                foreach (var propertyName in propertiesNames)
                {
                    if (operationType == OperationType.Insert
                        && tableInfo.DefaultValueProperties.Contains(propertyName)
                        && !tableInfo.PrimaryKeysPropertyColumnNameDict.ContainsKey(propertyName))
                    {
                        continue;
                    }

                    var propertyValue = GetPropertyValue(dbContext, tableInfo, propertyName, entity);
                    var propertyColumnName = tableInfo.PropertyColumnNamesDict.GetValueOrDefault(propertyName, "");
                    var columnType = tableInfo.OwnedJsonTypesDict.ContainsKey(propertyColumnName)
                        ? "jsonb"
                        : tableInfo.ColumnNamesTypesDict[propertyColumnName];

                    if (columnType.StartsWith("character"))
                    {
                        columnType = "character";
                    }
                    else if (columnType.StartsWith("varchar"))
                    {
                        columnType = "varchar";
                    }
                    else if (columnType.StartsWith("numeric") && columnType != "numeric[]")
                    {
                        columnType = "numeric";
                    }

                    if (columnType.StartsWith("timestamp("))
                    {
                        columnType = "timestamp" + columnType.Substring(12, columnType.Length - 12);
                    }

                    if (columnType.StartsWith("geometry"))
                    {
                        columnType = "geometry";
                    }
                    if (columnType.StartsWith("geography"))
                    {
                        columnType = "geography";
                    }

                    if (tableInfo.ConvertibleColumnConverterDict.TryGetValue(propertyColumnName, out var converter) && propertyValue != null)
                    {
                        if (converter.ModelClrType.IsEnum)
                        {
                            var clrType = converter.ProviderClrType;
                            if (clrType == typeof(byte))
                            {
                                propertyValue = (byte)propertyValue;
                            }
                            if (clrType == typeof(short))
                            {
                                propertyValue = (short)propertyValue;
                            }
                            if (clrType == typeof(int))
                            {
                                propertyValue = (int)propertyValue;
                            }
                            if (clrType == typeof(long))
                            {
                                propertyValue = (long)propertyValue;
                            }
                            if (clrType == typeof(string))
                            {
                                propertyValue = propertyValue.ToString();
                            }
                        }
                        else
                        {
                            try
                            {
                                propertyValue = converter.ConvertToProvider.Invoke(propertyValue);
                            }
                            catch (InvalidCastException ex)
                            {
                                if (!ex.Message.StartsWith("Invalid cast from 'System.String'"))
                                {
                                    throw;
                                }
                            }
                        }
                    }

                    if (isAsync)
                    {
                        await writer.WriteAsync(propertyValue, columnType, cancellationToken).ConfigureAwait(false);
                    }
                    else
                    {
                        writer.Write(propertyValue, columnType);
                    }
                }

                entitiesCopiedCount++;
                if (progress != null && tableInfo.BulkConfig.NotifyAfter != null
                    && tableInfo.BulkConfig.NotifyAfter != 0
                    && entitiesCopiedCount % tableInfo.BulkConfig.NotifyAfter == 0)
                {
                    progress.Invoke(ProgressHelper.GetProgress(entities.Count(), entitiesCopiedCount));
                }
            }

            if (isAsync)
            {
                await writer.CompleteAsync(cancellationToken).ConfigureAwait(false);
            }
            else
            {
                writer.Complete();
            }
        }
        finally
        {
            if (closeConnectionInternally)
            {
                if (isAsync)
                {
                    await connection.CloseAsync().ConfigureAwait(false);
                }
                else
                {
                    connection.Close();
                }
            }
        }
    }

    private static object? GetPropertyValue<T>(DbContext context, TableInfo tableInfo, string propertyName, T entity)
    {
        if (!tableInfo.FastPropertyDict.ContainsKey(propertyName.Replace('.', '_')) || entity is null)
        {
            object? propertyValue = null;
            var shadowPropertyColumnNamesDict = tableInfo.ColumnToPropertyDictionary
                .Where(a => a.Value.IsShadowProperty())
                .ToDictionary(a => a.Value.Name, a => a.Value.GetColumnName(tableInfo.ObjectIdentifier));

            if (shadowPropertyColumnNamesDict.ContainsKey(propertyName))
            {
                propertyValue = tableInfo.BulkConfig.ShadowPropertyValue == null
                    ? context.Entry(entity!).Property(propertyName).CurrentValue
                    : tableInfo.BulkConfig.ShadowPropertyValue(entity!, propertyName);

                if (tableInfo.ConvertibleColumnConverterDict.TryGetValue(propertyName, out var converter))
                {
                    propertyValue = converter.ConvertToProvider.Invoke(propertyValue);
                }

                return propertyValue;
            }

            return null;
        }

        object? propertyValueInner = entity;
        var fullPropertyName = string.Empty;
        foreach (var entry in propertyName.Split('.'))
        {
            if (propertyValueInner == null)
            {
                return null;
            }

            fullPropertyName = fullPropertyName.Length > 0 ? $"{fullPropertyName}_{entry}" : entry;
            propertyValueInner = tableInfo.FastPropertyDict[fullPropertyName].Get(propertyValueInner);
        }

        return propertyValueInner;
    }

    /// <inheritdoc/>
    public void Merge<T>(BulkContext context, Type type, IEnumerable<T> entities, TableInfo tableInfo, OperationType operationType,
        Action<decimal>? progress) where T : class
    {
        MergeAsync(context, type, entities, tableInfo, operationType, progress, isAsync: false, CancellationToken.None).GetAwaiter().GetResult();
    }

    /// <inheritdoc/>
    public async Task MergeAsync<T>(BulkContext context, Type type, IEnumerable<T> entities, TableInfo tableInfo, OperationType operationType,
        Action<decimal>? progress, CancellationToken cancellationToken) where T : class
    {
        await MergeAsync(context, type, entities, tableInfo, operationType, progress, isAsync: true, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc/>
    protected async Task MergeAsync<T>(BulkContext context, Type type, IEnumerable<T> entities, TableInfo tableInfo, OperationType operationType,
        Action<decimal>? progress, bool isAsync, CancellationToken cancellationToken) where T : class
    {
        var tempTableCreated = false;
        var outputTableCreated = false;
        var uniqueIndexCreated = false;
        var connectionOpenedInternally = false;
        var dbContext = context.DbContext;

        try
        {
            if (tableInfo.BulkConfig.CustomSourceTableName == null)
            {
                tableInfo.InsertToTempTable = true;
                var sqlCreateTableCopy = GaussDBQueryBuilder.CreateTableCopy(tableInfo.FullTableName, tableInfo.FullTempTableName,
                    tableInfo.BulkConfig.UseTempDB, tableInfo.BulkConfig.UseUnlogged);

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

            if (tableInfo.BulkConfig.CalculateStats)
            {
                var sqlCreateOutputTableCopy = GaussDBQueryBuilder.CreateOutputStatsTable(tableInfo.FullTempOutputTableName,
                    tableInfo.BulkConfig.UseTempDB, tableInfo.BulkConfig.UseUnlogged);

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

            var joinedEntityPk = string.Join("_", tableInfo.EntityPKPropertyColumnNameDict.Keys);
            var joinedPrimaryKeys = string.Join("_", tableInfo.PrimaryKeysPropertyColumnNameDict.Keys);
            var hasUniqueIndex = joinedEntityPk == joinedPrimaryKeys;

            if (!hasUniqueIndex)
            {
                (hasUniqueIndex, connectionOpenedInternally) = await CheckHasExplicitUniqueConstraintAsync(dbContext, tableInfo, isAsync, cancellationToken)
                    .ConfigureAwait(false);
            }

            if (!hasUniqueIndex)
            {
                var createUniqueIndex = GaussDBQueryBuilder.CreateUniqueIndex(tableInfo);
                if (isAsync)
                {
                    await dbContext.Database.ExecuteSqlRawAsync(createUniqueIndex, cancellationToken).ConfigureAwait(false);
                }
                else
                {
                    dbContext.Database.ExecuteSqlRaw(createUniqueIndex);
                }

                uniqueIndexCreated = true;
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

            var sqlMergeTable = GaussDBQueryBuilder.MergeTable<T>(tableInfo, operationType);
            if (operationType != OperationType.Read && (!tableInfo.BulkConfig.SetOutputIdentity || operationType == OperationType.Delete))
            {
                if (isAsync)
                {
                    await dbContext.Database.ExecuteSqlRawAsync(sqlMergeTable, cancellationToken).ConfigureAwait(false);
                }
                else
                {
                    dbContext.Database.ExecuteSqlRaw(sqlMergeTable);
                }
            }
            else
            {
                var sqlMergeTableOutput = sqlMergeTable.TrimEnd(';');
                var outputEntities = tableInfo.LoadOutputEntities<T>(dbContext, type, sqlMergeTableOutput);
                tableInfo.UpdateReadEntities(entities, outputEntities, dbContext);
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

            if (tableInfo.BulkConfig.CalculateStats)
            {
                var numberInserted = await GetStatsNumbersGaussDBAsync(dbContext, tableInfo, isAsync, cancellationToken).ConfigureAwait(false);
                tableInfo.BulkConfig.StatsInfo = new StatsInfo
                {
                    StatsNumberInserted = numberInserted,
                    StatsNumberUpdated = entities.Count() - numberInserted,
                };
            }
        }
        finally
        {
            try
            {
                if (uniqueIndexCreated)
                {
                    var dropUniqueIndex = GaussDBQueryBuilder.DropUniqueIndex(tableInfo);
                    if (isAsync)
                    {
                        await dbContext.Database.ExecuteSqlRawAsync(dropUniqueIndex, cancellationToken).ConfigureAwait(false);
                    }
                    else
                    {
                        dbContext.Database.ExecuteSqlRaw(dropUniqueIndex);
                    }
                }

                if (!tableInfo.BulkConfig.UseTempDB)
                {
                    if (outputTableCreated)
                    {
                        var sqlDropOutputTable = GaussDBQueryBuilder.DropTable(tableInfo.FullTempOutputTableName);
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
                        var sqlDropTable = GaussDBQueryBuilder.DropTable(tableInfo.FullTempTableName);
                        if (isAsync)
                        {
                            await dbContext.Database.ExecuteSqlRawAsync(sqlDropTable, cancellationToken).ConfigureAwait(false);
                        }
                        else
                        {
                            dbContext.Database.ExecuteSqlRaw(sqlDropTable);
                        }
                    }
                }
            }
            catch (PostgresException ex) when (ex.SqlState == "25P02")
            {
                // The transaction error generated during cleanup would conceal the original database error.
            }

            if (connectionOpenedInternally)
            {
                var connection = (GaussDBConnection)dbContext.Database.GetDbConnection();
                if (isAsync)
                {
                    await connection.CloseAsync().ConfigureAwait(false);
                }
                else
                {
                    connection.Close();
                }
            }
        }
    }

    /// <inheritdoc/>
    public void Read<T>(BulkContext context, Type type, IEnumerable<T> entities, TableInfo tableInfo, Action<decimal>? progress) where T : class
        => ReadAsync(context, type, entities, tableInfo, progress, isAsync: false, CancellationToken.None).GetAwaiter().GetResult();

    /// <inheritdoc/>
    public async Task ReadAsync<T>(BulkContext context, Type type, IEnumerable<T> entities, TableInfo tableInfo, Action<decimal>? progress,
        CancellationToken cancellationToken) where T : class
        => await ReadAsync(context, type, entities, tableInfo, progress, isAsync: true, cancellationToken).ConfigureAwait(false);

    /// <inheritdoc/>
    protected async Task ReadAsync<T>(BulkContext context, Type type, IEnumerable<T> entities, TableInfo tableInfo, Action<decimal>? progress,
        bool isAsync, CancellationToken cancellationToken) where T : class
        => await MergeAsync(context, type, entities, tableInfo, OperationType.Read, progress, isAsync, cancellationToken).ConfigureAwait(false);

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

    internal static async Task<(DbConnection, bool)> OpenAndGetGaussDBConnectionAsync(DbContext context, bool isAsync,
        CancellationToken cancellationToken)
    {
        var connectionOpenedInternally = false;
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

            connectionOpenedInternally = true;
        }

        return (connection, connectionOpenedInternally);
    }

    internal static async Task<(bool, bool)> CheckHasExplicitUniqueConstraintAsync(DbContext context, TableInfo tableInfo, bool isAsync,
        CancellationToken cancellationToken)
    {
        var countUniqueConstraint = GaussDBQueryBuilder.CountUniqueConstrain(tableInfo);
        var (connection, connectionOpenedInternally) = await OpenAndGetGaussDBConnectionAsync(context, isAsync, cancellationToken)
            .ConfigureAwait(false);
        var hasUniqueConstraint = false;

        using var command = connection.CreateCommand();
        command.CommandText = countUniqueConstraint;
        if (isAsync)
        {
            using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                hasUniqueConstraint = (long)reader[0] == 1;
            }
        }
        else
        {
            using var reader = command.ExecuteReader();
            while (reader.Read())
            {
                hasUniqueConstraint = (long)reader[0] == 1;
            }
        }

        return (hasUniqueConstraint, connectionOpenedInternally);
    }

    /// <summary>
    /// Gets the stats count of inserted entities.
    /// </summary>
    public static async Task<int> GetStatsNumbersGaussDBAsync(DbContext context, TableInfo tableInfo, bool isAsync,
        CancellationToken cancellationToken)
    {
        var sqlQuery = @$"SELECT COUNT(*) FROM {tableInfo.FullTempOutputTableName} WHERE ""xmaxNumber"" = 0;";
        sqlQuery = sqlQuery.Replace("[", @"""").Replace("]", @"""");

        var connection = (GaussDBConnection)context.Database.GetDbConnection();
        var isExternalTransaction = context.Database.CurrentTransaction != null;
        using var command = connection.CreateCommand();

        var dbTransaction = isExternalTransaction
            ? context.Database.CurrentTransaction?.GetUnderlyingTransaction(tableInfo.BulkConfig)
            : connection.BeginTransaction();
        var transaction = (GaussDBTransaction?)dbTransaction;
        command.CommandText = sqlQuery;

        var scalar = isAsync
            ? await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false)
            : command.ExecuteScalar();
        var counter = (long?)scalar ?? 0;

        if (!isExternalTransaction)
        {
            transaction?.Commit();
        }

        return (int)counter;
    }

    /// <inheritdoc/>
    public string? ReconfigureTableInfo(BulkContext context, TableInfo tableInfo)
    {
        var defaultSchema = "public";
        var csb = new GaussDBConnectionStringBuilder(context.DbContext.Database.GetConnectionString());
        if (!string.IsNullOrWhiteSpace(csb.SearchPath))
        {
            defaultSchema = csb.SearchPath.Split(',')[0];
        }

        return defaultSchema;
    }

    private static async Task<(GaussDBConnection connection, bool closeConnectionInternally)> GetOrCreateConnection(BulkContext context,
        bool isAsync, CancellationToken cancellationToken)
    {
        if (context.DbConnection is GaussDBConnection connection)
        {
            return (connection, false);
        }

        var (dbConnection, closeConnectionInternally) = await OpenAndGetGaussDBConnectionAsync(context.DbContext, isAsync, cancellationToken)
            .ConfigureAwait(false);
        return ((GaussDBConnection)dbConnection, closeConnectionInternally);
    }
}
