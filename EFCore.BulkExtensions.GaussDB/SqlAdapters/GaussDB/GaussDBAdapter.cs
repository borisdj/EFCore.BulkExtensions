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
        if (entities is null)
        {
            return;
        }

        // Bulk operations enumerate the input while writing rows and may also need its count for
        // progress reporting. Materialize one-shot enumerables once so a generator is not consumed
        // by Any/Count before the binary importer sees it.
        var entityList = entities as IReadOnlyCollection<T> ?? entities.ToList();
        if (entityList.Count == 0)
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

            // Use exactly the COPY column order. Resolve SQL types, converters and
            // property paths once per batch, rather than once per row and column.
            var propertiesByColumn = tableInfo.PropertyColumnNamesDict.ToDictionary(p => p.Value, p => p.Key);
            var columns = GaussDBQueryBuilder.GetColumnList(tableInfo, operationType).Select(column =>
            {
                var getValue = CreateValueGetter<T>(dbContext, tableInfo, propertiesByColumn[column], column);
                tableInfo.ConvertibleColumnConverterDict.TryGetValue(column, out var converter);
                var columnType = tableInfo.OwnedJsonTypesDict.ContainsKey(column)
                    ? "jsonb" : tableInfo.ColumnNamesTypesDict[column];
                // Remove length/precision modifiers while retaining array suffixes and time zones.
                int modifier = columnType.IndexOf('(');
                int endModifier = columnType.IndexOf(')');
                if (modifier >= 0 && endModifier > modifier)
                    columnType = columnType.Remove(modifier, endModifier - modifier + 1);
                return (GetValue: getValue, ColumnType: columnType, Convert: converter?.ConvertToProvider);
            }).ToArray();
            var entitiesCopiedCount = 0;

            foreach (var entity in entityList)
            {
                if (isAsync)
                {
                    await writer.StartRowAsync(cancellationToken).ConfigureAwait(false);
                }
                else
                {
                    writer.StartRow();
                }

                foreach (var column in columns)
                {
                    var propertyValue = column.GetValue(entity);
                    if (propertyValue != null && column.Convert != null)
                        propertyValue = column.Convert(propertyValue);
                    var columnType = column.ColumnType;

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
                    progress.Invoke(ProgressHelper.GetProgress(entityList.Count, entitiesCopiedCount));
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
            if (!tableInfo.InsertToTempTable && tableInfo.BulkConfig.CalculateStats)
                tableInfo.BulkConfig.StatsInfo = new StatsInfo { StatsNumberInserted = entityList.Count };
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

    private static Func<T, object?> CreateValueGetter<T>(DbContext context, TableInfo tableInfo, string propertyName, string columnName)
    {
        if (tableInfo.ColumnToPropertyDictionary.TryGetValue(columnName, out var property) && property.IsShadowProperty())
        {
            return entity => tableInfo.BulkConfig.ShadowPropertyValue == null
                ? context.Entry(entity!).Property(propertyName).CurrentValue
                : tableInfo.BulkConfig.ShadowPropertyValue(entity!, propertyName);
        }

        var path = new List<FastProperty>();
        var fullName = string.Empty;
        foreach (var segment in propertyName.Split('.'))
        {
            fullName = fullName.Length == 0 ? segment : fullName + "_" + segment;
            if (!tableInfo.FastPropertyDict.TryGetValue(fullName, out var accessor))
                return _ => null;
            path.Add(accessor);
        }
        var accessors = path.ToArray();
        return entity =>
        {
            object? value = entity;
            foreach (var accessor in accessors)
            {
                if (value == null) return null;
                value = accessor.Get(value);
            }
            return value;
        };
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
        var entityList = entities as IReadOnlyCollection<T> ?? entities.ToList();
        var tempTableCreated = false;
        var outputTableCreated = false;
        var uniqueIndexCreated = false;
        var connectionOpenedInternally = false;
        var dbContext = context.DbContext;

        try
        {
            // Keep one session across staging, COPY, DML and output loading.
            // This also prevents repeated pool checkout/authentication for every command.
            (_, connectionOpenedInternally) = await OpenAndGetGaussDBConnectionAsync(dbContext, isAsync, cancellationToken).ConfigureAwait(false);
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

            var collectInsertStats = tableInfo.BulkConfig.CalculateStats &&
                operationType is OperationType.Insert or OperationType.InsertOrUpdate;
            if (collectInsertStats)
            {
                var sqlCreateOutputTableCopy = GaussDBQueryBuilder.CreateOutputStatsTable(GaussDBQueryBuilder.GetStatsTableName(tableInfo),
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

            if (operationType == OperationType.InsertOrUpdate && !hasUniqueIndex)
            {
                (hasUniqueIndex, _) = await CheckHasExplicitUniqueConstraintAsync(dbContext, tableInfo, isAsync, cancellationToken).ConfigureAwait(false);
            }

            if (operationType == OperationType.InsertOrUpdate && !hasUniqueIndex)
            {
                var createUniqueIndex = GaussDBQueryBuilder.CreateUniqueIndex(tableInfo,
                    concurrently: dbContext.Database.CurrentTransaction == null);
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
                    await InsertAsync(context, type, entityList, tableInfo, progress, cancellationToken).ConfigureAwait(false);
                }
                else
                {
                    Insert(context, type, entityList, tableInfo, progress);
                }
            }

            var sqlMergeTable = GaussDBQueryBuilder.MergeTable<T>(tableInfo, operationType);
            int affectedCount;
            if (operationType != OperationType.Read && (!tableInfo.BulkConfig.SetOutputIdentity || operationType == OperationType.Delete))
            {
                if (isAsync)
                {
                    affectedCount = await dbContext.Database.ExecuteSqlRawAsync(sqlMergeTable, cancellationToken).ConfigureAwait(false);
                }
                else
                {
                    affectedCount = dbContext.Database.ExecuteSqlRaw(sqlMergeTable);
                }
            }
            else
            {
                var sqlMergeTableOutput = sqlMergeTable.TrimEnd(';');
                var outputEntities = isAsync
                    ? await tableInfo.LoadOutputEntitiesAsync<T>(dbContext, type, sqlMergeTableOutput, cancellationToken).ConfigureAwait(false)
                    : tableInfo.LoadOutputEntities<T>(dbContext, type, sqlMergeTableOutput);
                affectedCount = outputEntities.Count;
                if (operationType == OperationType.Read && tableInfo.BulkConfig.ReplaceReadEntities)
                {
                    if (entities is not List<T> list)
                        throw new NotSupportedException("ReplaceReadEntities requires a List<T>.");
                    list.Clear();
                    list.AddRange(outputEntities);
                }
                else
                {
                    tableInfo.UpdateReadEntities(entityList, outputEntities, dbContext);
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

            if (tableInfo.BulkConfig.CalculateStats)
            {
                var numberInserted = 0;
                if (collectInsertStats)
                    (numberInserted, affectedCount) = await ReadStatisticsAsync(dbContext, tableInfo, isAsync, cancellationToken).ConfigureAwait(false);
                tableInfo.BulkConfig.StatsInfo = new StatsInfo
                {
                    StatsNumberInserted = numberInserted,
                    StatsNumberUpdated = operationType == OperationType.Delete ? 0 : affectedCount - numberInserted,
                    StatsNumberDeleted = operationType == OperationType.Delete ? affectedCount : 0,
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
                        await dbContext.Database.ExecuteSqlRawAsync(dropUniqueIndex, CancellationToken.None).ConfigureAwait(false);
                    }
                    else
                    {
                        dbContext.Database.ExecuteSqlRaw(dropUniqueIndex);
                    }
                }

                // Drop session-local tables too: a caller can reuse the same connection/transaction.
                {
                    if (outputTableCreated)
                    {
                        var sqlDropOutputTable = GaussDBQueryBuilder.DropTable(GaussDBQueryBuilder.GetStatsTableName(tableInfo));
                        if (isAsync)
                        {
                            await dbContext.Database.ExecuteSqlRawAsync(sqlDropOutputTable, CancellationToken.None).ConfigureAwait(false);
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
                            await dbContext.Database.ExecuteSqlRawAsync(sqlDropTable, CancellationToken.None).ConfigureAwait(false);
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

            finally
            {
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
        var countUniqueConstraint = GaussDBQueryBuilder.CountUniqueIndex(tableInfo);
        var (connection, connectionOpenedInternally) = await OpenAndGetGaussDBConnectionAsync(context, isAsync, cancellationToken)
            .ConfigureAwait(false);
        var hasUniqueConstraint = false;

        using var command = connection.CreateCommand();
        command.CommandText = countUniqueConstraint;
        command.Transaction = context.Database.CurrentTransaction?.GetUnderlyingTransaction(tableInfo.BulkConfig);
        if (isAsync)
        {
            using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                hasUniqueConstraint |= Convert.ToInt64(reader[0]) > 0;
            }
        }
        else
        {
            using var reader = command.ExecuteReader();
            while (reader.Read())
            {
                hasUniqueConstraint |= Convert.ToInt64(reader[0]) > 0;
            }
        }

        return (hasUniqueConstraint, connectionOpenedInternally);
    }

    /// <summary>
    /// Gets the stats count of inserted entities.
    /// </summary>
    public static async Task<int> GetStatsNumbersGaussDBAsync(DbContext context, TableInfo tableInfo, bool isAsync,
        CancellationToken cancellationToken)
        => (await ReadStatisticsAsync(context, tableInfo, isAsync, cancellationToken).ConfigureAwait(false)).Inserted;

    private static async Task<(int Inserted, int Total)> ReadStatisticsAsync(DbContext context, TableInfo tableInfo,
        bool isAsync, CancellationToken cancellationToken)
    {
        var (connection, openedInternally) = await OpenAndGetGaussDBConnectionAsync(context, isAsync, cancellationToken).ConfigureAwait(false);
        try
        {
            using var command = connection.CreateCommand();
            command.Transaction = context.Database.CurrentTransaction?.GetUnderlyingTransaction(tableInfo.BulkConfig);
            command.CommandText = $"SELECT COUNT(CASE WHEN \"xmaxNumber\" = 0 THEN 1 END), COUNT(*) FROM " +
                GaussDBQueryBuilder.GetStatsTableName(tableInfo).Replace('[', '"').Replace(']', '"');
            using var reader = isAsync
                ? await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false)
                : command.ExecuteReader();
            if (isAsync) await reader.ReadAsync(cancellationToken).ConfigureAwait(false);
            else reader.Read();
            return (Convert.ToInt32(reader[0]), Convert.ToInt32(reader[1]));
        }
        finally
        {
            if (openedInternally)
            {
                if (isAsync) await connection.CloseAsync().ConfigureAwait(false);
                else connection.Close();
            }
        }
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
