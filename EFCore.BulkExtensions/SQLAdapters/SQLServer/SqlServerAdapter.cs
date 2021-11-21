using EFCore.BulkExtensions.Helpers;
using EFCore.BulkExtensions.SqlAdapters;
using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;
using NetTopologySuite.Geometries;
using NetTopologySuite.IO;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;

namespace EFCore.BulkExtensions.SQLAdapters.SQLServer
{
    public class SqlOperationsServerAdapter: ISqlOperationsAdapter
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
        // Publish Async and NonAsync are merged into single operation flow with protected method using arg: bool isAsync (keeps code DRY)
        // https://docs.microsoft.com/en-us/archive/msdn-magazine/2015/july/async-programming-brownfield-async-development#the-flag-argument-hack
        protected async Task InsertAsync<T>(DbContext context, Type type, IList<T> entities, TableInfo tableInfo, Action<decimal> progress, CancellationToken cancellationToken, bool isAsync)
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
                tableInfo.SetSqlBulkCopyConfig(sqlBulkCopy, entities, setColumnMapping, progress);
                try
                {
                    var dataTable = GetDataTable(context, type, entities, sqlBulkCopy, tableInfo);
                    if (isAsync)
                    {
                        await sqlBulkCopy.WriteToServerAsync(dataTable, cancellationToken).ConfigureAwait(false);
                    }
                    else
                    {
                        sqlBulkCopy.WriteToServer(dataTable);
                    }
                }
                catch (InvalidOperationException ex)
                {
                    if (ex.Message.Contains(BulkExceptionMessage.ColumnMappingNotMatch))
                    {
                        bool tableExist = isAsync ? await tableInfo.CheckTableExistAsync(context, tableInfo, cancellationToken, isAsync: true).ConfigureAwait(false)
                                                        : tableInfo.CheckTableExistAsync(context, tableInfo, cancellationToken, isAsync: false).GetAwaiter().GetResult();

                        if (!tableExist)
                        {
                            var sqlCreateTableCopy = SqlQueryBuilder.CreateTableCopy(tableInfo.FullTableName, tableInfo.FullTempTableName, tableInfo);
                            var sqlDropTable = SqlQueryBuilder.DropTable(tableInfo.FullTempTableName, tableInfo.BulkConfig.UseTempDB);

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
            if (!tableInfo.CreatedOutputTable)
            {
                tableInfo.CheckToSetIdentityForPreserveOrder(tableInfo, entities, reset: true);
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
            tableInfo.InsertToTempTable = true;
            var entityPropertyWithDefaultValue = entities.GetPropertiesWithDefaultValue(type);

            var dropTempTableIfExists = tableInfo.BulkConfig.UseTempDB;

            if (dropTempTableIfExists)
            {
                var sqlDropTable = SqlQueryBuilder.DropTable(tableInfo.FullTempTableName, tableInfo.BulkConfig.UseTempDB);
                if (isAsync)
                {
                    await context.Database.ExecuteSqlRawAsync(sqlDropTable, cancellationToken).ConfigureAwait(false);
                }
                else
                {
                    context.Database.ExecuteSqlRaw(sqlDropTable);
                }
            }

            var sqlCreateTableCopy = SqlQueryBuilder.CreateTableCopy(tableInfo.FullTableName, tableInfo.FullTempTableName, tableInfo);
            if (isAsync)
            {
                await context.Database.ExecuteSqlRawAsync(sqlCreateTableCopy, cancellationToken).ConfigureAwait(false);
            }
            else
            {
                context.Database.ExecuteSqlRaw(sqlCreateTableCopy);
            }

            if (tableInfo.TimeStampColumnName != null)
            {
                var sqlAddColumn = SqlQueryBuilder.AddColumn(tableInfo.FullTempTableName, tableInfo.TimeStampColumnName, tableInfo.TimeStampOutColumnType);
                if (isAsync)
                {
                    await context.Database.ExecuteSqlRawAsync(sqlAddColumn, cancellationToken).ConfigureAwait(false);
                }
                else
                {
                    context.Database.ExecuteSqlRaw(sqlAddColumn);
                }
            }
            if (tableInfo.CreatedOutputTable)
            {
                var sqlCreateOutputTableCopy = SqlQueryBuilder.CreateTableCopy(tableInfo.FullTableName, tableInfo.FullTempOutputTableName, tableInfo, true);
                if (isAsync)
                {
                    await context.Database.ExecuteSqlRawAsync(sqlCreateOutputTableCopy, cancellationToken).ConfigureAwait(false);
                }
                else
                {
                    context.Database.ExecuteSqlRaw(sqlCreateOutputTableCopy);
                }

                if (tableInfo.TimeStampColumnName != null)
                {
                    var sqlAddColumn = SqlQueryBuilder.AddColumn(tableInfo.FullTempOutputTableName, tableInfo.TimeStampColumnName, tableInfo.TimeStampOutColumnType);
                    if (isAsync)
                    {
                        await context.Database.ExecuteSqlRawAsync(sqlAddColumn, cancellationToken).ConfigureAwait(false);
                    }
                    else
                    {
                        context.Database.ExecuteSqlRaw(sqlAddColumn);
                    }
                }

                if (operationType == OperationType.InsertOrUpdateDelete)
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

            bool keepIdentity = tableInfo.BulkConfig.SqlBulkCopyOptions.HasFlag(SqlBulkCopyOptions.KeepIdentity);
            try
            {
                if (isAsync)
                {
                    await InsertAsync(context, type, entities, tableInfo, progress, cancellationToken).ConfigureAwait(false);
                }
                else
                {
                    Insert(context, type, entities, tableInfo, progress);
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
                }

                var sqlMergeTable = SqlQueryBuilder.MergeTable<T>(context, tableInfo, operationType, entityPropertyWithDefaultValue);
                if (isAsync)
                {
                    await context.Database.ExecuteSqlRawAsync(sqlMergeTable.sql, sqlMergeTable.parameters, cancellationToken).ConfigureAwait(false);
                }
                else
                {
                    context.Database.ExecuteSqlRaw(sqlMergeTable.sql, sqlMergeTable.parameters);
                }

                if (tableInfo.CreatedOutputTable)
                {
                    if (isAsync)
                    {
                        await tableInfo.LoadOutputDataAsync(context, type, entities, tableInfo, cancellationToken, isAsync: true).ConfigureAwait(false);
                    }
                    else
                    {
                        tableInfo.LoadOutputDataAsync(context, type, entities, tableInfo, cancellationToken, isAsync: false).GetAwaiter().GetResult();
                    }
                }
            }
            finally
            {
                if (!tableInfo.BulkConfig.UseTempDB)
                {
                    if (tableInfo.CreatedOutputTable)
                    {
                        var sqlDropOutputTable = SqlQueryBuilder.DropTable(tableInfo.FullTempOutputTableName, tableInfo.BulkConfig.UseTempDB);
                        if (isAsync)
                        {
                            await context.Database.ExecuteSqlRawAsync(sqlDropOutputTable, cancellationToken).ConfigureAwait(false);
                        }
                        else
                        {
                            context.Database.ExecuteSqlRaw(sqlDropOutputTable);
                        }

                    }
                    var sqlDropTable = SqlQueryBuilder.DropTable(tableInfo.FullTempTableName, tableInfo.BulkConfig.UseTempDB);
                    if (isAsync)
                    {
                        await context.Database.ExecuteSqlRawAsync(sqlDropTable, cancellationToken).ConfigureAwait(false);
                    }
                    else
                    {
                        context.Database.ExecuteSqlRaw(sqlDropTable);
                    }
                }

                if (keepIdentity && tableInfo.HasIdentity)
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
            Dictionary<string, string> previousPropertyColumnNamesDict = tableInfo.ConfigureBulkReadTableInfo();

            var sqlCreateTableCopy = SqlQueryBuilder.CreateTableCopy(tableInfo.FullTableName, tableInfo.FullTempTableName, tableInfo);
            if (isAsync)
            {
                await context.Database.ExecuteSqlRawAsync(sqlCreateTableCopy, cancellationToken).ConfigureAwait(false);
            }
            else
            {
                context.Database.ExecuteSqlRaw(sqlCreateTableCopy);
            }

            try
            {
                if (isAsync)
                {
                    await InsertAsync(context, type, entities, tableInfo, progress, cancellationToken).ConfigureAwait(false);
                }
                else
                {
                    InsertAsync(context, type, entities, tableInfo, progress, cancellationToken, isAsync: false).GetAwaiter().GetResult();
                }

                tableInfo.PropertyColumnNamesDict = tableInfo.OutputPropertyColumnNamesDict;

                var sqlSelectJoinTable = SqlQueryBuilder.SelectJoinTable(tableInfo);

                tableInfo.PropertyColumnNamesDict = previousPropertyColumnNamesDict; // TODO Consider refactor and integrate with TimeStampPropertyName, also check for Calculated props.
                                                                                     // Output only PropertisToInclude and for getting Id with SetOutputIdentity
                if (tableInfo.TimeStampPropertyName != null && !tableInfo.PropertyColumnNamesDict.ContainsKey(tableInfo.TimeStampPropertyName))
                {
                    tableInfo.PropertyColumnNamesDict.Add(tableInfo.TimeStampPropertyName, tableInfo.TimeStampColumnName);
                }

                List<T> existingEntities;
                if (typeof(T) == type)
                {
                    Expression<Func<DbContext, IQueryable<T>>> expression = tableInfo.GetQueryExpression<T>(sqlSelectJoinTable, false);
                    var compiled = EF.CompileQuery(expression); // instead using Compiled queries
                    existingEntities = compiled(context).ToList();
                }
                else // TODO: Consider removing
                {
                    Expression<Func<DbContext, IEnumerable>> expression = tableInfo.GetQueryExpression(type, sqlSelectJoinTable, false);
                    var compiled = EF.CompileQuery(expression); // instead using Compiled queries
                    existingEntities = compiled(context).Cast<T>().ToList();
                }

                tableInfo.UpdateReadEntities(type, entities, existingEntities);

                if (tableInfo.TimeStampPropertyName != null && !tableInfo.PropertyColumnNamesDict.ContainsKey(tableInfo.TimeStampPropertyName))
                {
                    tableInfo.PropertyColumnNamesDict.Remove(tableInfo.TimeStampPropertyName);
                }
            }
            finally
            {
                if (!tableInfo.BulkConfig.UseTempDB)
                {
                    var sqlDropTable = SqlQueryBuilder.DropTable(tableInfo.FullTempTableName, tableInfo.BulkConfig.UseTempDB);
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

        // Truncate
        public void Truncate(DbContext context, TableInfo tableInfo)
        {
            var sqlTruncateTable = SqlQueryBuilder.TruncateTable(tableInfo.FullTableName);
            context.Database.ExecuteSqlRaw(sqlTruncateTable);
        }

        public async Task TruncateAsync(DbContext context, TableInfo tableInfo, CancellationToken cancellationToken)
        {
            var sqlTruncateTable = SqlQueryBuilder.TruncateTable(tableInfo.FullTableName);
            await context.Database.ExecuteSqlRawAsync(sqlTruncateTable, cancellationToken).ConfigureAwait(false);
        }
        #endregion

        #region Connection
        private static SqlBulkCopy GetSqlBulkCopy(SqlConnection sqlConnection, IDbContextTransaction transaction, BulkConfig config)
        {
            var sqlTransaction = transaction == null ? null : (SqlTransaction)transaction.GetUnderlyingTransaction(config);
            var sqlBulkCopy = new SqlBulkCopy(sqlConnection, config.SqlBulkCopyOptions, sqlTransaction);
            return sqlBulkCopy;
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
        internal static DataTable GetDataTable<T>(DbContext context, Type type, IList<T> entities, SqlBulkCopy sqlBulkCopy, TableInfo tableInfo)
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
        private static DataTable InnerGetDataTable<T>(DbContext context, ref Type type, IList<T> entities, TableInfo tableInfo)
        {
            var dataTable = new DataTable();
            var columnsDict = new Dictionary<string, object>();
            var ownedEntitiesMappedProperties = new HashSet<string>();

            var isSqlServer = context.Database.ProviderName.EndsWith(DbServer.SqlServer.ToString());
            var sqlServerBytesWriter = new SqlServerBytesWriter();

            var objectIdentifier = tableInfo.ObjectIdentifier;
            type = tableInfo.HasAbstractList ? entities[0].GetType() : type;
            var entityType = context.Model.FindEntityType(type);
            var entityTypeProperties = entityType.GetProperties();
            var entityPropertiesDict = entityTypeProperties.Where(a => tableInfo.PropertyColumnNamesDict.ContainsKey(a.Name) ||
                                                                       (tableInfo.BulkConfig.OperationType != OperationType.Read && a.Name == tableInfo.TimeStampPropertyName))
                                                           .ToDictionary(a => a.Name, a => a);
            var entityNavigationOwnedDict = entityType.GetNavigations().Where(a => a.TargetEntityType.IsOwned()).ToDictionary(a => a.Name, a => a);
            var entityShadowFkPropertiesDict = entityTypeProperties.Where(a => a.IsShadowProperty() &&
                                                                               a.IsForeignKey() &&
                                                                               a.GetContainingForeignKeys().FirstOrDefault()?.DependentToPrincipal?.Name != null)
                                                                         .ToDictionary(x => x.GetContainingForeignKeys().First().DependentToPrincipal.Name, a => a);
            var entityShadowFkPropertyColumnNamesDict = entityShadowFkPropertiesDict.ToDictionary(a => a.Key, a => a.Value.GetColumnName(objectIdentifier));
            var shadowPropertyColumnNamesDict = entityPropertiesDict.Where(a => a.Value.IsShadowProperty()).ToDictionary(a => a.Key, a => a.Value.GetColumnName(objectIdentifier));

            var properties = type.GetProperties(BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic);
            var discriminatorColumn = GetDiscriminatorColumn(tableInfo);

            foreach (var property in properties)
            {
                var hasDefaultVauleOnInsert = tableInfo.BulkConfig.OperationType == OperationType.Insert && !tableInfo.BulkConfig.SetOutputIdentity && tableInfo.DefaultValueProperties.Contains(property.Name);

                if (entityPropertiesDict.ContainsKey(property.Name))
                {
                    var propertyEntityType = entityPropertiesDict[property.Name];
                    string columnName = propertyEntityType.GetColumnName(objectIdentifier);

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

                    if (!columnsDict.ContainsKey(property.Name) && !hasDefaultVauleOnInsert)
                    {
                        dataTable.Columns.Add(columnName, propertyType);
                        columnsDict.Add(property.Name, null);
                    }
                }
                else if (entityShadowFkPropertiesDict.ContainsKey(property.Name))
                {
                    var fk = entityShadowFkPropertiesDict[property.Name];

                    entityPropertiesDict.TryGetValue(fk.GetColumnName(objectIdentifier), out var entityProperty);
                    if (entityProperty == null) // BulkRead
                        continue;

                    var columnName = entityProperty.GetColumnName(objectIdentifier);
                    var propertyType = entityProperty.ClrType;
                    var underlyingType = Nullable.GetUnderlyingType(propertyType);
                    if (underlyingType != null)
                    {
                        propertyType = underlyingType;
                    }

                    if (propertyType == typeof(Geometry) && isSqlServer)
                    {
                        propertyType = typeof(byte[]);
                    }

                    if (!columnsDict.ContainsKey(columnName) && !hasDefaultVauleOnInsert)
                    {
                        dataTable.Columns.Add(columnName, propertyType);
                        columnsDict.Add(columnName, null);
                    }
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
                            string columnName = ownedEntityProperty.GetColumnName(objectIdentifier);
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
                    var columnName = shadowProperty.GetColumnName(objectIdentifier);

                    // If a model has an entity which has a relationship without an explicity defined FK, the data table will already contain the foreign key shadow property
                    if (dataTable.Columns.Contains(columnName))
                        continue;

                    var isConvertible = tableInfo.ConvertibleColumnConverterDict.ContainsKey(columnName);
                    var propertyType = isConvertible ? tableInfo.ConvertibleColumnConverterDict[columnName].ProviderClrType : shadowProperty.ClrType;

                    var underlyingType = Nullable.GetUnderlyingType(propertyType);
                    if (underlyingType != null)
                    {
                        propertyType = underlyingType;
                    }

                    if (isSqlServer && (propertyType == typeof(Geometry) || propertyType.IsSubclassOf(typeof(Geometry))))
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

            foreach (var entity in entities)
            {
                var propertiesToLoad = properties.Where(a => !tableInfo.AllNavigationsDictionary.ContainsKey(a.Name) || entityShadowFkPropertiesDict.ContainsKey(a.Name) || tableInfo.OwnedTypesDict.ContainsKey(a.Name)); // omit virtual Navigation (except Owned and ShadowNavig.) since it's Getter can cause unwanted Select-s from Db
                foreach (var property in propertiesToLoad)
                {
                    var propertyValue = tableInfo.FastPropertyDict.ContainsKey(property.Name) ? tableInfo.FastPropertyDict[property.Name].Get(entity) : null;
                    var hasDefaultVauleOnInsert = tableInfo.BulkConfig.OperationType == OperationType.Insert && !tableInfo.BulkConfig.SetOutputIdentity && tableInfo.DefaultValueProperties.Contains(property.Name);

                    if (tableInfo.BulkConfig.DateTime2PrecisionForceRound && isSqlServer && tableInfo.DateTime2PropertiesPrecisionLessThen7Dict.ContainsKey(property.Name))
                    {
                        DateTime dateTimePropertyValue = (DateTime)propertyValue;

                        int precision = tableInfo.DateTime2PropertiesPrecisionLessThen7Dict[property.Name];
                        int digitsToRemove = 7 - precision;
                        int powerOf10 = (int)Math.Pow(10, digitsToRemove);

                        long subsecondTicks = dateTimePropertyValue.Ticks % 10000000;
                        long ticksToRound = subsecondTicks + (subsecondTicks % 10 == 0 ? 1 : 0); // if ends with 0 add 1 tick to make sure rounding of value .5_zeros is rounded to Upper like SqlServer is doing, not to Even as Math.Round works
                        int roundedTicks = Convert.ToInt32(Math.Round((decimal)ticksToRound / powerOf10, 0)) * powerOf10;
                        dateTimePropertyValue = dateTimePropertyValue.AddTicks(-subsecondTicks).AddTicks(roundedTicks);

                        propertyValue = dateTimePropertyValue;
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

                    if (entityPropertiesDict.ContainsKey(property.Name) && !hasDefaultVauleOnInsert)
                    {
                        columnsDict[property.Name] = propertyValue;
                    }
                    else if (entityShadowFkPropertiesDict.ContainsKey(property.Name))
                    {
                        var foreignKeyShadowProperty = entityShadowFkPropertiesDict[property.Name];
                        var columnName = entityShadowFkPropertyColumnNamesDict[property.Name];
                        entityPropertiesDict.TryGetValue(columnName, out var entityProperty);
                        if (entityProperty == null) // BulkRead
                            continue;

                        columnsDict[columnName] = propertyValue == null ? null : foreignKeyShadowProperty.FindFirstPrincipal().PropertyInfo.GetValue(propertyValue); // TODO Check if can be optimized
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
                        var columnName = shadowPropertyColumnNamesDict[shadowPropertyName];
                        var propertyValue = context.Entry(entity).Property(shadowPropertyName).CurrentValue;

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

        private static string GetDiscriminatorColumn(TableInfo tableInfo)
        {
            string discriminatorColumn = null;
            if (!tableInfo.BulkConfig.EnableShadowProperties && tableInfo.ShadowProperties.Count > 0)
            {
                var stringColumns = tableInfo.ColumnNamesTypesDict.Where(a => a.Value.Contains("char")).Select(a => a.Key).ToList();
                discriminatorColumn = tableInfo.ShadowProperties.Where(a => stringColumns.Contains(a)).ElementAt(0);
            }
            return discriminatorColumn;
        }
       #endregion
    }
}
