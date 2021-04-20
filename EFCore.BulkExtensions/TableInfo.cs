using EFCore.BulkExtensions.SqlAdapters;
using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.Storage.ValueConversion;
using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Data;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace EFCore.BulkExtensions
{
    public class TableInfo
    {
        public string Schema { get; set; }
        public string SchemaFormated => Schema != null ? $"[{Schema}]." : "";
        public string TableName { get; set; }
        public string FullTableName => $"{SchemaFormated}[{TableName}]";
        public Dictionary<string, string> PrimaryKeysPropertyColumnNameDict { get; set; }
        public bool HasSinglePrimaryKey { get; set; }
        public bool UpdateByPropertiesAreNullable { get; set; }

        protected string TempDBPrefix => BulkConfig.UseTempDB ? "#" : "";
        public string TempTableSufix { get; set; }
        public string TempTableName => $"{TableName}{TempTableSufix}";
        public string FullTempTableName => $"{SchemaFormated}[{TempDBPrefix}{TempTableName}]";
        public string FullTempOutputTableName => $"{SchemaFormated}[{TempDBPrefix}{TempTableName}Output]";

        public bool CreatedOutputTable => BulkConfig.SetOutputIdentity || BulkConfig.CalculateStats;

        public bool InsertToTempTable { get; set; }
        public string IdentityColumnName { get; set; }
        public bool HasIdentity => IdentityColumnName != null;
        public bool HasOwnedTypes { get; set; }
        public bool HasAbstractList { get; set; }
        public bool ColumnNameContainsSquareBracket { get; set; }
        public bool LoadOnlyPKColumn { get; set; }
        public bool HasSpatialType { get; set; }
        public int NumberOfEntities { get; set; }

        public BulkConfig BulkConfig { get; set; }
        public Dictionary<string, string> OutputPropertyColumnNamesDict { get; set; } = new Dictionary<string, string>();
        public Dictionary<string, string> PropertyColumnNamesDict { get; set; } = new Dictionary<string, string>();
        public Dictionary<string, string> ColumnNamesTypesDict { get; set; } = new Dictionary<string, string>();
        public Dictionary<string, string> PropertyColumnNamesCompareDict { get; set; } = new Dictionary<string, string>();
        public Dictionary<string, string> PropertyColumnNamesUpdateDict { get; set; } = new Dictionary<string, string>();
        public Dictionary<string, FastProperty> FastPropertyDict { get; set; } = new Dictionary<string, FastProperty>();
        public Dictionary<string, INavigation> AllNavigationsDictionary { get; private set; }
        public Dictionary<string, INavigation> OwnedTypesDict { get; set; } = new Dictionary<string, INavigation>();
        public HashSet<string> ShadowProperties { get; set; } = new HashSet<string>();
        public HashSet<string> DefaultValueProperties { get; set; } = new HashSet<string>();

        public Dictionary<string, string> ConvertiblePropertyColumnDict { get; set; } = new Dictionary<string, string>();
        public Dictionary<string, ValueConverter> ConvertibleColumnConverterDict { get; set; } = new Dictionary<string, ValueConverter>();
        public Dictionary<string, int> DateTime2PropertiesPrecisionLessThen7Dict { get; set; } = new Dictionary<string, int>();

        public string TimeStampOutColumnType => "varbinary(8)";
        public string TimeStampPropertyName { get; set; }
        public string TimeStampColumnName { get; set; }

        public StoreObjectIdentifier ObjectIdentifier { get; set; }

        internal SqliteConnection SqliteConnection { get; set; }
        internal SqliteTransaction SqliteTransaction { get; set; }

        public static TableInfo CreateInstance<T>(DbContext context, Type type, IList<T> entities, OperationType operationType, BulkConfig bulkConfig)
        {
            var tableInfo = new TableInfo
            {
                NumberOfEntities = entities.Count,
                BulkConfig = bulkConfig ?? new BulkConfig() { }
            };
            tableInfo.BulkConfig.OperationType = operationType;

            bool isExplicitTransaction = context.Database.GetDbConnection().State == ConnectionState.Open;
            if (tableInfo.BulkConfig.UseTempDB == true && !isExplicitTransaction && (operationType != OperationType.Insert || tableInfo.BulkConfig.SetOutputIdentity))
            {
                throw new InvalidOperationException("When 'UseTempDB' is set then BulkOperation has to be inside Transaction. " +
                                                    "Otherwise destination table gets dropped too early because transaction ends before operation is finished."); // throws: 'Cannot access destination table'
            }

            var isDeleteOperation = operationType == OperationType.Delete;
            tableInfo.LoadData(context, type, entities, isDeleteOperation);
            return tableInfo;
        }

        #region Main
        public void LoadData<T>(DbContext context, Type type, IList<T> entities, bool loadOnlyPKColumn)
        {
            LoadOnlyPKColumn = loadOnlyPKColumn;
            var entityType = context.Model.FindEntityType(type);
            if (entityType == null)
            {
                type = entities[0].GetType();
                entityType = context.Model.FindEntityType(type);
                HasAbstractList = true;
            }
            if (entityType == null)
            {
                throw new InvalidOperationException($"DbContext does not contain EntitySet for Type: { type.Name }");
            }

            //var relationalData = entityType.Relational(); relationalData.Schema relationalData.TableName // DEPRECATED in Core3.0
            bool isSqlServer = context.Database.ProviderName.EndsWith(DbServer.SqlServer.ToString());
            string defaultSchema = isSqlServer ? "dbo" : null;

            string customSchema = null;
            string customTableName = null;
            if (BulkConfig.CustomDestinationTableName != null)
            {
                customTableName = BulkConfig.CustomDestinationTableName;
                if (customTableName.Contains('.'))
                {
                    var tableNameSplitList = BulkConfig.CustomDestinationTableName.Split('.');
                    customSchema = tableNameSplitList[0];
                    customTableName = tableNameSplitList[1];
                }
            }
            Schema = customSchema ?? entityType.GetSchema() ?? defaultSchema;
            TableName = customTableName ?? entityType.GetTableName();
            ObjectIdentifier = StoreObjectIdentifier.Table(TableName, entityType.GetSchema());

            TempTableSufix = "Temp";

            if (!BulkConfig.UseTempDB || BulkConfig.UniqueTableNameTempDb)
            {
                TempTableSufix += Guid.NewGuid().ToString().Substring(0, 8); // 8 chars of Guid as tableNameSufix to avoid same name collision with other tables
            }

            var allProperties = entityType.GetProperties().Where(a => a.GetColumnName(ObjectIdentifier) != null);
            ColumnNamesTypesDict = allProperties.ToDictionary(a => a.GetColumnName(ObjectIdentifier), a => a.GetColumnType());

            bool AreSpecifiedUpdateByProperties = BulkConfig.UpdateByProperties?.Count() > 0;
            var primaryKeys = entityType.FindPrimaryKey()?.Properties?.ToDictionary(a => a.Name, b => b.GetColumnName(ObjectIdentifier));
            HasSinglePrimaryKey = primaryKeys?.Count == 1;
            PrimaryKeysPropertyColumnNameDict = AreSpecifiedUpdateByProperties ? BulkConfig.UpdateByProperties.ToDictionary(a => a, b => allProperties.First(p => p.Name == b).GetColumnName(ObjectIdentifier))
                                                                               : (primaryKeys ?? new Dictionary<string, string>());

            if (BulkConfig.DateTime2PrecisionForceRound)
            {
                var propertyMappings = allProperties.Select(a => a.GetTableColumnMappings().ToList());
                foreach (var propertyMap in propertyMappings)
                {
                    IColumn column = propertyMap.FirstOrDefault().Column;
                    var columnType = column.StoreType;
                    if (columnType.StartsWith("datetime2(") && !columnType.EndsWith("7)"))
                    {
                        string precisionText = columnType.Substring(10, 1);
                        int precision = int.Parse(precisionText);
                        DateTime2PropertiesPrecisionLessThen7Dict.Add(propertyMap.FirstOrDefault().Property.Name, precision); // SqlBulkCopy does Floor instead of Round so Rounding done in memory
                    }
                }
            }

            // load all derived type properties
            if (entityType.IsAbstract())
            {
                var extendedAllProperties = allProperties.ToList();
                foreach (var derived in entityType.GetDirectlyDerivedTypes())
                {
                    extendedAllProperties.AddRange(derived.GetProperties());
                }

                allProperties = extendedAllProperties.Distinct();
            }

            var navigations = entityType.GetNavigations();
            AllNavigationsDictionary = navigations.ToDictionary(nav => nav.Name, nav => nav);

            var ownedTypes = navigations.Where(a => a.TargetEntityType.IsOwned());
            HasOwnedTypes = ownedTypes.Any();
            OwnedTypesDict = ownedTypes.ToDictionary(a => a.Name, a => a);

            IdentityColumnName = allProperties.SingleOrDefault(a => a.IsPrimaryKey() &&
                                                                     (a.ClrType.Name.StartsWith("Byte") ||
                                                                      a.ClrType.Name.StartsWith("SByte") ||
                                                                      a.ClrType.Name.StartsWith("Int") ||
                                                                      a.ClrType.Name.StartsWith("UInt") ||
                                                                      (isSqlServer && a.ClrType.Name.StartsWith("Decimal"))) &&
                                                                    !a.ClrType.Name.EndsWith("[]") && 
                                                                    a.ValueGenerated == ValueGenerated.OnAdd
                                                              )?.GetColumnName(ObjectIdentifier); // ValueGenerated equals OnAdd even for nonIdentity column like Guid so we only type int as second condition

            // timestamp/row version properties are only set by the Db, the property has a [Timestamp] Attribute or is configured in FluentAPI with .IsRowVersion()
            // They can be identified by the columne type "timestamp" or .IsConcurrencyToken in combination with .ValueGenerated == ValueGenerated.OnAddOrUpdate
            string timestampDbTypeName = nameof(TimestampAttribute).Replace("Attribute", "").ToLower(); // = "timestamp";
            IEnumerable<IProperty> timeStampProperties;
            if (BulkConfig.IgnoreRowVersion)
                timeStampProperties = new List<IProperty>();
            else
                timeStampProperties = allProperties.Where(a => (a.IsConcurrencyToken && a.ValueGenerated == ValueGenerated.OnAddOrUpdate) || a.GetColumnType() == timestampDbTypeName);
            TimeStampColumnName = timeStampProperties.FirstOrDefault()?.GetColumnName(ObjectIdentifier); // can be only One
            TimeStampPropertyName = timeStampProperties.FirstOrDefault()?.Name; // can be only One
            var allPropertiesExceptTimeStamp = allProperties.Except(timeStampProperties);
            var properties = allPropertiesExceptTimeStamp.Where(a => a.GetComputedColumnSql() == null);

            var propertiesWithDefaultValues = allPropertiesExceptTimeStamp.Where(a => a.GetDefaultValue() != null || a.GetDefaultValueSql() != null);
            foreach (var propertyWithDefaultValue in propertiesWithDefaultValues)
            {
                var propertyType = propertyWithDefaultValue.ClrType;
                var instance = propertyType.IsValueType || propertyType.GetConstructor(Type.EmptyTypes) != null
                                  ? Activator.CreateInstance(propertyType)
                                  : null; // when type does not have parameterless constructor, like String for example, then default value is 'null'

                bool listHasAllDefaultValues = !entities.Any(a => a.GetType().GetProperty(propertyWithDefaultValue.Name).GetValue(a, null)?.ToString() != instance?.ToString());
                if (listHasAllDefaultValues) // it is not feasible to have in same list simultaniously both entities groups With and Without default values, they are ommited OnInsert only if all have default values
                {
                    DefaultValueProperties.Add(propertyWithDefaultValue.Name);
                }
            }

            var propertiesOnCompare = allPropertiesExceptTimeStamp.Where(a => a.GetComputedColumnSql() == null);
            var propertiesOnUpdate = allPropertiesExceptTimeStamp.Where(a => a.GetComputedColumnSql() == null);

            // TimeStamp prop. is last column in OutputTable since it is added later with varbinary(8) type in which Output can be inserted
            var outputProperties = allPropertiesExceptTimeStamp.Where(a => a.GetColumnName(ObjectIdentifier) != null).Concat(timeStampProperties);
            OutputPropertyColumnNamesDict = outputProperties.ToDictionary(a => a.Name, b => b.GetColumnName(ObjectIdentifier).Replace("]", "]]")); // square brackets have to be escaped

            bool AreSpecifiedPropertiesToInclude = BulkConfig.PropertiesToInclude?.Count() > 0;
            bool AreSpecifiedPropertiesToExclude = BulkConfig.PropertiesToExclude?.Count() > 0;

            bool AreSpecifiedPropertiesToIncludeOnCompare = BulkConfig.PropertiesToIncludeOnCompare?.Count() > 0;
            bool AreSpecifiedPropertiesToExcludeOnCompare = BulkConfig.PropertiesToExcludeOnCompare?.Count() > 0;

            bool AreSpecifiedPropertiesToIncludeOnUpdate = BulkConfig.PropertiesToIncludeOnUpdate?.Count() > 0;
            bool AreSpecifiedPropertiesToExcludeOnUpdate = BulkConfig.PropertiesToExcludeOnUpdate?.Count() > 0;

            if (AreSpecifiedPropertiesToInclude)
            {
                if (AreSpecifiedUpdateByProperties) // Adds UpdateByProperties to PropertyToInclude if they are not already explicitly listed
                {
                    foreach (var updateByProperty in BulkConfig.UpdateByProperties)
                    {
                        if (!BulkConfig.PropertiesToInclude.Contains(updateByProperty))
                        {
                            BulkConfig.PropertiesToInclude.Add(updateByProperty);
                        }
                    }
                }
                else // Adds PrimaryKeys to PropertyToInclude if they are not already explicitly listed
                {
                    foreach (var primaryKey in PrimaryKeysPropertyColumnNameDict)
                    {
                        if (!BulkConfig.PropertiesToInclude.Contains(primaryKey.Key))
                        {
                            BulkConfig.PropertiesToInclude.Add(primaryKey.Key);
                        }
                    }
                }
            }

            foreach (var property in allProperties)
            {
                if (property.PropertyInfo != null) // skip Shadow Property
                {
                    FastPropertyDict.Add(property.Name, new FastProperty(property.PropertyInfo));
                }
            }

            UpdateByPropertiesAreNullable = properties.Any(a => PrimaryKeysPropertyColumnNameDict.ContainsKey(a.Name) && a.IsNullable);

            if (AreSpecifiedPropertiesToInclude || AreSpecifiedPropertiesToExclude)
            {
                if (AreSpecifiedPropertiesToInclude && AreSpecifiedPropertiesToExclude)
                {
                    throw new MultiplePropertyListSetException(nameof(BulkConfig.PropertiesToInclude), nameof(BulkConfig.PropertiesToExclude));
                }  
                if (AreSpecifiedPropertiesToInclude)
                {
                    properties = properties.Where(a => BulkConfig.PropertiesToInclude.Contains(a.Name));
                    ValidateSpecifiedPropertiesList(BulkConfig.PropertiesToInclude, nameof(BulkConfig.PropertiesToInclude));
                }
                if (AreSpecifiedPropertiesToExclude)
                {
                    properties = properties.Where(a => !BulkConfig.PropertiesToExclude.Contains(a.Name));
                    ValidateSpecifiedPropertiesList(BulkConfig.PropertiesToExclude, nameof(BulkConfig.PropertiesToExclude));
                }
            }

            if (AreSpecifiedPropertiesToIncludeOnCompare || AreSpecifiedPropertiesToExcludeOnCompare)
            {
                if (AreSpecifiedPropertiesToIncludeOnCompare && AreSpecifiedPropertiesToExcludeOnCompare)
                {
                    throw new MultiplePropertyListSetException(nameof(BulkConfig.PropertiesToIncludeOnCompare), nameof(BulkConfig.PropertiesToExcludeOnCompare));
                }
                if (AreSpecifiedPropertiesToIncludeOnCompare)
                {
                    propertiesOnCompare = propertiesOnCompare.Where(a => BulkConfig.PropertiesToIncludeOnCompare.Contains(a.Name));
                    ValidateSpecifiedPropertiesList(BulkConfig.PropertiesToIncludeOnCompare, nameof(BulkConfig.PropertiesToIncludeOnCompare));
                }
                if (AreSpecifiedPropertiesToExcludeOnCompare)
                {
                    propertiesOnCompare = propertiesOnCompare.Where(a => !BulkConfig.PropertiesToExcludeOnCompare.Contains(a.Name));
                    ValidateSpecifiedPropertiesList(BulkConfig.PropertiesToExcludeOnCompare, nameof(BulkConfig.PropertiesToExcludeOnCompare));
                }
            }
            else
            {
                propertiesOnCompare = properties;
            }
            if (AreSpecifiedPropertiesToIncludeOnUpdate || AreSpecifiedPropertiesToExcludeOnUpdate)
            {
                if (AreSpecifiedPropertiesToIncludeOnUpdate && AreSpecifiedPropertiesToExcludeOnUpdate)
                {
                    throw new MultiplePropertyListSetException(nameof(BulkConfig.PropertiesToIncludeOnUpdate), nameof(BulkConfig.PropertiesToExcludeOnUpdate));
                }
                if (AreSpecifiedPropertiesToIncludeOnUpdate)
                {
                    propertiesOnUpdate = propertiesOnUpdate.Where(a => BulkConfig.PropertiesToIncludeOnUpdate.Contains(a.Name));
                    ValidateSpecifiedPropertiesList(BulkConfig.PropertiesToIncludeOnUpdate, nameof(BulkConfig.PropertiesToIncludeOnUpdate));
                }
                if (AreSpecifiedPropertiesToExcludeOnUpdate)
                {
                    propertiesOnUpdate = propertiesOnUpdate.Where(a => !BulkConfig.PropertiesToExcludeOnUpdate.Contains(a.Name));
                    ValidateSpecifiedPropertiesList(BulkConfig.PropertiesToExcludeOnUpdate, nameof(BulkConfig.PropertiesToExcludeOnUpdate));
                }
            }
            else
            {
                propertiesOnUpdate = properties;

                if (BulkConfig.UpdateByProperties != null) // to remove NonIdentity PK like Guid from SET ID = ID, ...
                {
                    propertiesOnUpdate = propertiesOnUpdate.Where(a => !BulkConfig.UpdateByProperties.Contains(a.Name));
                }
                else if (primaryKeys != null)
                {
                    propertiesOnUpdate = propertiesOnUpdate.Where(a => !primaryKeys.ContainsKey(a.Name));
                }
            }

            PropertyColumnNamesCompareDict = propertiesOnCompare.ToDictionary(a => a.Name, b => b.GetColumnName(ObjectIdentifier).Replace("]", "]]"));
            PropertyColumnNamesUpdateDict = propertiesOnUpdate.ToDictionary(a => a.Name, b => b.GetColumnName(ObjectIdentifier).Replace("]", "]]"));

            if (loadOnlyPKColumn)
            {
                if (PrimaryKeysPropertyColumnNameDict.Count() == 0)
                    throw new InvalidBulkConfigException("If no PrimaryKey is defined operation requres bulkConfig set with 'UpdatedByProperties'.");
                PropertyColumnNamesDict = properties.Where(a => PrimaryKeysPropertyColumnNameDict.ContainsKey(a.Name)).ToDictionary(a => a.Name, b => b.GetColumnName(ObjectIdentifier).Replace("]", "]]"));
            }
            else
            {
                PropertyColumnNamesDict = properties.ToDictionary(a => a.Name, b => b.GetColumnName(ObjectIdentifier).Replace("]", "]]"));
                ShadowProperties = new HashSet<string>(properties.Where(p => p.IsShadowProperty() && !p.IsForeignKey()).Select(p => p.GetColumnName(ObjectIdentifier)));
                foreach (var property in properties)
                {
                    var converter = property.GetTypeMapping().Converter;

                    if (converter is null)
                    {
                        continue;
                    }

                    var columnName = property.GetColumnName(ObjectIdentifier);
                    ConvertiblePropertyColumnDict.Add(property.Name, columnName);
                    ConvertibleColumnConverterDict.Add(columnName, converter);
                }

                foreach (var navigation in entityType.GetNavigations().Where(a => !a.IsCollection && !a.TargetEntityType.IsOwned()))
                {
                    FastPropertyDict.Add(navigation.Name, new FastProperty(navigation.PropertyInfo));
                }

                if (HasOwnedTypes)  // Support owned entity property update. TODO: Optimize
                {
                    foreach (var navigationProperty in ownedTypes)
                    {
                        var property = navigationProperty.PropertyInfo;
                        FastPropertyDict.Add(property.Name, new FastProperty(property));

                        Type navOwnedType = type.Assembly.GetType(property.PropertyType.FullName);
                        var ownedEntityType = context.Model.FindEntityType(property.PropertyType);
                        if (ownedEntityType == null) // when entity has more then one ownedType (e.g. Address HomeAddress, Address WorkAddress) or one ownedType is in multiple Entities like Audit is usually.
                        {
                            ownedEntityType = context.Model.GetEntityTypes().SingleOrDefault(a => a.DefiningNavigationName == property.Name && a.DefiningEntityType.Name == entityType.Name);
                        }
                        var ownedEntityProperties = ownedEntityType.GetProperties().ToList();
                        var ownedEntityPropertyNameColumnNameDict = new Dictionary<string, string>();

                        foreach (var ownedEntityProperty in ownedEntityProperties)
                        {
                            if (!ownedEntityProperty.IsPrimaryKey())
                            {
                                string columnName = ownedEntityProperty.GetColumnName(ObjectIdentifier);
                                ownedEntityPropertyNameColumnNameDict.Add(ownedEntityProperty.Name, columnName);
                                var ownedEntityPropertyFullName = property.Name + "_" + ownedEntityProperty.Name;
                                if (!FastPropertyDict.ContainsKey(ownedEntityPropertyFullName))
                                {
                                    FastPropertyDict.Add(ownedEntityPropertyFullName, new FastProperty(ownedEntityProperty.PropertyInfo));
                                }
                            }

                            var converter = ownedEntityProperty.GetValueConverter();
                            if (converter != null)
                            {
                                ConvertibleColumnConverterDict.Add($"{navigationProperty.Name}_{ownedEntityProperty.Name}", converter);
                            }
                        }
                        var ownedProperties = property.PropertyType.GetProperties();
                        foreach (var ownedProperty in ownedProperties)
                        {
                            if (ownedEntityPropertyNameColumnNameDict.ContainsKey(ownedProperty.Name))
                            {
                                string ownedPropertyFullName = property.Name + "." + ownedProperty.Name;
                                var ownedPropertyType = Nullable.GetUnderlyingType(ownedProperty.PropertyType) ?? ownedProperty.PropertyType;

                                bool doAddProperty = true;
                                if (AreSpecifiedPropertiesToInclude && !BulkConfig.PropertiesToInclude.Contains(ownedPropertyFullName))
                                {
                                    doAddProperty = false;
                                }
                                if (AreSpecifiedPropertiesToExclude && BulkConfig.PropertiesToExclude.Contains(ownedPropertyFullName))
                                {
                                    doAddProperty = false;
                                }

                                if (doAddProperty)
                                {
                                    string columnName = ownedEntityPropertyNameColumnNameDict[ownedProperty.Name];
                                    PropertyColumnNamesDict.Add(ownedPropertyFullName, columnName);
                                    PropertyColumnNamesUpdateDict.Add(ownedPropertyFullName, columnName);
                                    OutputPropertyColumnNamesDict.Add(ownedPropertyFullName, columnName);
                                }
                            }
                        }
                    }
                }
            }
        }

        protected void ValidateSpecifiedPropertiesList(List<string> specifiedPropertiesList, string specifiedPropertiesListName)
        {
            foreach (var configSpecifiedPropertyName in specifiedPropertiesList)
            {

                if (!FastPropertyDict.Any(a => a.Key == configSpecifiedPropertyName) &&
                    !configSpecifiedPropertyName.Contains(".") && // Those with dot "." skiped from validating for now since FastPropertyDict here does not contain them
                    !(specifiedPropertiesListName == nameof(BulkConfig.PropertiesToIncludeOnUpdate) && configSpecifiedPropertyName == "") // In PropsToIncludeOnUpdate empty is allowed as config for skipping Update
                   )
                {
                    throw new InvalidOperationException($"PropertyName '{configSpecifiedPropertyName}' specified in '{specifiedPropertiesListName}' not found in Properties.");
                }
            }
        }

        /// <summary>
        /// Supports <see cref="Microsoft.Data.SqlClient.SqlBulkCopy"/>
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="sqlBulkCopy"></param>
        /// <param name="entities"></param>
        /// <param name="setColumnMapping"></param>
        /// <param name="progress"></param>
        public void SetSqlBulkCopyConfig<T>(Microsoft.Data.SqlClient.SqlBulkCopy sqlBulkCopy, IList<T> entities, bool setColumnMapping, Action<decimal> progress)
        {
            sqlBulkCopy.DestinationTableName = InsertToTempTable ? FullTempTableName : FullTableName;
            sqlBulkCopy.BatchSize = BulkConfig.BatchSize;
            sqlBulkCopy.NotifyAfter = BulkConfig.NotifyAfter ?? BulkConfig.BatchSize;
            sqlBulkCopy.SqlRowsCopied += (sender, e) =>
            {
                progress?.Invoke(ProgressHelper.GetProgress(entities.Count, e.RowsCopied)); // round to 4 decimal places
            };
            sqlBulkCopy.BulkCopyTimeout = BulkConfig.BulkCopyTimeout ?? sqlBulkCopy.BulkCopyTimeout;
            sqlBulkCopy.EnableStreaming = BulkConfig.EnableStreaming;

            if (setColumnMapping)
            {
                foreach (var element in PropertyColumnNamesDict)
                {
                    sqlBulkCopy.ColumnMappings.Add(element.Key, element.Value);
                }
            }
        }
        #endregion

        #region SqlCommands
        public async Task<bool> CheckTableExistAsync(DbContext context, TableInfo tableInfo, CancellationToken cancellationToken, bool isAsync)
        {
            if (isAsync)
            {
                await context.Database.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
            }
            else
            {
                context.Database.OpenConnection();
            }

            bool tableExist = false;
            try
            {
                var sqlConnection = context.Database.GetDbConnection();
                var currentTransaction = context.Database.CurrentTransaction;

                using var command = sqlConnection.CreateCommand();
                if (currentTransaction != null)
                    command.Transaction = currentTransaction.GetDbTransaction();
                command.CommandText = SqlQueryBuilder.CheckTableExist(tableInfo.FullTempTableName, tableInfo.BulkConfig.UseTempDB);

                if (isAsync)
                {
                    using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
                    if (reader.HasRows)
                    {
                        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                        {
                            tableExist = (int)reader[0] == 1;
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
                            tableExist = (int)reader[0] == 1;
                        }
                    }
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
            return tableExist;
        }

        protected async Task<int> GetNumberUpdatedAsync(DbContext context, CancellationToken cancellationToken, bool isAsync)
        {
            var resultParameter = (IDbDataParameter)Activator.CreateInstance(typeof(Microsoft.Data.SqlClient.SqlParameter));
            resultParameter.ParameterName = "@result";
            resultParameter.DbType = DbType.Int32;
            resultParameter.Direction = ParameterDirection.Output;
            string sqlQueryCount = SqlQueryBuilder.SelectCountIsUpdateFromOutputTable(this);

            var sqlSetResult = $"SET @result = ({sqlQueryCount});";
            if (isAsync)
            {
                await context.Database.ExecuteSqlRawAsync(sqlSetResult, new object[] { resultParameter }, cancellationToken).ConfigureAwait(false);
            }
            else
            {
                context.Database.ExecuteSqlRaw(sqlSetResult, resultParameter);
            }
            return (int)resultParameter.Value;
        }

        protected async Task<int> GetNumberDeletedAsync(DbContext context, CancellationToken cancellationToken, bool isAsync)
        {
            var resultParameter = (IDbDataParameter)Activator.CreateInstance(typeof(Microsoft.Data.SqlClient.SqlParameter));
            resultParameter.ParameterName = "@result";
            resultParameter.DbType = DbType.Int32;
            resultParameter.Direction = ParameterDirection.Output;
            string sqlQueryCount = SqlQueryBuilder.SelectCountIsDeleteFromOutputTable(this);

            var sqlSetResult = $"SET @result = ({sqlQueryCount});";
            if (isAsync)
            {
                await context.Database.ExecuteSqlRawAsync(sqlSetResult, new object[] { resultParameter }, cancellationToken).ConfigureAwait(false);
            }
            else
            {
                context.Database.ExecuteSqlRaw(sqlSetResult, resultParameter);
            }
            return (int)resultParameter.Value;
        }

        #endregion

        public static string GetUniquePropertyValues(object entity, List<string> propertiesNames, Dictionary<string, FastProperty> fastPropertyDict)
        {
            StringBuilder uniqueBuilder = new StringBuilder(1024);
            string delimiter = "_"; // TODO: Consider making it Config-urable
            foreach (var propertyName in propertiesNames)
            {
                uniqueBuilder.Append(fastPropertyDict[propertyName].Get(entity).ToString());
                uniqueBuilder.Append(delimiter);
            }
            string result = uniqueBuilder.ToString();
            result = result.Substring(0, result.Length - 1); // removes last delimiter
            return result;
        }

        #region ReadProcedures
        public Dictionary<string, string> ConfigureBulkReadTableInfo()
        {
            InsertToTempTable = true;

            var previousPropertyColumnNamesDict = PropertyColumnNamesDict;
            BulkConfig.PropertiesToInclude = PrimaryKeysPropertyColumnNameDict.Select(a => a.Key).ToList();
            PropertyColumnNamesDict = PropertyColumnNamesDict.Where(a => PrimaryKeysPropertyColumnNameDict.ContainsKey(a.Key)).ToDictionary(a => a.Key, a => a.Value);
            return previousPropertyColumnNamesDict;
        }

        internal void UpdateReadEntities<T>(Type type, IList<T> entities, IList<T> existingEntities)
        {
            List<string> propertyNames = PropertyColumnNamesDict.Keys.ToList();
            if (HasOwnedTypes)
            {
                foreach (string ownedTypeName in OwnedTypesDict.Keys)
                {
                    var ownedTypeProperties = OwnedTypesDict[ownedTypeName].ClrType.GetProperties();
                    foreach (var ownedTypeProperty in ownedTypeProperties)
                    {
                        propertyNames.Remove(ownedTypeName + "." + ownedTypeProperty.Name);
                    }
                    propertyNames.Add(ownedTypeName);
                }
            }

            List<string> selectByPropertyNames = PropertyColumnNamesDict.Keys.Where(a => PrimaryKeysPropertyColumnNameDict.ContainsKey(a)).ToList();

            Dictionary<string, T> existingEntitiesDict = new Dictionary<string, T>();
            foreach (var existingEntity in existingEntities)
            {
                string uniqueProperyValues = GetUniquePropertyValues(existingEntity, selectByPropertyNames, FastPropertyDict);
                existingEntitiesDict.Add(uniqueProperyValues, existingEntity);
            }

            for (int i = 0; i < NumberOfEntities; i++)
            {
                T entity = entities[i];
                string uniqueProperyValues = GetUniquePropertyValues(entity, selectByPropertyNames, FastPropertyDict);
                if (existingEntitiesDict.TryGetValue(uniqueProperyValues, out T existingEntity))
                {
                    foreach (var propertyName in propertyNames)
                    {
                        var propertyValue = FastPropertyDict[propertyName].Get(existingEntity);
                        FastPropertyDict[propertyName].Set(entity, propertyValue);
                    }
                }
            }
        }
        #endregion
          
        public void CheckToSetIdentityForPreserveOrder<T>(IList<T> entities, bool reset = false)
        {
            string identityPropertyName = PropertyColumnNamesDict.SingleOrDefault(a => a.Value == IdentityColumnName).Key;

            bool doSetIdentityColumnsForInsertOrder = BulkConfig.PreserveInsertOrder &&
                                                      entities.Count() > 1 &&
                                                      PrimaryKeysPropertyColumnNameDict?.Count() == 1 &&
                                                      PrimaryKeysPropertyColumnNameDict?.Select(a => a.Value).First() == IdentityColumnName &&
                                                      ((reset == true && Convert.ToInt64(FastPropertyDict[identityPropertyName].Get(entities[0])) < 0)
                                                        || (Convert.ToInt64(FastPropertyDict[identityPropertyName].Get(entities[0])) == 0 &&
                                                            Convert.ToInt64(FastPropertyDict[identityPropertyName].Get(entities[1])) == 0));
            if (doSetIdentityColumnsForInsertOrder)
            {
                long i = -entities.Count();
                foreach (var entity in entities)
                {
                    var identityFastProperty = FastPropertyDict[identityPropertyName];
                    if (Convert.ToInt64(identityFastProperty.Get(entity)) == 0 ||           // set only zero(0) values
                        (Convert.ToInt64(identityFastProperty.Get(entity)) <  0 && reset))  // set only negative(-N) values if reset
                    {
                        long value = reset ? 0 : i;

                        object idValue;
                        var idType = identityFastProperty.Property.PropertyType;
                        if (idType == typeof(ushort))
                            idValue = (ushort)value;
                        if (idType == typeof(short))
                            idValue = (short)value;
                        else if (idType == typeof(uint))
                            idValue = (uint)value;
                        else if (idType == typeof(int))
                            idValue = (int)value;
                        else if (idType == typeof(ulong))
                            idValue = (ulong)value;
                        else
                            idValue = (long)value;

                        identityFastProperty.Set(entity, idValue);
                        i++;
                    }
                }
            }
        }
      
        protected void UpdateEntitiesIdentity(DbContext context, TableInfo tableInfo, IList<object> entities, IList<object> entitiesWithOutputIdentity)
        {
            var identityPropertyName = OutputPropertyColumnNamesDict.SingleOrDefault(a => a.Value == IdentityColumnName).Key;

            if (BulkConfig.PreserveInsertOrder) // Updates Db changed Columns in entityList
            {
                int countDiff = entities.Count - entitiesWithOutputIdentity.Count;
                if (countDiff > 0) // When some ommited from Merge because of TimeStamp conflict then changes are not loaded but output is set in TimeStampInfo
                {
                    tableInfo.BulkConfig.TimeStampInfo = new TimeStampInfo {
                        NumberOfSkippedForUpdate = countDiff,
                        EntitiesOutput = entitiesWithOutputIdentity.Cast<object>().ToList()
                };
                    return;
                }
                for (int i = 0; i < NumberOfEntities; i++)
                {
                    if (i == entitiesWithOutputIdentity.Count)
                        break;

                    if (identityPropertyName != null)
                    {
                        var identityPropertyValue = FastPropertyDict[identityPropertyName].Get(entitiesWithOutputIdentity[i]);
                        FastPropertyDict[identityPropertyName].Set(entities[i], identityPropertyValue);
                    }

                    if (TimeStampColumnName != null) // timestamp/rowversion is also generated by the SqlServer so if exist should ba updated as well
                    {
                        string timeStampPropertyName = OutputPropertyColumnNamesDict.SingleOrDefault(a => a.Value == TimeStampColumnName).Key;
                        var timeStampPropertyValue = FastPropertyDict[timeStampPropertyName].Get(entitiesWithOutputIdentity[i]);
                        FastPropertyDict[timeStampPropertyName].Set(entities[i], timeStampPropertyValue);
                    }

                    var propertiesToLoad = tableInfo.OutputPropertyColumnNamesDict.Keys.Where(a => a != identityPropertyName && a != TimeStampColumnName && // already loaded in segmet above
                                                                                                   !tableInfo.PropertyColumnNamesDict.ContainsKey(a)); // add Computed and DefaultValues
                    foreach (var outputPropertyName in propertiesToLoad)
                    {
                        var propertyValue = FastPropertyDict[outputPropertyName].Get(entitiesWithOutputIdentity[i]);
                        FastPropertyDict[outputPropertyName].Set(entities[i], propertyValue);
                    }
                }
            }
            else if (BulkConfig.IncludeGraph)
            {
                for (int i = 0; i < NumberOfEntities; i++)
                {
                    if (i == entitiesWithOutputIdentity.Count())
                        break;
                    var originalEntity = entities[i];
                    var outputEntity = entitiesWithOutputIdentity[i];

                    if (context.Entry(originalEntity).IsKeySet == false)
                    {
                        var newPk = context.Entry(outputEntity).Property(identityPropertyName).CurrentValue;
                        context.Entry(originalEntity).Property(identityPropertyName).CurrentValue = newPk;
                    }
                }
            }
            else // Clears entityList and then refills it with loaded entites from Db
            {
                entities.Clear();
                ((List<object>)entities).AddRange(entitiesWithOutputIdentity);
            }
        }

        // Compiled queries created manually to avoid EF Memory leak bug when using EF with dynamic SQL:
        // https://github.com/borisdj/EFCore.BulkExtensions/issues/73
        // Once the following Issue gets fixed(expected in EF 3.0) this can be replaced with code segment: DirectQuery
        // https://github.com/aspnet/EntityFrameworkCore/issues/12905
        #region CompiledQuery
        public async Task LoadOutputDataAsync<T>(DbContext context, Type type, IList<T> entities, TableInfo tableInfo, CancellationToken cancellationToken, bool isAsync) where T : class
        {
            bool hasIdentity = OutputPropertyColumnNamesDict.Any(a => a.Value == IdentityColumnName);
            int totallNumber = entities.Count;
            if (BulkConfig.SetOutputIdentity && hasIdentity)
            {
                string sqlQuery = SqlQueryBuilder.SelectFromOutputTable(this);
                //var entitiesWithOutputIdentity = await QueryOutputTableAsync<T>(context, sqlQuery).ToListAsync(cancellationToken).ConfigureAwait(false); // TempFIX
                var entitiesWithOutputIdentity = QueryOutputTable(context, type, sqlQuery).Cast<object>().ToList();
                //var entitiesWithOutputIdentity = (typeof(T) == type) ? QueryOutputTable<object>(context, sqlQuery).ToList() : QueryOutputTable(context, type, sqlQuery).Cast<object>().ToList();

                var entitiesObjects = entities.Cast<object>().ToList();
                UpdateEntitiesIdentity(context, tableInfo, entitiesObjects, entitiesWithOutputIdentity);
                totallNumber = entitiesWithOutputIdentity.Count;
            }
            if (BulkConfig.CalculateStats)
            {
                int numberUpdated;
                int numberDeleted;
                if (isAsync)
                {
                    numberUpdated = await GetNumberUpdatedAsync(context, cancellationToken, isAsync: true).ConfigureAwait(false);
                    numberDeleted = await GetNumberDeletedAsync(context, cancellationToken, isAsync: true).ConfigureAwait(false);
                }
                else
                {
                    numberUpdated = GetNumberUpdatedAsync(context, cancellationToken, isAsync: false).GetAwaiter().GetResult();
                    numberDeleted = GetNumberDeletedAsync(context, cancellationToken, isAsync: false).GetAwaiter().GetResult();
                }
                BulkConfig.StatsInfo = new StatsInfo
                {
                    StatsNumberUpdated = numberUpdated,
                    StatsNumberDeleted = numberDeleted,
                    StatsNumberInserted = totallNumber - numberUpdated - numberDeleted
                };
            }
        }

        protected IEnumerable QueryOutputTable(DbContext context, Type type, string sqlQuery)
        {
            var compiled = EF.CompileQuery(GetQueryExpression(type, sqlQuery));
            var result = compiled(context);
            return result;
        }

        /*protected IEnumerable<T> QueryOutputTable<T>(DbContext context, string sqlQuery) where T : class
        {
            var compiled = EF.CompileQuery(GetQueryExpression<T>(sqlQuery));
            var result = compiled(context);
            return result;
        }*/

        /*protected IAsyncEnumerable<T> QueryOutputTableAsync<T>(DbContext context, string sqlQuery) where T : class
        {
            var compiled = EF.CompileAsyncQuery(GetQueryExpression<T>(sqlQuery));
            var result = compiled(context);
            return result;
        }*/

        public Expression<Func<DbContext, IQueryable<T>>> GetQueryExpression<T>(string sqlQuery, bool ordered = true) where T : class
        {
            Expression<Func<DbContext, IQueryable<T>>> expression = null;
            if (BulkConfig.TrackingEntities) // If Else can not be replaced with Ternary operator for Expression
            {
                expression = (ctx) => ctx.Set<T>().FromSqlRaw(sqlQuery);
            }
            else
            {
                expression = (ctx) => ctx.Set<T>().FromSqlRaw(sqlQuery).AsNoTracking();
            }
            return ordered ? Expression.Lambda<Func<DbContext, IQueryable<T>>>(OrderBy(typeof(T), expression.Body, PrimaryKeysPropertyColumnNameDict.Select(a => a.Key).First()), expression.Parameters) : expression;

            // ALTERNATIVELY OrderBy with DynamicLinq ('using System.Linq.Dynamic.Core;' NuGet required) that eliminates need for custom OrderBy<T> method with Expression.
            //var queryOrdered = query.OrderBy(PrimaryKeys[0]);
        }

        public Expression<Func<DbContext, IEnumerable>> GetQueryExpression(Type entityType, string sqlQuery, bool ordered = true)
        {
            var parameter = Expression.Parameter(typeof(DbContext), "ctx");
            var expression = Expression.Call(parameter, "Set", new Type[] { entityType });
            expression = Expression.Call(typeof(RelationalQueryableExtensions), "FromSqlRaw", new Type[] { entityType }, expression, Expression.Constant(sqlQuery), Expression.Constant(Array.Empty<object>()));
            if (BulkConfig.TrackingEntities) // If Else can not be replaced with Ternary operator for Expression
            {
            }
            else
            {
                expression = Expression.Call(typeof(EntityFrameworkQueryableExtensions), "AsNoTracking", new Type[] { entityType }, expression);
            }
            expression = ordered ? OrderBy(entityType, expression, PrimaryKeysPropertyColumnNameDict.Select(a => a.Key).First()) : expression;
            return Expression.Lambda<Func<DbContext, IEnumerable>>(expression, parameter);

            // ALTERNATIVELY OrderBy with DynamicLinq ('using System.Linq.Dynamic.Core;' NuGet required) that eliminates need for custom OrderBy<T> method with Expression.
            //var queryOrdered = query.OrderBy(PrimaryKeys[0]);
        }

        private static MethodCallExpression OrderBy(Type entityType, Expression source, string ordering)
        {
            PropertyInfo property = entityType.GetProperty(ordering);
            ParameterExpression parameter = Expression.Parameter(entityType);
            MemberExpression propertyAccess = Expression.MakeMemberAccess(parameter, property);
            LambdaExpression orderByExp = Expression.Lambda(propertyAccess, parameter);
            return Expression.Call(typeof(Queryable), "OrderBy", new Type[] { entityType, property.PropertyType }, source, Expression.Quote(orderByExp));
        }
        #endregion

        // Currently not used until issue from previous segment is fixed in EFCore
        #region DirectQuery
        /*public void UpdateOutputIdentity<T>(DbContext context, IList<T> entities) where T : class
        {
            if (HasSinglePrimaryKey)
            {
                var entitiesWithOutputIdentity = QueryOutputTable<T>(context).ToList();
                UpdateEntitiesIdentity(entities, entitiesWithOutputIdentity);
            }
        }

        public async Task UpdateOutputIdentityAsync<T>(DbContext context, IList<T> entities) where T : class
        {
            if (HasSinglePrimaryKey)
            {
                var entitiesWithOutputIdentity = await QueryOutputTable<T>(context).ToListAsync().ConfigureAwait(false);
                UpdateEntitiesIdentity(entities, entitiesWithOutputIdentity);
            }
        }

        protected IQueryable<T> QueryOutputTable<T>(DbContext context) where T : class
        {
            string q = SqlQueryBuilder.SelectFromOutputTable(this);
            var query = context.Set<T>().FromSql(q);
            if (!BulkConfig.TrackingEntities)
            {
                query = query.AsNoTracking();
            }

            var queryOrdered = OrderBy(query, PrimaryKeys[0]);
            // ALTERNATIVELY OrderBy with DynamicLinq ('using System.Linq.Dynamic.Core;' NuGet required) that eliminates need for custom OrderBy<T> method with Expression.
            //var queryOrdered = query.OrderBy(PrimaryKeys[0]);

            return queryOrdered;
        }

        private static IQueryable<T> OrderBy<T>(IQueryable<T> source, string ordering)
        {
            Type entityType = typeof(T);
            PropertyInfo property = entityType.GetProperty(ordering);
            ParameterExpression parameter = Expression.Parameter(entityType);
            MemberExpression propertyAccess = Expression.MakeMemberAccess(parameter, property);
            LambdaExpression orderByExp = Expression.Lambda(propertyAccess, parameter);
            MethodCallExpression resultExp = Expression.Call(typeof(Queryable), "OrderBy", new Type[] { entityType, property.PropertyType }, source.Expression, Expression.Quote(orderByExp));
            var orderedQuery = source.Provider.CreateQuery<T>(resultExp);
            return orderedQuery;
        }*/
        #endregion
    }
}
