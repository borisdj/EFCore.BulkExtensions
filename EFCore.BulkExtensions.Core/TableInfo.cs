using EFCore.BulkExtensions.SqlAdapters;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.ChangeTracking.Internal;
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

namespace EFCore.BulkExtensions;

/// <summary>
/// Provides a list of information for EFCore.BulkExtensions that is used internally to know what to do with the data source received
/// </summary>
public class TableInfo
{
#pragma warning disable CS1591 // No XML comments required here.
    public string? Schema { get; set; }
    public string SchemaFormated => Schema != null ? $"[{Schema}]." : "";
    public string? TempSchema { get; set; }
    public string TempSchemaFormated => TempSchema != null ? $"[{TempSchema}]." : "";
    public string? TableName { get; set; }
    public string FullTableName => $"{SchemaFormated}[{TableName}]";
    public Dictionary<string, string> PrimaryKeysPropertyColumnNameDict { get; set; } = null!;
    public Dictionary<string, string> EntityPKPropertyColumnNameDict { get; set; } = null!;
    public bool HasSinglePrimaryKey { get; set; }
    public bool UpdateByPropertiesAreNullable { get; set; }

    protected string TempDBPrefix => BulkConfig.UseTempDB ? "#" : "";
    public string? TempTableSufix { get; set; }
    public string? TempTableName { get; set; }
    public string FullTempTableName => $"{TempSchemaFormated}[{TempDBPrefix}{TempTableName}]";
    public string FullTempOutputTableName => $"{SchemaFormated}[{TempDBPrefix}{TempTableName}Output]";

    public bool CreateOutputTable => BulkConfig.SetOutputIdentity || BulkConfig.CalculateStats;

    public bool InsertToTempTable { get; set; }
    public string? IdentityColumnName { get; set; }
    public bool HasIdentity => IdentityColumnName != null;
    public ValueConverter? IdentityColumnConverter { get; set; }
    public bool HasOwnedTypes { get; set; }
    public bool HasJsonTypes { get; set; }
    public bool HasAbstractList { get; set; }
    public bool ColumnNameContainsSquareBracket { get; set; }
    public bool LoadOnlyPKColumn { get; set; }
    public bool HasSpatialType { get; set; }
    public bool HasTemporalColumns { get; set; }
    public int NumberOfEntities { get; set; }

    public BulkConfig BulkConfig { get; set; } = null!;
    public Dictionary<string, string> OutputPropertyColumnNamesDict { get; set; } = new();
    public Dictionary<string, string> PropertyColumnNamesDict { get; set; } = new();
    public Dictionary<string, string> ColumnNamesTypesDict { get; set; } = new();
    public Dictionary<string, IProperty> ColumnToPropertyDictionary { get; set; } = new();
    public Dictionary<string, string> PropertyColumnNamesCompareDict { get; set; } = new();
    public Dictionary<string, string> PropertyColumnNamesUpdateDict { get; set; } = new();
    public Dictionary<string, FastProperty> FastPropertyDict { get; set; } = new();
    public Dictionary<string, INavigation> AllNavigationsDictionary { get; private set; } = null!;
    public Dictionary<string, INavigation> OwnedTypesDict { get; set; } = new();

    public Dictionary<string, INavigation> OwnedRegularTypesDict { get; set; } = new();
    public Dictionary<string, INavigation> OwnedJsonTypesDict { get; set; } = new();
    public HashSet<string> ShadowProperties { get; set; } = new HashSet<string>();
    public HashSet<string> DefaultValueProperties { get; set; } = new HashSet<string>();

    public Dictionary<string, string> ConvertiblePropertyColumnDict { get; set; } = new Dictionary<string, string>();
    public Dictionary<string, ValueConverter> ConvertibleColumnConverterDict { get; set; } = new Dictionary<string, ValueConverter>();
    public Dictionary<string, int> DateTime2PropertiesPrecisionLessThen7Dict { get; set; } = new Dictionary<string, int>();

    public static string TimeStampOutColumnType => "varbinary(8)";
    public string? TimeStampPropertyName { get; set; }
    public string? TimeStampColumnName { get; set; }

    public string? TextValueFirstPK { get; set; }

    public string SqlActionIUD => "SqlActionIUD";

    protected IEnumerable<object>? EntitiesSortedReference { get; set; } // Operation Merge writes In Output table first Existing that were Updated then for new that were Inserted so this makes sure order is same in list when need to set Output

    public StoreObjectIdentifier ObjectIdentifier { get; set; }

    ////Sqlite
    //internal SqliteConnection? SqliteConnection { get; set; }
    //internal SqliteTransaction? SqliteTransaction { get; set; }


    ////PostgreSql
    //internal NpgsqlConnection? NpgsqlConnection { get; set; }
    ////internal NpgsqlTransaction? NpgsqlTransaction { get; set; }

    ////MySql
    //internal MySqlConnection? MySqlConnection { get; set; }


#pragma warning restore CS1591 // No XML comments required here.

    /// <summary>
    /// Creates an instance of TableInfo
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <param name="context"></param>
    /// <param name="type"></param>
    /// <param name="entities"></param>
    /// <param name="operationType"></param>
    /// <param name="bulkConfig"></param>
    /// <returns></returns>
    /// <exception cref="InvalidOperationException"></exception>
    public static TableInfo CreateInstance<T>(DbContext context, Type? type, IEnumerable<T> entities, OperationType operationType, BulkConfig? bulkConfig)
    {
        var tableInfo = new TableInfo
        {
            NumberOfEntities = entities.Count(),
            BulkConfig = bulkConfig ?? new BulkConfig() { }
        };
        tableInfo.BulkConfig.OperationType = operationType;

        bool isExplicitTransaction = context.Database.GetDbConnection().State == ConnectionState.Open;
        if (tableInfo.BulkConfig.UseTempDB == true && !isExplicitTransaction && (operationType != OperationType.Insert || tableInfo.BulkConfig.SetOutputIdentity))
        {
            throw new InvalidOperationException("When 'UseTempDB' is set then BulkOperation has to be inside Transaction. " +
                                                "Otherwise destination table gets dropped too early because transaction ends before operation is finished.");
        }                                       // throws: 'Cannot access destination table'

        var isDeleteOperation = operationType == OperationType.Delete;
        tableInfo.LoadData(context, type, entities, isDeleteOperation);
        return tableInfo;
    }

    #region Main
    /// <summary>
    /// Configures the table info based on entity data 
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <param name="context"></param>
    /// <param name="type"></param>
    /// <param name="entities"></param>
    /// <param name="loadOnlyPKColumn"></param>
    /// <exception cref="InvalidOperationException"></exception>
    /// <exception cref="MultiplePropertyListSetException"></exception>
    /// <exception cref="InvalidBulkConfigException"></exception>
    public void LoadData<T>(DbContext context, Type? type, IEnumerable<T> entities, bool loadOnlyPKColumn)

    {
        LoadOnlyPKColumn = loadOnlyPKColumn;
        var entityType = type is null ? null : context.Model.FindEntityType(type);
        if (entityType == null)
        {
            type = entities.FirstOrDefault()?.GetType() ?? throw new ArgumentNullException(nameof(type));
            entityType = context.Model.FindEntityType(type);
            HasAbstractList = true;
        }
        if (entityType == null)
        {
            throw new InvalidOperationException($"DbContext does not contain EntitySet for Type: {type?.Name}");
        }

        //var relationalData = entityType.Relational(); relationalData.Schema relationalData.TableName // DEPRECATED in Core3.0
        string? providerName = context.Database.ProviderName?.ToLower();
        bool isSqlServer = providerName?.EndsWith(SqlType.SqlServer.ToString().ToLower()) ?? false;
        bool isNpgsql = providerName?.EndsWith(SqlType.PostgreSql.ToString().ToLower()) ?? false;
        bool isSqlite = providerName?.EndsWith(SqlType.Sqlite.ToString().ToLower()) ?? false;
        bool isMySql = providerName?.EndsWith(SqlType.MySql.ToString().ToLower()) ?? false;

        string? defaultSchema = null;
        if (isSqlServer)
        {
            defaultSchema = "dbo";
        }
        else if (isNpgsql)
        {
            var adapter = SqlAdaptersMapping.CreateBulkOperationsAdapter();
            defaultSchema = adapter.ReconfigureTableInfo(context, this);
        }

        string? customSchema = null;
        string? customTableName = null;
        if (BulkConfig.CustomDestinationTableName != null)
        {
            customTableName = BulkConfig.CustomDestinationTableName;
            if (customTableName.Contains('.'))
            {
                var tableNameSplitList = customTableName.Split('.');
                customSchema = tableNameSplitList[0];
                customTableName = tableNameSplitList[1];
            }
        }

        Schema = customTableName != null ? customSchema : entityType.GetSchema() ?? defaultSchema;

        var entityTableName = entityType.GetTableName();
        TableName = customTableName ?? entityTableName;

        string? sourceSchema = null;
        string? sourceTableName = null;
        if (BulkConfig.CustomSourceTableName != null)
        {
            sourceTableName = BulkConfig.CustomSourceTableName;
            if (sourceTableName.Contains('.'))
            {
                var tableNameSplitList = sourceTableName.Split('.');
                sourceSchema = tableNameSplitList[0];
                sourceTableName = tableNameSplitList[1];
            }
            BulkConfig.UseTempDB = false;
        }

        TempSchema = sourceSchema ?? (isNpgsql && BulkConfig.UseTempDB ? null : Schema);
        TempTableSufix = sourceTableName != null ? "" : "Temp";
        if (BulkConfig.UniqueTableNameTempDb)
        {
            // 8 chars of Guid as tableNameSufix to avoid same name collision with other tables
            TempTableSufix += Guid.NewGuid().ToString()[..8];
            // TODO Consider Hash                                                             
        }
        TempTableName = sourceTableName ?? $"{TableName}{TempTableSufix}";

        if (entityTableName is null)
        {
            throw new ArgumentException("Entity does not contain a table name");
        }

        ObjectIdentifier = StoreObjectIdentifier.Table(entityTableName, entityType.GetSchema());

        var allProperties = new List<IProperty>();
        foreach (var entityProperty in entityType.GetProperties())
        {
            var columnName = entityProperty.GetColumnName(ObjectIdentifier);
            bool isTemporalColumn = columnName is not null
                && entityProperty.IsShadowProperty()
                && entityProperty.ClrType == typeof(DateTime)
                && BulkConfig.TemporalColumns.Contains(columnName);

            HasTemporalColumns = HasTemporalColumns || isTemporalColumn;

            if (columnName == null || isTemporalColumn)
                continue;

            allProperties.Add(entityProperty);
            ColumnNamesTypesDict.Add(columnName, entityProperty.GetColumnType());
            ColumnToPropertyDictionary.Add(columnName, entityProperty);

            if (BulkConfig.DateTime2PrecisionForceRound)
            {
                var columnMappings = entityProperty.GetTableColumnMappings();
                var firstMapping = columnMappings.FirstOrDefault();
                var columnType = firstMapping?.Column.StoreType;
                if ((columnType?.StartsWith("datetime2(") ?? false) && (!columnType?.EndsWith("7)") ?? false))
                {
                    string precisionText = columnType!.Substring(10, 1);
                    int precision = int.Parse(precisionText);
                    DateTime2PropertiesPrecisionLessThen7Dict.Add(firstMapping!.Property.Name, precision); // SqlBulkCopy does Floor instead of Round so Rounding done in memory
                }
            }
        }

        bool areSpecifiedUpdateByProperties = BulkConfig.UpdateByProperties?.Count > 0;
        var primaryKeys = entityType.FindPrimaryKey()?.Properties?.ToDictionary(a => a.Name, b => b.GetColumnName(ObjectIdentifier) ?? string.Empty);
        EntityPKPropertyColumnNameDict = primaryKeys ?? new Dictionary<string, string>();

        if (BulkConfig.UpdateByProperties?.Any(up => !allProperties.Any(pp => pp.Name == up)) is true)
        {
            var wrongNames = BulkConfig.UpdateByProperties!.Where(up => !allProperties.Any(pp => pp.Name == up));
            throw new ArgumentException($"""
                UpdateByProperties contains property names, that doesn't exist in entity properties list.
                Wrong properties: {string.Join(", ", wrongNames)}.
                All properties: {string.Join(", ", allProperties)}.
                """);
        }

        HasSinglePrimaryKey = primaryKeys?.Count == 1;
        PrimaryKeysPropertyColumnNameDict = areSpecifiedUpdateByProperties ? BulkConfig.UpdateByProperties?.ToDictionary(a => a, b => allProperties.First(p => p.Name == b).GetColumnName(ObjectIdentifier) ?? string.Empty) ?? new()
                                                                           : (primaryKeys ?? new Dictionary<string, string>());

        // load all derived type properties
        if (entityType.IsAbstract())
        {
            foreach (var derivedType in entityType.GetDirectlyDerivedTypes())
            {
                foreach (var derivedProperty in derivedType.GetProperties())
                {
                    if (!allProperties.Contains(derivedProperty))
                        allProperties.Add(derivedProperty);
                }
            }
        }

        var navigations = entityType.GetNavigations();
        AllNavigationsDictionary = navigations.ToDictionary(nav => nav.Name, nav => nav);

        OwnedTypesDict = navigations.Where(a => a.TargetEntityType.IsOwned()).ToDictionary(a => a.Name, a => a);
        HasOwnedTypes = OwnedTypesDict.Count > 0;

#if NET7_0_OR_GREATER
        OwnedRegularTypesDict = navigations.Where(a => a.TargetEntityType.IsOwned() && !a.TargetEntityType.IsMappedToJson()).ToDictionary(a => a.Name, a => a);
        OwnedJsonTypesDict = navigations.Where(a => a.TargetEntityType.IsMappedToJson()).ToDictionary(a => a.Name, a => a);
#else
        OwnedRegularTypesDict = navigations.Where(a => a.TargetEntityType.IsOwned()).ToDictionary(a => a.Name, a => a);
        OwnedJsonTypesDict = navigations.Where(a => a.TargetEntityType == null).ToDictionary(a => a.Name, a => a); // should be empty
#endif

        HasJsonTypes = OwnedJsonTypesDict.Count > 0;

        if (isSqlServer || isNpgsql || isMySql)
        {
            var strategyName = SqlAdaptersMapping.DbServer.ValueGenerationStrategy;
            if (!strategyName.Contains(":Value"))
            {
                strategyName = strategyName.Replace("Value", ":Value"); //example 'SqlServer:ValueGenerationStrategy'
            }

            foreach (var property in allProperties)
            {
                var annotation = property.FindAnnotation(strategyName);
                bool hasIdentity = false;
                if (annotation != null)
                {
                    hasIdentity = SqlAdaptersMapping.DbServer.PropertyHasIdentity(annotation);
                }
                if (hasIdentity)
                {
                    IdentityColumnName = property.GetColumnName(ObjectIdentifier);
                    break;
                }
            }
        }
        if (isSqlite) // SQLite no ValueGenerationStrategy
        {
            // for HiLo on SqlServer was returning True when should be False
            IdentityColumnName = allProperties.SingleOrDefault(a => a.IsPrimaryKey() &&
                                                    a.ValueGenerated == ValueGenerated.OnAdd && // ValueGenerated equals OnAdd for nonIdentity column like Guid so take only number types
                                                    (a.ClrType.Name.StartsWith("Byte") ||
                                                     a.ClrType.Name.StartsWith("SByte") ||
                                                     a.ClrType.Name.StartsWith("Int") ||
                                                     a.ClrType.Name.StartsWith("UInt") ||
                                                     (isSqlServer && a.ClrType.Name.StartsWith("Decimal")))
                                              )?.GetColumnName(ObjectIdentifier);
        }

        // timestamp/row version properties are only set by the Db, the property has a [Timestamp] Attribute or is configured in FluentAPI with .IsRowVersion()
        // They can be identified by the columne type "timestamp" or .IsConcurrencyToken in combination with .ValueGenerated == ValueGenerated.OnAddOrUpdate
        string timestampDbTypeName = nameof(TimestampAttribute).Replace("Attribute", "").ToLower(); // = "timestamp";
        IEnumerable<IProperty> timeStampProperties;
        if (BulkConfig.IgnoreRowVersion)
            timeStampProperties = new List<IProperty>();
        else
            timeStampProperties = allProperties.Where(a => a.IsConcurrencyToken && a.ValueGenerated == ValueGenerated.OnAddOrUpdate); // || a.GetColumnType() == timestampDbTypeName // removed as unnecessary and might not be correct

        TimeStampColumnName = timeStampProperties.FirstOrDefault()?.GetColumnName(ObjectIdentifier); // can be only One
        TimeStampPropertyName = timeStampProperties.FirstOrDefault()?.Name; // can be only One
        var allPropertiesExceptTimeStamp = allProperties.Except(timeStampProperties);
        var properties = allPropertiesExceptTimeStamp.Where(a => a.GetComputedColumnSql() == null);

        var propertiesWithDefaultValues = allPropertiesExceptTimeStamp.Where(a =>
            !a.IsShadowProperty() &&
            (a.GetDefaultValueSql() != null ||
             (a.GetDefaultValue() != null &&
              a.ValueGenerated != ValueGenerated.Never &&
              a.ClrType != typeof(Guid)) // Since .Net_6.0 in EF 'Guid' type has DefaultValue even when not explicitly defined with Annotation or FluentApi
            ));
        foreach (var propertyWithDefaultValue in propertiesWithDefaultValues)
        {
            var propertyType = propertyWithDefaultValue.ClrType;
            var instance = propertyType.IsValueType || propertyType.GetConstructor(Type.EmptyTypes) != null
                              ? Activator.CreateInstance(propertyType)
                              : null; // when type does not have parameterless constructor, like String for example, then default value is 'null'

            bool listHasAllDefaultValues = !entities.Any(a => GetPropertyUnambiguous(a?.GetType(), propertyWithDefaultValue.Name)?.GetValue(a, null)?.ToString() != instance?.ToString());
            // it is not feasible to have in same list simultaneously both entities groups With and Without default values, they are omitted OnInsert only if all have default values or if it is PK (like Guid DbGenerated)
            if (listHasAllDefaultValues || (PrimaryKeysPropertyColumnNameDict.ContainsKey(propertyWithDefaultValue.Name) && propertyType == typeof(Guid)))
            {
                DefaultValueProperties.Add(propertyWithDefaultValue.Name);
            }
        }

        var propertiesOnCompare = allPropertiesExceptTimeStamp.Where(a => a.GetComputedColumnSql() == null);
        var propertiesOnUpdate = allPropertiesExceptTimeStamp.Where(a => a.GetComputedColumnSql() == null);

        // TimeStamp prop. is last column in OutputTable since it is added later with varbinary(8) type in which Output can be inserted
        var outputProperties = allPropertiesExceptTimeStamp.Where(a => a.GetColumnName(ObjectIdentifier) != null).Concat(timeStampProperties);
        OutputPropertyColumnNamesDict = outputProperties.ToDictionary(a => a.Name, b => b.GetColumnName(ObjectIdentifier)?.Replace("]", "]]") ?? string.Empty); // square brackets have to be escaped
        if (HasTemporalColumns)
        {
            foreach (var temporalColumns in BulkConfig.TemporalColumns)
                OutputPropertyColumnNamesDict.Add(temporalColumns, temporalColumns);
        }

        bool AreSpecifiedPropertiesToInclude = BulkConfig.PropertiesToInclude?.Count > 0;
        bool AreSpecifiedPropertiesToExclude = BulkConfig.PropertiesToExclude?.Count > 0;

        bool AreSpecifiedPropertiesToIncludeOnCompare = BulkConfig.PropertiesToIncludeOnCompare?.Count > 0;
        bool AreSpecifiedPropertiesToExcludeOnCompare = BulkConfig.PropertiesToExcludeOnCompare?.Count > 0;

        bool AreSpecifiedPropertiesToIncludeOnUpdate = BulkConfig.PropertiesToIncludeOnUpdate?.Count > 0;
        bool AreSpecifiedPropertiesToExcludeOnUpdate = BulkConfig.PropertiesToExcludeOnUpdate?.Count > 0;

        if (AreSpecifiedPropertiesToInclude)
        {
            if (areSpecifiedUpdateByProperties) // Adds UpdateByProperties to PropertyToInclude if they are not already explicitly listed
            {
                if (BulkConfig.UpdateByProperties is not null)
                {
                    foreach (var updateByProperty in BulkConfig.UpdateByProperties)
                    {
                        if (!BulkConfig.PropertiesToInclude?.Contains(updateByProperty) ?? false)
                        {
                            BulkConfig.PropertiesToInclude?.Add(updateByProperty);
                        }
                    }
                }
            }
            else // Adds PrimaryKeys to PropertyToInclude if they are not already explicitly listed
            {
                foreach (var primaryKey in PrimaryKeysPropertyColumnNameDict)
                {
                    if (!BulkConfig.PropertiesToInclude?.Contains(primaryKey.Key) ?? false)
                    {
                        BulkConfig.PropertiesToInclude?.Add(primaryKey.Key);
                    }
                }
            }
        }

        foreach (var property in allProperties)
        {
            if (property.PropertyInfo != null) // skip Shadow Property
            {
                FastPropertyDict.Add(property.Name, FastProperty.GetOrCreate(property.PropertyInfo));
            }

            if (property.IsShadowProperty() && property.IsForeignKey())
            {
                // TODO: Does Shadow ForeignKey Property aways contain only one ForgeignKey? 
                var navigationProperty = property.GetContainingForeignKeys().FirstOrDefault()?.DependentToPrincipal?.PropertyInfo;
                if (navigationProperty is not null)
                {
                    var navigationEntityType = context.Model.FindEntityType(navigationProperty.PropertyType);
                    var navigationProperties = navigationEntityType?.GetProperties().Where(p => p.IsPrimaryKey()).ToList() ?? new();

                    foreach (var navEntityProperty in navigationProperties)
                    {
                        var fullName = navigationProperty.Name + "_" + navEntityProperty.Name;
                        if (!FastPropertyDict.ContainsKey(fullName) && navEntityProperty.PropertyInfo is not null)
                        {
                            FastPropertyDict.Add(fullName, FastProperty.GetOrCreate(navEntityProperty.PropertyInfo));
                        }
                    }
                }
            }

            var converter = property.GetTypeMapping().Converter;
            if (converter is not null)
            {
                var columnName = property.GetColumnName(ObjectIdentifier) ?? string.Empty;
                ConvertiblePropertyColumnDict.Add(property.Name, columnName);
                if (!ConvertibleColumnConverterDict.ContainsKey(columnName))
                {
                    ConvertibleColumnConverterDict.Add(columnName, converter);
                }

                if (columnName == IdentityColumnName)
                    IdentityColumnConverter = converter;
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
                properties = properties.Where(a => BulkConfig.PropertiesToInclude?.Contains(a.Name) ?? false);
                ValidateSpecifiedPropertiesList(BulkConfig.PropertiesToInclude, nameof(BulkConfig.PropertiesToInclude));
            }
            if (AreSpecifiedPropertiesToExclude)
            {
                properties = properties.Where(a => !BulkConfig.PropertiesToExclude?.Contains(a.Name) ?? false);
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
                propertiesOnCompare = propertiesOnCompare.Where(a => BulkConfig.PropertiesToIncludeOnCompare?.Contains(a.Name) ?? false);
                ValidateSpecifiedPropertiesList(BulkConfig.PropertiesToIncludeOnCompare, nameof(BulkConfig.PropertiesToIncludeOnCompare));
            }
            if (AreSpecifiedPropertiesToExcludeOnCompare)
            {
                propertiesOnCompare = propertiesOnCompare.Where(a => !BulkConfig.PropertiesToExcludeOnCompare?.Contains(a.Name) ?? false);
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
                propertiesOnUpdate = propertiesOnUpdate.Where(a => BulkConfig.PropertiesToIncludeOnUpdate?.Contains(a.Name) ?? false);
                ValidateSpecifiedPropertiesList(BulkConfig.PropertiesToIncludeOnUpdate, nameof(BulkConfig.PropertiesToIncludeOnUpdate));
            }
            if (AreSpecifiedPropertiesToExcludeOnUpdate)
            {
                propertiesOnUpdate = propertiesOnUpdate.Where(a => !BulkConfig.PropertiesToExcludeOnUpdate?.Contains(a.Name) ?? false);
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

        PropertyColumnNamesCompareDict = propertiesOnCompare.ToDictionary(a => a.Name, b => b.GetColumnName(ObjectIdentifier)?.Replace("]", "]]") ?? string.Empty);
        PropertyColumnNamesUpdateDict = propertiesOnUpdate.ToDictionary(a => a.Name, b => b.GetColumnName(ObjectIdentifier)?.Replace("]", "]]") ?? string.Empty);

        if (loadOnlyPKColumn)
        {
            if (PrimaryKeysPropertyColumnNameDict.Count == 0)
                throw new InvalidBulkConfigException("If no PrimaryKey is defined operation requres bulkConfig set with 'UpdatedByProperties'.");
            PropertyColumnNamesDict = properties.Where(a => PrimaryKeysPropertyColumnNameDict.ContainsKey(a.Name)).ToDictionary(a => a.Name, b => b.GetColumnName(ObjectIdentifier)?.Replace("]", "]]") ?? string.Empty);
        }
        else
        {
            PropertyColumnNamesDict = properties.ToDictionary(a => a.Name, b => b.GetColumnName(ObjectIdentifier)?.Replace("]", "]]") ?? string.Empty);
            ShadowProperties = new HashSet<string>(properties.Where(p => p.IsShadowProperty() && !p.IsForeignKey()).Select(p => p.GetColumnName(ObjectIdentifier) ?? string.Empty));

            foreach (var navigation in entityType.GetNavigations().Where(a => !a.IsCollection && !a.TargetEntityType.IsOwned()))
            {
                if (navigation.PropertyInfo is not null)
                {
                    FastPropertyDict.Add(navigation.Name, FastProperty.GetOrCreate(navigation.PropertyInfo));
                }
            }

            if (HasOwnedTypes)  // Support owned entity property update. TODO: Optimize
            {
                foreach (var navigationProperty in OwnedRegularTypesDict.Values.ToList())
                {
                    AddOwnedType(navigationProperty);

                    void AddOwnedType(INavigation navigationProperty, string prefix = "")
                    {
                        // Add it to the dictionary if it doesn't exist already
                        OwnedRegularTypesDict.TryAdd(prefix + navigationProperty.Name, navigationProperty);

                        var property = navigationProperty.PropertyInfo;
                        FastPropertyDict.Add(prefix + property!.Name, FastProperty.GetOrCreate(property));

                        // If the OwnedType is mapped to the separate table, don't try merge it into its owner
                        if (OwnedTypeUtil.IsOwnedInSameTableAsOwner(navigationProperty) == false)
                            return;

                        prefix += $"{property.Name}_";

                        var ownedList = context.Model.FindEntityTypes(property.PropertyType).Where(x => x.IsInOwnershipPath(entityType)).ToList();
                        var ownedEntityType = ownedList.Count == 1
                             ? ownedList[0] // IsInOwnershipPath fix for with multiple parents (issue #1149)
                             : context.Model.GetEntityTypes().SingleOrDefault(x => x.ClrType == property.PropertyType && x.Name.StartsWith(entityType.Name + "." + property.Name + "#"));
                               // fix when entity has more then one ownedType (e.g. Address HomeAddress, Address WorkAddress) or one ownedType is in multiple Entities like Audit is usually.

                        var ownedEntityProperties = ownedEntityType?.GetProperties().ToList() ?? [];
                        var ownedEntityPropertyNameColumnNameDict = new Dictionary<string, string>();

                        foreach (var ownedEntityProperty in ownedEntityProperties)
                        {
                            string columnName = ownedEntityProperty.GetColumnName(ObjectIdentifier) ?? string.Empty;

                            if (!ownedEntityProperty.IsPrimaryKey())
                            {
                                ownedEntityPropertyNameColumnNameDict.Add(ownedEntityProperty.Name, columnName);
                                var ownedEntityPropertyFullName = prefix + ownedEntityProperty.Name;
                                if (!FastPropertyDict.ContainsKey(ownedEntityPropertyFullName) && ownedEntityProperty.PropertyInfo is not null)
                                {
                                    FastPropertyDict.Add(ownedEntityPropertyFullName, FastProperty.GetOrCreate(ownedEntityProperty.PropertyInfo));
                                }
                            }

                            var converter = ownedEntityProperty.GetValueConverter() ?? ownedEntityProperty.GetTypeMapping().Converter;
                            if (converter != null)
                            {
                                ConvertibleColumnConverterDict.Add($"{prefix}{ownedEntityProperty.Name}", converter);
                            }

                            ColumnNamesTypesDict[columnName] = ownedEntityProperty.GetColumnType();
                        }
                        foreach (var ownedProperty in property.PropertyType.GetProperties())
                        {
                            if (ownedEntityPropertyNameColumnNameDict.TryGetValue(ownedProperty.Name, out string? columnName))
                            {
                                string ownedPropertyFullName = prefix.Replace('_', '.') + ownedProperty.Name;
                                var ownedPropertyType = Nullable.GetUnderlyingType(ownedProperty.PropertyType) ?? ownedProperty.PropertyType;

                                bool doAddProperty = true;
                                if (AreSpecifiedPropertiesToInclude && !(BulkConfig.PropertiesToInclude?.Contains(ownedPropertyFullName) ?? false))
                                {
                                    doAddProperty = false;
                                }
                                if (AreSpecifiedPropertiesToExclude && (BulkConfig.PropertiesToExclude?.Contains(ownedPropertyFullName) ?? false))
                                {
                                    doAddProperty = false;
                                }

                                if (doAddProperty)
                                {
                                    PropertyColumnNamesDict.Add(ownedPropertyFullName, columnName);
                                    PropertyColumnNamesCompareDict.Add(ownedPropertyFullName, columnName);
                                    PropertyColumnNamesUpdateDict.Add(ownedPropertyFullName, columnName);
                                    OutputPropertyColumnNamesDict.Add(ownedPropertyFullName, columnName);
                                }
                            }
                        }
                        IEnumerable<INavigation>? ownedTypes;
#if NET6_0
                        ownedTypes = ownedEntityType?.GetNavigations().Where(a => a.TargetEntityType.IsOwned() && !a.TargetEntityType.IsMappedToJson());
#else
                        ownedTypes = ownedEntityType?.GetNavigations().Where(a => a.TargetEntityType.IsOwned() && !a.TargetEntityType.IsMappedToJson());
#endif
                        foreach (var ownedNavigationProperty in ownedTypes ?? [])
                        {
                            AddOwnedType(ownedNavigationProperty, prefix);
                        }
                    }
                }
            }

            if (HasJsonTypes)
            {
                var jsonTypes = OwnedJsonTypesDict.Values.ToList();
                foreach (var jsonProperty in jsonTypes)
                {
                    var property = jsonProperty.PropertyInfo;

                    //var value = FastPropertyDict[property?.Name!].Get(jsonProperty);
                    //var jsonValue = System.Text.Json.JsonSerializer.Serialize(value);
                    //var j1 = jsonProperty.TargetEntityType.GetJsonPropertyName();

                    string columnName = property?.Name!;
                    string propertyName = property?.Name!;

                    bool skipColumn = (BulkConfig.PropertiesToInclude != null && !BulkConfig.PropertiesToInclude.Contains(propertyName)) ||
                                      (BulkConfig.PropertiesToExclude != null && BulkConfig.PropertiesToExclude.Contains(propertyName));

                    if (!skipColumn)
                    {
                        FastPropertyDict.Add(property!.Name, FastProperty.GetOrCreate(property));
                        PropertyColumnNamesDict.Add(propertyName, columnName);
                        PropertyColumnNamesCompareDict.Add(propertyName, columnName);
                        PropertyColumnNamesUpdateDict.Add(propertyName, columnName);
                        OutputPropertyColumnNamesDict.Add(propertyName, columnName);
                    }
                }
            }
        }

        if (PrimaryKeysPropertyColumnNameDict.Count == 1)
        {
            string pkName = PrimaryKeysPropertyColumnNameDict.Keys.First();
            if (entities != null && entities.Count() > 0)
            {
                object? instance = entities.First();
                TextValueFirstPK = FastPropertyDict[pkName].Get(instance ?? "")?.ToString();
            }
        }
    }

    private static PropertyInfo? GetPropertyUnambiguous(Type? type, string name)
    {
        if (name == null) throw new ArgumentNullException(nameof(name));
        if (type == null) return null;

        while (type != null)
        {
            var property = type.GetProperty(name, BindingFlags.DeclaredOnly | BindingFlags.Public | BindingFlags.Instance);

            if (property != null)
            {
                return property;
            }

            type = type.BaseType;
        }

        return null;
    }

    /// <summary>
    /// Validates the specified property list
    /// </summary>
    /// <param name="specifiedPropertiesList"></param>
    /// <param name="specifiedPropertiesListName"></param>
    /// <exception cref="InvalidOperationException"></exception>
    protected void ValidateSpecifiedPropertiesList(List<string>? specifiedPropertiesList, string specifiedPropertiesListName)

    {
        if (specifiedPropertiesList is not null)
        {
            foreach (var configSpecifiedPropertyName in specifiedPropertiesList)
            {

                if (!FastPropertyDict.Any(a => a.Key == configSpecifiedPropertyName) &&
                    !configSpecifiedPropertyName.Contains('.') && // Those with dot "." skiped from validating for now since FastPropertyDict here does not contain them
                    !(specifiedPropertiesListName == nameof(BulkConfig.PropertiesToIncludeOnUpdate) && configSpecifiedPropertyName == "") && // In PropsToIncludeOnUpdate empty is allowed as config for skipping Update
                    !BulkConfig.TemporalColumns.Contains(configSpecifiedPropertyName)
                    )
                {
                    throw new InvalidOperationException($"PropertyName '{configSpecifiedPropertyName}' specified in '{specifiedPropertiesListName}' not found in Properties.");
                }
            }
        }
    }

    #endregion

    #region SqlCommands

    /// <summary>
    /// Checks if the table exists
    /// </summary>
    /// <param name="context"></param>
    /// <param name="tableInfo"></param>
    /// <param name="cancellationToken"></param>
    /// <param name="isAsync"></param>
    /// <returns></returns>
    public static async Task<bool> CheckTableExistAsync(DbContext context, TableInfo tableInfo, bool isAsync, CancellationToken cancellationToken)
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

    /// <summary>
    /// Checks the IUD Stats numbers of entities
    /// </summary>
    /// <param name="context"></param>
    /// <param name="cancellationToken"></param>
    /// <param name="isAsync"></param>
    /// <returns></returns>
    protected async Task<int[]> GetStatsNumbersAsync(DbContext context, bool isAsync, CancellationToken cancellationToken)
    {
        var sqlQueryCountBase = $"SELECT COUNT(*) FROM {FullTempOutputTableName} WHERE [{SqlActionIUD}] = ";
        var actionCodes = new List<string> { "I", "U", "D" }; // IUD - Inserted, Updated, Deleted

        var sqlQueryCounts = new List<string>();
        var sqlParamsNames = new List<string>();
        var sqlParams = new List<IDbDataParameter>();
        foreach (var actionCode in actionCodes)
        {
            sqlQueryCounts.Add(sqlQueryCountBase + $"'{actionCode}'");

            var resultParameter = SqlAdaptersMapping.DbServer.QueryBuilder.CreateParameter("@result" + actionCode, null);
            if (resultParameter is null)
            {
                throw new ArgumentException("Unable to create an instance of IDbDataParameter");
            }
            resultParameter.DbType = DbType.Int32;
            resultParameter.Direction = ParameterDirection.Output;

            sqlParams.Add(resultParameter);

            sqlParamsNames.Add(resultParameter.ParameterName);
        }

        var sqlSetResult = $"SET {sqlParamsNames[0]} = ({sqlQueryCounts[0]}); " +
                           $"SET {sqlParamsNames[1]} = ({sqlQueryCounts[1]}); " +
                           $"SET {sqlParamsNames[2]} = ({sqlQueryCounts[2]});";

        var sqlParamsArray = sqlParams.ToArray();
        if (isAsync)
        {
            await context.Database.ExecuteSqlRawAsync(sqlSetResult, sqlParamsArray, cancellationToken).ConfigureAwait(false);
        }
        else
        {
            context.Database.ExecuteSqlRaw(sqlSetResult, sqlParamsArray);
        }

        var resultArray = new int[] { (int)sqlParams[0].Value!, (int)sqlParams[1].Value!, (int)sqlParams[2].Value! };
        return resultArray;
    }
    #endregion

    /// <summary>
    /// Returns the unique property values
    /// </summary>
    /// <param name="entity"></param>
    /// <param name="propertiesNames"></param>
    /// <param name="fastPropertyDict"></param>
    /// <returns></returns>
    public static string GetUniquePropertyValues(object entity, List<string> propertiesNames, Dictionary<string, FastProperty> fastPropertyDict)
    {
        StringBuilder uniqueBuilder = new(1024);
        string delimiter = "_"; // TODO: Consider making it Config-urable
        foreach (var propertyName in propertiesNames)
        {
            var property = fastPropertyDict[propertyName].Get(entity);
            if (property is Array propertyArray)
            {
                foreach (var element in propertyArray)
                {
                    uniqueBuilder.Append(element?.ToString() ?? "null");
                }
            }
            else
            {
                uniqueBuilder.Append(property?.ToString() ?? "null");
            }

            uniqueBuilder.Append(delimiter);
        }
        string result = uniqueBuilder.ToString() == "null" ? "" : uniqueBuilder.ToString();
        result = result[0..^1]; // removes last delimiter
        return result;
    }

    #region ReadProcedures
    /// <summary>
    /// Configures the bulk read column names for the table info
    /// </summary>
    /// <returns></returns>
    public Dictionary<string, string> ConfigureBulkReadTableInfo()
    {
        InsertToTempTable = true;

        var previousPropertyColumnNamesDict = PropertyColumnNamesDict;
        BulkConfig.PropertiesToInclude = PrimaryKeysPropertyColumnNameDict.Select(a => a.Key).ToList();
        PropertyColumnNamesDict = PropertyColumnNamesDict.Where(a => PrimaryKeysPropertyColumnNameDict.ContainsKey(a.Key)).ToDictionary(a => a.Key, a => a.Value);
        return previousPropertyColumnNamesDict;
    }

    internal void UpdateReadEntities<T>(IEnumerable<T> entities, IList<T> existingEntities, DbContext context)
    {
        var propColDict = BulkConfig.LoadOnlyIncludedColumns ? PropertyColumnNamesDict : OutputPropertyColumnNamesDict;
        List<string> propertyNames = propColDict.Keys.ToList();
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

        List<string> selectByPropertyNames = PropertyColumnNamesDict.Keys
            .Where(a => PrimaryKeysPropertyColumnNameDict.ContainsKey(a)).ToList();

        Dictionary<string, T> existingEntitiesDict = new();
        foreach (var existingEntity in existingEntities)
        {
            string uniqueProperyValues = GetUniquePropertyValues(existingEntity!, selectByPropertyNames, FastPropertyDict);
            existingEntitiesDict.TryAdd(uniqueProperyValues, existingEntity);
        }

        for (int i = 0; i < NumberOfEntities; i++)
        {
            T entity = entities.ElementAt(i);
            string uniqueProperyValues = GetUniquePropertyValues(entity!, selectByPropertyNames, FastPropertyDict);

            existingEntitiesDict.TryGetValue(uniqueProperyValues, out T? existingEntity);
            bool isPostgreSql = context.Database.ProviderName?.EndsWith(SqlType.PostgreSql.ToString(), StringComparison.InvariantCultureIgnoreCase) ?? false;
            if (existingEntity == null && isPostgreSql && i < existingEntities.Count && entities.Count() == existingEntities.Count) // && entities.Count == existingEntities.Count conf fix for READ. TODO change (issue 1027)
            {
                existingEntity = existingEntities.ElementAt(i); // TODO check if BinaryImport with COPY on Postgres preserves order
            }
            if (existingEntity != null)
            {
                foreach (var propertyName in propertyNames)
                {
                    if (FastPropertyDict.ContainsKey(propertyName))
                    {
                        var propertyValue = FastPropertyDict[propertyName].Get(existingEntity);
                        FastPropertyDict[propertyName].Set(entity!, propertyValue);
                    }
                    else
                    {
                        //TODO: Shadow FK property update
                    }
                }
            }
        }
    }

    internal void ReplaceReadEntities<T>(IEnumerable<T> entities, IList<T> existingEntities)
    {
        if (typeof(T) == existingEntities.FirstOrDefault()?.GetType())
        {
            var entitiesList = (List<T>)entities;
            entitiesList.Clear();
            entitiesList.AddRange(existingEntities);
        }
        else
        {
            var entitiesObjects = entities.Cast<object>().ToList();
            entitiesObjects.Clear();
            entitiesObjects.AddRange((IEnumerable<object>)existingEntities);
        }
    }
    #endregion
    /// <summary>
    /// Sets the identity preserve order
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <param name="tableInfo"></param>
    /// <param name="entities"></param>
    /// <param name="reset"></param>
    public void CheckToSetIdentityForPreserveOrder<T>(TableInfo tableInfo, IEnumerable<T> entities, bool reset = false)
    {
        string identityPropertyName = PropertyColumnNamesDict.SingleOrDefault(a => a.Value == IdentityColumnName).Key;

        bool doSetIdentityColumnsForInsertOrder = BulkConfig.PreserveInsertOrder &&
                                                  entities.Count() > 1 &&
                                                  PrimaryKeysPropertyColumnNameDict?.Count == 1 &&
                                                  PrimaryKeysPropertyColumnNameDict?.Select(a => a.Value).First() == IdentityColumnName;

        var operationType = tableInfo.BulkConfig.OperationType;
        if (doSetIdentityColumnsForInsertOrder == true)
        {
            if (operationType == OperationType.Insert) // Insert should either have all zeros for automatic order, or they can be manually set
            {
                var propertyValue = FastPropertyDict[identityPropertyName].Get(entities.ElementAt(0)!);
                var identityValue = Convert.ToInt64(IdentityColumnConverter != null ? IdentityColumnConverter.ConvertToProvider(propertyValue) : propertyValue);

                if (identityValue != 0) // (to check it fast, condition for all 0s is only done on first one)
                {
                    doSetIdentityColumnsForInsertOrder = false;
                }
            }
        }

        if (doSetIdentityColumnsForInsertOrder)
        {
            bool sortEntities = !reset && BulkConfig.SetOutputIdentity &&
                                (operationType == OperationType.Update || operationType == OperationType.InsertOrUpdate || operationType == OperationType.InsertOrUpdateOrDelete);
            var entitiesExistingDict = new Dictionary<long, T>();
            var entitiesNew = new List<T>();
            var entitiesSorted = new List<T>();

            long i = -entities.Count();
            foreach (var entity in entities)
            {
                var identityFastProperty = FastPropertyDict[identityPropertyName];
                var propertyValue = identityFastProperty.Get(entity!);
                long identityValue = Convert.ToInt64(IdentityColumnConverter != null ? IdentityColumnConverter.ConvertToProvider(propertyValue) : propertyValue);

                if (identityValue == 0 ||         // set only zero(0) values
                    (identityValue < 0 && reset)) // set only negative(-N) values if reset
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
                    else if (idType == typeof(decimal))
                        idValue = (decimal)value;
                    else
                        idValue = value; // type 'long' left as default

                    identityFastProperty.Set(entity!, IdentityColumnConverter != null ? IdentityColumnConverter.ConvertFromProvider(idValue) : idValue);
                    i++;
                }
                if (sortEntities)
                {
                    if (identityValue != 0)
                        entitiesExistingDict.Add(identityValue, entity); // first load existing ones
                    else
                        entitiesNew.Add(entity);
                }
            }
            if (sortEntities)
            {
                entitiesSorted = entitiesExistingDict.OrderBy(a => a.Key).Select(a => a.Value).ToList();
                entitiesSorted.AddRange(entitiesNew); // then append new ones
                tableInfo.EntitiesSortedReference = entitiesSorted.Cast<object>().ToList();
            }
        }
    }

    /// <summary>
    /// Loads the ouput entities
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <param name="context"></param>
    /// <param name="type"></param>
    /// <param name="sqlSelect"></param>
    /// <returns></returns>
    public List<T> LoadOutputEntities<T>(DbContext context, Type type, string sqlSelect) where T : class
    {
        List<T> existingEntities;
        if (typeof(T) == type)
        {
            Expression<Func<DbContext, IQueryable<T>>> expression = GetQueryExpression<T>(sqlSelect, false);
            var compiled = EF.CompileQuery(expression); // instead using Compiled queries
            existingEntities = compiled(context).ToList();
        }
        else // TODO: Consider removing
        {
            Expression<Func<DbContext, IEnumerable>> expression = GetQueryExpression(type, sqlSelect, false);
            var compiled = EF.CompileQuery(expression); // instead using Compiled queries
            existingEntities = compiled(context).Cast<T>().ToList();
        }
        return existingEntities;
    }

    /// <summary>
    /// Updates the entities' identity field
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <param name="context"></param>
    /// <param name="tableInfo"></param>
    /// <param name="entities"></param>
    /// <param name="entitiesWithOutputIdentity"></param>
    public void UpdateEntitiesIdentity<T>(DbContext context, TableInfo tableInfo, IEnumerable<T> entities, IEnumerable<object> entitiesWithOutputIdentity)
    {
        string? identifierPropertyName = null;
        if (IdentityColumnName != null)
        {
            identifierPropertyName = OutputPropertyColumnNamesDict.SingleOrDefault(a => a.Value == IdentityColumnName).Key; // is Identity autoincrement 
        }
        else if (PrimaryKeysPropertyColumnNameDict.Count() == 1 && 
                 DefaultValueProperties.Contains(PrimaryKeysPropertyColumnNameDict.FirstOrDefault().Key))                   // or PK with default sql value
        {
            identifierPropertyName = PrimaryKeysPropertyColumnNameDict.FirstOrDefault().Key;
        }

        if (identifierPropertyName != null)
        {
            var fastProperty = FastPropertyDict[identifierPropertyName];

            if (fastProperty.Property.PropertyType == typeof(string) ||
                fastProperty.Property.PropertyType == typeof(Guid))
            {
                entities = entities.OrderBy(p => fastProperty.Property!.GetValue(p, null)).ToList();
                entitiesWithOutputIdentity = entitiesWithOutputIdentity.OrderBy(p => fastProperty.Property!.GetValue(p, null)).ToList();
            }
        }

        if (BulkConfig.PreserveInsertOrder) // Updates Db changed Columns in entityList
        {
            int countDiff = entities.Count() - entitiesWithOutputIdentity.Count();
            if (countDiff > 0) // When some ommited from Merge because of TimeStamp conflict then changes are not loaded but output is set in TimeStampInfo
            {
                tableInfo.BulkConfig.TimeStampInfo = new TimeStampInfo
                {
                    NumberOfSkippedForUpdate = countDiff,
                    EntitiesOutput = entitiesWithOutputIdentity.Cast<object>().ToList()
                };
                return;
            }

            if (tableInfo.EntitiesSortedReference != null)
            {
                entities = tableInfo.EntitiesSortedReference.Cast<T>().ToList();
            }

            var entitiesDict = new Dictionary<object, T>();
            var numberOfOutputEntities = Math.Min(NumberOfEntities, entitiesWithOutputIdentity.Count());
            for (int i = 0; i < numberOfOutputEntities; i++)
            {
                if (identifierPropertyName != null)
                {
                    var customPK = tableInfo.PrimaryKeysPropertyColumnNameDict.Keys;
                    if (!(customPK.Count == 1 && customPK.First() == identifierPropertyName) &&
                        (tableInfo.BulkConfig.OperationType == OperationType.Update ||
                         tableInfo.BulkConfig.OperationType == OperationType.InsertOrUpdate ||
                         tableInfo.BulkConfig.OperationType == OperationType.InsertOrUpdateOrDelete)
                       ) // (UpsertOrderTest) fix for BulkInsertOrUpdate assigns wrong output IDs when PreserveInsertOrder = true and SetOutputIdentity = true
                    {
                        if (entitiesDict.Count == 0)
                        {
                            foreach (var entity in entities)
                            {
                                PrimaryKeysPropertyColumnNameValues customPKValue = new(customPK.Select(c => FastPropertyDict[c].Get(entity!)));
                                entitiesDict.Add(customPKValue, entity);
                            }
                        }
                        var identityPropertyValue = FastPropertyDict[identifierPropertyName].Get(entitiesWithOutputIdentity.ElementAt(i));
                        PrimaryKeysPropertyColumnNameValues customPKOutputValue = new(customPK.Select(c => FastPropertyDict[c].Get(entitiesWithOutputIdentity.ElementAt(i))));
                        FastPropertyDict[identifierPropertyName].Set(entitiesDict[customPKOutputValue]!, identityPropertyValue);
                    }
                    else
                    {
                        bool outputIdentityOnly = BulkConfig.SetOutputNonIdentityColumns == false && IdentityColumnName != null;
                        var element = entitiesWithOutputIdentity.ElementAt(i);
                        var identityPropertyValue = outputIdentityOnly ? element
                                                                       : FastPropertyDict[identifierPropertyName].Get(element);
                        FastPropertyDict[identifierPropertyName].Set(entities.ElementAt(i)!, identityPropertyValue);
                    }
                }

                if (TimeStampColumnName != null) // timestamp/rowversion is also generated by the SqlServer so if exist should be updated as well
                {
                    string timeStampPropertyName = OutputPropertyColumnNamesDict.SingleOrDefault(a => a.Value == TimeStampColumnName).Key;
                    var timeStampPropertyValue = FastPropertyDict[timeStampPropertyName].Get(entitiesWithOutputIdentity.ElementAt(i));
                    FastPropertyDict[timeStampPropertyName].Set(entities.ElementAt(i)!, timeStampPropertyValue);
                }

                var outputProperties = tableInfo.OutputPropertyColumnNamesDict.Keys;
                var propertiesToLoad = outputProperties.Where(a => a != identifierPropertyName &&
                                                                   a != TimeStampColumnName &&                          // already loaded in segmet above
                                                                   !tableInfo.BulkConfig.TemporalColumns.Contains(a) && // temporal columns not accessible as direct property
                                                                   (tableInfo.DefaultValueProperties.Contains(a) ||     // add Computed and DefaultValues
                                                                   !tableInfo.PropertyColumnNamesDict.ContainsKey(a))); // remove others since already have same have (could be omited)
                foreach (var outputPropertyName in propertiesToLoad)
                {
                    var propertyValue = FastPropertyDict[outputPropertyName].Get(entitiesWithOutputIdentity.ElementAt(i));
                    FastPropertyDict[outputPropertyName].Set(entities.ElementAt(i)!, propertyValue);
                }
            }
        }
        else // Clears entityList and then refills it with loaded entites from Db
        {
            ((List<T>)entities).Clear();

            if (typeof(T) == entitiesWithOutputIdentity.FirstOrDefault()?.GetType())
            {
                ((List<T>)entities).AddRange(entitiesWithOutputIdentity.Cast<T>().ToList());
            }
            else
            {
                var entitiesObjects = entities.Cast<object>().ToList();
                entitiesObjects.AddRange(entitiesWithOutputIdentity);
            }
        }
    }

    // Compiled queries created manually to avoid EF Memory leak bug when using EF with dynamic SQL:
    // https://github.com/borisdj/EFCore.BulkExtensions/issues/73
    // Once the following Issue gets fixed(expected in EF 3.0) this can be replaced with code segment: DirectQuery
    // https://github.com/aspnet/EntityFrameworkCore/issues/12905
    #region CompiledQuery
    /// <summary>
    /// Loads the output data
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <param name="context"></param>
    /// <param name="type"></param>
    /// <param name="entities"></param>
    /// <param name="tableInfo"></param>
    /// <param name="cancellationToken"></param>
    /// <param name="isAsync"></param>
    /// <returns></returns>
    public async Task LoadOutputDataAsync<T>(DbContext context, Type type, IEnumerable<T> entities, TableInfo tableInfo, bool isAsync, CancellationToken cancellationToken) where T : class
    {
        bool hasIdentity = OutputPropertyColumnNamesDict.Any(a => a.Value == IdentityColumnName) ||
                           (tableInfo.HasSinglePrimaryKey && tableInfo.DefaultValueProperties.Contains(tableInfo.PrimaryKeysPropertyColumnNameDict.FirstOrDefault().Key));
        int totalNumber = entities.Count();
        if (BulkConfig.SetOutputIdentity && (hasIdentity || tableInfo.TimeStampColumnName == null))
        {
            var databaseType = SqlAdaptersMapping.GetDatabaseType();
            string sqlQuery = SqlAdaptersMapping.DbServer.QueryBuilder.SelectFromOutputTable(this);
            //var entitiesWithOutputIdentity = await QueryOutputTableAsync<T>(context, sqlQuery).ToListAsync(cancellationToken).ConfigureAwait(false); // TempFIX
            var entitiesWithOutputIdentity = QueryOutputTable(context, type, sqlQuery).Cast<object>().ToList();
            //var entitiesWithOutputIdentity = (typeof(T) == type) ? QueryOutputTable<object>(context, sqlQuery).ToList() : QueryOutputTable(context, type, sqlQuery).Cast<object>().ToList();

            //var entitiesObjects = entities.Cast<object>().ToList();
            UpdateEntitiesIdentity(context, tableInfo, entities, entitiesWithOutputIdentity);
            totalNumber = entitiesWithOutputIdentity.Count;
        }
        if (BulkConfig.CalculateStats)
        {
            int[] statsNumbers;
            if (isAsync)
            {
                statsNumbers = await GetStatsNumbersAsync(context, isAsync: true, cancellationToken).ConfigureAwait(false);
            }
            else
            {
                statsNumbers = GetStatsNumbersAsync(context, isAsync: false, cancellationToken).GetAwaiter().GetResult();
            }
            BulkConfig.StatsInfo = new StatsInfo
            {
                StatsNumberInserted = statsNumbers[0],
                StatsNumberUpdated = statsNumbers[1],
                StatsNumberDeleted = statsNumbers[2],
            };
        }
    }

    /// <summary>
    /// Queries the output table data
    /// </summary>
    /// <param name="context"></param>
    /// <param name="type"></param>
    /// <param name="sqlQuery"></param>
    /// <returns></returns>
    protected IEnumerable QueryOutputTable(DbContext context, Type type, string sqlQuery)
    {
        bool doSelect = BulkConfig.SetOutputIdentity && BulkConfig.SetOutputNonIdentityColumns == false && IdentityColumnName != null;

        var compiled = EF.CompileQuery(GetQueryExpression(type, sqlQuery, true, doSelect));
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

    /// <summary>
    /// Returns an expression for the SQL query
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <param name="sqlQuery"></param>
    /// <param name="ordered"></param>
    /// <returns></returns>
    public Expression<Func<DbContext, IQueryable<T>>> GetQueryExpression<T>(string sqlQuery, bool ordered = true) where T : class
    {
        Expression<Func<DbContext, IQueryable<T>>>? expression = null;
        if (BulkConfig.TrackingEntities) // If Else can not be replaced with Ternary operator for Expression
        {
            expression = BulkConfig.IgnoreGlobalQueryFilters ?
                (ctx) => ctx.Set<T>().FromSqlRaw(sqlQuery).IgnoreQueryFilters() :
                (ctx) => ctx.Set<T>().FromSqlRaw(sqlQuery);
        }
        else
        {
            expression = BulkConfig.IgnoreGlobalQueryFilters ?
                (ctx) => ctx.Set<T>().FromSqlRaw(sqlQuery).AsNoTracking().IgnoreQueryFilters() :
                (ctx) => ctx.Set<T>().FromSqlRaw(sqlQuery).AsNoTracking();
        }
        return ordered ?
            Expression.Lambda<Func<DbContext, IQueryable<T>>>(OrderBy(typeof(T), expression.Body, PrimaryKeysPropertyColumnNameDict.Select(a => a.Key).ToList()), expression.Parameters) :
            expression;

        // ALTERNATIVELY OrderBy with DynamicLinq ('using System.Linq.Dynamic.Core;' NuGet required) that eliminates need for custom OrderBy<T> method with Expression.
        //var queryOrdered = query.OrderBy(PrimaryKeys[0]);
    }

    /// <summary>
    /// Returns an expression for the SQL query
    /// </summary>
    /// <param name="entityType"></param>
    /// <param name="sqlQuery"></param>
    /// <param name="doOrder"></param>
    /// <param name="doSelect"></param>
    /// <returns></returns>
    public Expression<Func<DbContext, IEnumerable>> GetQueryExpression(Type entityType, string sqlQuery, bool doOrder = true, bool doSelect = false)
    {
        var parameter = Expression.Parameter(typeof(DbContext), "ctx");
        var expression = Expression.Call(parameter, "Set", new Type[] { entityType });
        expression = Expression.Call(typeof(RelationalQueryableExtensions), "FromSqlRaw", new Type[] { entityType }, expression, Expression.Constant(sqlQuery), Expression.Constant(Array.Empty<object>()));

        if (!BulkConfig.TrackingEntities) // If Else can not be replaced with Ternary operator for Expression
        {
            expression = Expression.Call(typeof(EntityFrameworkQueryableExtensions), "AsNoTracking", new Type[] { entityType }, expression);
        }

        if (BulkConfig.IgnoreGlobalQueryFilters)
        {
            expression = Expression.Call(typeof(EntityFrameworkQueryableExtensions), "IgnoreQueryFilters", new Type[] { entityType }, expression);
        }

        if (doOrder)
        {
            var primaryKeys = PrimaryKeysPropertyColumnNameDict.Select(a => a.Key).ToList();
            expression = OrderBy(entityType, expression, primaryKeys);
        }

        if (doSelect)
        {
            var identityPropName = PropertyColumnNamesDict.Where(a => a.Value == IdentityColumnName).Select(a => a.Key).ToList();
            expression = Select(entityType, expression, identityPropName);
        }

        var expressionResult = Expression.Lambda<Func<DbContext, IEnumerable>>(expression, parameter);

        return expressionResult;

        // ALTERNATIVELY OrderBy with DynamicLinq ('using System.Linq.Dynamic.Core;' NuGet required) that eliminates need for custom OrderBy<T> method with Expression.
        //var queryOrdered = query.OrderBy(PrimaryKeys[0]);
    }

    private static MethodCallExpression OrderBy(Type entityType, Expression source, List<string> orderings)
    {
        var expression = (MethodCallExpression)source;
        ParameterExpression parameter = Expression.Parameter(entityType);
        bool firstArgOrderBy = true;
        foreach (var ordering in orderings)
        {
            PropertyInfo? property = GetPropertyUnambiguous(entityType, ordering);
            if (property != null)
            {
                MemberExpression propertyAccess = Expression.MakeMemberAccess(parameter, property);
                LambdaExpression orderByExp = Expression.Lambda(propertyAccess, parameter);
                string methodName = firstArgOrderBy ? "OrderBy" : "ThenBy";
                expression = Expression.Call(typeof(Queryable), methodName, new Type[] { entityType, property.PropertyType }, expression, Expression.Quote(orderByExp));
                firstArgOrderBy = false;
            }
        }
        return expression;
    }

    private static MethodCallExpression Select(Type entityType, Expression source, List<string> selectProps)
    {
        var expression = (MethodCallExpression)source;
        ParameterExpression parameter = Expression.Parameter(entityType);
        //foreach (var selectProp in selectProps)
        {
            PropertyInfo? property = GetPropertyUnambiguous(entityType, selectProps[0]); // currently supports Select only 1 property, first in list, that is Identity
            if (property != null)
            {
                MemberExpression propertyAccess = Expression.MakeMemberAccess(parameter, property);
                LambdaExpression selectExp = Expression.Lambda(propertyAccess, parameter);
                string methodName = "Select";
                var typeArgs = new Type[] { entityType, property.PropertyType };
                expression = Expression.Call(typeof(Queryable), methodName, typeArgs, expression, Expression.Quote(selectExp));
            }
        }
        return expression;
    }
    #endregion

    // Currently not used until issue from previous segment is fixed in EFCore
    #region DirectQuery
    /*public void UpdateOutputIdentity<T>(DbContext context, IEnumerable<T> entities) where T : class
    {
        if (HasSinglePrimaryKey)
        {
            var entitiesWithOutputIdentity = QueryOutputTable<T>(context).ToList();
            UpdateEntitiesIdentity(entities, entitiesWithOutputIdentity);
        }
    }

    public async Task UpdateOutputIdentityAsync<T>(DbContext context, IEnumerable<T> entities) where T : class
    {
        if (HasSinglePrimaryKey)
        {
            var entitiesWithOutputIdentity = await QueryOutputTable<T>(context).ToListAsync().ConfigureAwait(false);
            UpdateEntitiesIdentity(entities, entitiesWithOutputIdentity);
        }
    }

    protected IQueryable<T> QueryOutputTable<T>(DbContext context) where T : class
    {
        string q = SqlQueryBuilderBase.SelectFromOutputTable(this);
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
        PropertyInfo property = GetPropertyUnambiguous(entityType, ordering);
        ParameterExpression parameter = Expression.Parameter(entityType);
        MemberExpression propertyAccess = Expression.MakeMemberAccess(parameter, property);
        LambdaExpression orderByExp = Expression.Lambda(propertyAccess, parameter);
        MethodCallExpression resultExp = Expression.Call(typeof(Queryable), "OrderBy", new Type[] { entityType, property.PropertyType }, source.Expression, Expression.Quote(orderByExp));
        var orderedQuery = source.Provider.CreateQuery<T>(resultExp);
        return orderedQuery;
    }*/
    #endregion
}

internal class PrimaryKeysPropertyColumnNameValues
{
    public List<object?> PkValues { get; }

    public PrimaryKeysPropertyColumnNameValues(IEnumerable<object?> pkValues)
    {
        PkValues = pkValues.ToList();
    }

    public override bool Equals(object? obj)
    {
        return obj is PrimaryKeysPropertyColumnNameValues values &&
               PkValues.SequenceEqual(values.PkValues);
    }

    public override int GetHashCode()
    {
        unchecked
        {
            int hash = 19;
            foreach (var value in PkValues)
            {
                hash = hash * 31 + (value == null ? 0 : value.GetHashCode());
            }
            return hash;
        }
    }
}


/// <summary>
/// Provides a list of information for EFCore.BulkExtensions that is used internally to know what to do with the data source received
/// Currently not yet used
/// </summary>
public class PropertyColumnInfo
{
#pragma warning disable CS1591 // No XML comments required here.
    public string PropertyName { get; set; } = null!;

    public string ColumnName { get; set; } = null!;

    public bool IsForOutput { get; set; }
}
