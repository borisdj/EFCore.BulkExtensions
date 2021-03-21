using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using EFCore.BulkExtensions.SqlAdapters;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Storage;
using NetTopologySuite.Geometries;
using NetTopologySuite.IO;

namespace EFCore.BulkExtensions.SQLAdapters.SQLServer
{
    public class SqlOperationsServerAdapter: ISqlOperationsAdapter
    {
        internal static string ColumnMappingExceptionMessage => "The given ColumnMapping does not match up with any column in the source or destination";
        
        public void Insert<T>(DbContext context, Type type, IList<T> entities, TableInfo tableInfo, Action<decimal> progress)
        {
            tableInfo.CheckToSetIdentityForPreserveOrder(entities);
            var connection = OpenAndGetSqlConnection(context, tableInfo.BulkConfig);
            try
            {
                var transaction = context.Database.CurrentTransaction;

                // separate logic for System.Data.SqlClient and Microsoft.Data.SqlClient
                if (SqlClientHelper.IsSystemConnection(connection))
                {
                    using (var sqlBulkCopy = GetSqlBulkCopy((System.Data.SqlClient.SqlConnection)connection, transaction, tableInfo.BulkConfig))
                    {
                        bool setColumnMapping = false;
                        tableInfo.SetSqlBulkCopyConfig(sqlBulkCopy, entities, setColumnMapping, progress);
                        try
                        {
                            var dataTable = GetDataTable(context, type, entities, sqlBulkCopy, tableInfo);
                            sqlBulkCopy.WriteToServer(dataTable);
                        }
                        catch (InvalidOperationException ex)
                        {
                            if (ex.Message.Contains(ColumnMappingExceptionMessage))
                            {
                                if (!tableInfo.CheckTableExist(context, tableInfo))
                                {
                                    context.Database.ExecuteSqlRaw(SqlQueryBuilder.CreateTableCopy(tableInfo.FullTableName, tableInfo.FullTempTableName, tableInfo)); // Will throw Exception specify missing db column: Invalid column name ''
                                    context.Database.ExecuteSqlRaw(SqlQueryBuilder.DropTable(tableInfo.FullTempTableName, tableInfo.BulkConfig.UseTempDB));
                                }
                            }
                            throw;
                        }
                    }
                }
                else
                {
                    using (var sqlBulkCopy = GetSqlBulkCopy((Microsoft.Data.SqlClient.SqlConnection)connection, transaction, tableInfo.BulkConfig))
                    {
                        bool setColumnMapping = false;
                        tableInfo.SetSqlBulkCopyConfig(sqlBulkCopy, entities, setColumnMapping, progress);
                        try
                        {
                            var dataTable = GetDataTable(context, type, entities, sqlBulkCopy, tableInfo);
                            sqlBulkCopy.WriteToServer(dataTable);
                        }
                        catch (InvalidOperationException ex)
                        {
                            if (ex.Message.Contains(ColumnMappingExceptionMessage))
                            {
                                if (!tableInfo.CheckTableExist(context, tableInfo))
                                {
                                    context.Database.ExecuteSqlRaw(SqlQueryBuilder.CreateTableCopy(tableInfo.FullTableName, tableInfo.FullTempTableName, tableInfo)); // Will throw Exception specify missing db column: Invalid column name ''
                                    context.Database.ExecuteSqlRaw(SqlQueryBuilder.DropTable(tableInfo.FullTempTableName, tableInfo.BulkConfig.UseTempDB));
                                }
                            }
                            throw;
                        }
                    }
                }
            }
            finally
            {
                context.Database.CloseConnection();
            }
            if (!tableInfo.CreatedOutputTable)
            {
                tableInfo.CheckToSetIdentityForPreserveOrder(entities, reset: true);
            }
        }

        public async Task InsertAsync<T>(DbContext context, Type type, IList<T> entities, TableInfo tableInfo, Action<decimal> progress, CancellationToken cancellationToken)
        {
            tableInfo.CheckToSetIdentityForPreserveOrder(entities);
            var connection = await OpenAndGetSqlConnectionAsync(context, tableInfo.BulkConfig, cancellationToken).ConfigureAwait(false);
            try
            {
                var transaction = context.Database.CurrentTransaction;

                // separate logic for System.Data.SqlClient and Microsoft.Data.SqlClient
                if (SqlClientHelper.IsSystemConnection(connection))
                {
                    using (var sqlBulkCopy = GetSqlBulkCopy((System.Data.SqlClient.SqlConnection)connection, transaction, tableInfo.BulkConfig))
                    {
                        bool setColumnMapping = false;
                        tableInfo.SetSqlBulkCopyConfig(sqlBulkCopy, entities, setColumnMapping, progress);
                        try
                        {
                            var dataTable = GetDataTable(context, type, entities, sqlBulkCopy, tableInfo);
                            await sqlBulkCopy.WriteToServerAsync(dataTable, cancellationToken).ConfigureAwait(false);
                        }
                        catch (InvalidOperationException ex)
                        {
                            if (ex.Message.Contains(ColumnMappingExceptionMessage))
                            {
                                if (!await tableInfo.CheckTableExistAsync(context, tableInfo, cancellationToken).ConfigureAwait(false))
                                {
                                    await context.Database.ExecuteSqlRawAsync(SqlQueryBuilder.CreateTableCopy(tableInfo.FullTableName, tableInfo.FullTempTableName, tableInfo), cancellationToken).ConfigureAwait(false);
                                    await context.Database.ExecuteSqlRawAsync(SqlQueryBuilder.DropTable(tableInfo.FullTempTableName, tableInfo.BulkConfig.UseTempDB), cancellationToken).ConfigureAwait(false);
                                }
                            }
                            throw;
                        }
                    }
                }
                else
                {
                    using (var sqlBulkCopy = GetSqlBulkCopy((Microsoft.Data.SqlClient.SqlConnection)connection, transaction, tableInfo.BulkConfig))
                    {
                        bool setColumnMapping = false;
                        tableInfo.SetSqlBulkCopyConfig(sqlBulkCopy, entities, setColumnMapping, progress);
                        try
                        {
                            var dataTable = GetDataTable(context, type, entities, sqlBulkCopy, tableInfo);
                            await sqlBulkCopy.WriteToServerAsync(dataTable, cancellationToken).ConfigureAwait(false);
                        }
                        catch (InvalidOperationException ex)
                        {
                            if (ex.Message.Contains(ColumnMappingExceptionMessage))
                            {
                                if (!await tableInfo.CheckTableExistAsync(context, tableInfo, cancellationToken).ConfigureAwait(false))
                                {
                                    await context.Database.ExecuteSqlRawAsync(SqlQueryBuilder.CreateTableCopy(tableInfo.FullTableName, tableInfo.FullTempTableName, tableInfo), cancellationToken).ConfigureAwait(false);
                                    await context.Database.ExecuteSqlRawAsync(SqlQueryBuilder.DropTable(tableInfo.FullTempTableName, tableInfo.BulkConfig.UseTempDB), cancellationToken).ConfigureAwait(false);
                                }
                            }
                            throw;
                        }
                    }
                }
            }
            finally
            {
                await context.Database.CloseConnectionAsync().ConfigureAwait(false);
            }
            if (!tableInfo.CreatedOutputTable)
            {
                tableInfo.CheckToSetIdentityForPreserveOrder(entities, reset: true);
            }
        }

        public void Merge<T>(DbContext context, Type type, IList<T> entities, TableInfo tableInfo, OperationType operationType, Action<decimal> progress) where T : class
        {
            tableInfo.InsertToTempTable = true;

            var dropTempTableIfExists = tableInfo.BulkConfig.UseTempDB;

            if (dropTempTableIfExists)
            {
                context.Database.ExecuteSqlRaw(SqlQueryBuilder.DropTable(tableInfo.FullTempTableName, tableInfo.BulkConfig.UseTempDB));
            }

            context.Database.ExecuteSqlRaw(SqlQueryBuilder.CreateTableCopy(tableInfo.FullTableName, tableInfo.FullTempTableName, tableInfo));
            if (tableInfo.TimeStampColumnName != null)
            {
                context.Database.ExecuteSqlRaw(SqlQueryBuilder.AddColumn(tableInfo.FullTempTableName, tableInfo.TimeStampColumnName, tableInfo.TimeStampOutColumnType));
            }
            if (tableInfo.CreatedOutputTable)
            {
                context.Database.ExecuteSqlRaw(SqlQueryBuilder.CreateTableCopy(tableInfo.FullTableName, tableInfo.FullTempOutputTableName, tableInfo, true));
                if (tableInfo.TimeStampColumnName != null)
                {
                    context.Database.ExecuteSqlRaw(SqlQueryBuilder.AddColumn(tableInfo.FullTempOutputTableName, tableInfo.TimeStampColumnName, tableInfo.TimeStampOutColumnType));
                }
                context.Database.ExecuteSqlRaw(SqlQueryBuilder.AlterTableColumnsToNullable(tableInfo.FullTempOutputTableName, tableInfo));
            }

            bool keepIdentity = tableInfo.BulkConfig.SqlBulkCopyOptions.HasFlag(Microsoft.Data.SqlClient.SqlBulkCopyOptions.KeepIdentity);
            try
            {
                Insert(context, type, entities, tableInfo, progress);

                if (keepIdentity && tableInfo.HasIdentity)
                {
                    context.Database.OpenConnection();
                    context.Database.ExecuteSqlRaw(SqlQueryBuilder.SetIdentityInsert(tableInfo.FullTableName, true));
                }

                string sql = SqlQueryBuilder.MergeTable(tableInfo, operationType);
                context.Database.ExecuteSqlRaw(sql);

                if (tableInfo.CreatedOutputTable)
                {
                    tableInfo.LoadOutputData(context, type, entities, tableInfo);
                }
            }
            finally
            {
                if (!tableInfo.BulkConfig.UseTempDB)
                {
                    if (tableInfo.CreatedOutputTable)
                    {
                        context.Database.ExecuteSqlRaw(SqlQueryBuilder.DropTable(tableInfo.FullTempOutputTableName, tableInfo.BulkConfig.UseTempDB));
                    }
                    context.Database.ExecuteSqlRaw(SqlQueryBuilder.DropTable(tableInfo.FullTempTableName, tableInfo.BulkConfig.UseTempDB));
                }

                if (keepIdentity && tableInfo.HasIdentity)
                {
                    context.Database.ExecuteSqlRaw(SqlQueryBuilder.SetIdentityInsert(tableInfo.FullTableName, false));
                    context.Database.CloseConnection();
                }
            }
        }

        public async Task MergeAsync<T>(DbContext context, Type type, IList<T> entities, TableInfo tableInfo, OperationType operationType, Action<decimal> progress, CancellationToken cancellationToken) where T : class
        {
            tableInfo.InsertToTempTable = true;

            var dropTempTableIfExists = tableInfo.BulkConfig.UseTempDB;

            if (dropTempTableIfExists)
            {
                await context.Database.ExecuteSqlRawAsync(SqlQueryBuilder.DropTable(tableInfo.FullTempTableName, tableInfo.BulkConfig.UseTempDB), cancellationToken).ConfigureAwait(false);
            }

            await context.Database.ExecuteSqlRawAsync(SqlQueryBuilder.CreateTableCopy(tableInfo.FullTableName, tableInfo.FullTempTableName, tableInfo), cancellationToken).ConfigureAwait(false);
            if (tableInfo.CreatedOutputTable)
            {
                await context.Database.ExecuteSqlRawAsync(SqlQueryBuilder.CreateTableCopy(tableInfo.FullTableName, tableInfo.FullTempOutputTableName, tableInfo, true), cancellationToken).ConfigureAwait(false);
                if (tableInfo.TimeStampColumnName != null)
                {
                    await context.Database.ExecuteSqlRawAsync(SqlQueryBuilder.AddColumn(tableInfo.FullTempOutputTableName, tableInfo.TimeStampColumnName, tableInfo.TimeStampOutColumnType), cancellationToken).ConfigureAwait(false);
                }
                context.Database.ExecuteSqlRaw(SqlQueryBuilder.AlterTableColumnsToNullable(tableInfo.FullTempOutputTableName, tableInfo));
            }

            bool keepIdentity = tableInfo.BulkConfig.SqlBulkCopyOptions.HasFlag(Microsoft.Data.SqlClient.SqlBulkCopyOptions.KeepIdentity);
            try
            {
                await InsertAsync(context, type, entities, tableInfo, progress, cancellationToken).ConfigureAwait(false);

                if (keepIdentity && tableInfo.HasIdentity)
                {
                    await context.Database.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
                    await context.Database.ExecuteSqlRawAsync(SqlQueryBuilder.SetIdentityInsert(tableInfo.FullTableName, true), cancellationToken).ConfigureAwait(false);
                }

                await context.Database.ExecuteSqlRawAsync(SqlQueryBuilder.MergeTable(tableInfo, operationType), cancellationToken).ConfigureAwait(false);

                if (tableInfo.CreatedOutputTable)
                {
                    await tableInfo.LoadOutputDataAsync(context, type, entities, tableInfo, cancellationToken).ConfigureAwait(false);
                }
            }
            finally
            {
                if (!tableInfo.BulkConfig.UseTempDB)
                {
                    if (tableInfo.CreatedOutputTable)
                    {
                        await context.Database.ExecuteSqlRawAsync(SqlQueryBuilder.DropTable(tableInfo.FullTempOutputTableName, tableInfo.BulkConfig.UseTempDB), cancellationToken).ConfigureAwait(false);
                    }
                    await context.Database.ExecuteSqlRawAsync(SqlQueryBuilder.DropTable(tableInfo.FullTempTableName, tableInfo.BulkConfig.UseTempDB), cancellationToken).ConfigureAwait(false);
                }

                if (keepIdentity && tableInfo.HasIdentity)
                {
                    await context.Database.ExecuteSqlRawAsync(SqlQueryBuilder.SetIdentityInsert(tableInfo.FullTableName, false), cancellationToken).ConfigureAwait(false);
                    context.Database.CloseConnection();
                }
            }
        }

        public void Read<T>(DbContext context, Type type, IList<T> entities, TableInfo tableInfo, Action<decimal> progress) where T : class
        {
            Dictionary<string, string> previousPropertyColumnNamesDict = tableInfo.ConfigureBulkReadTableInfo(context);

            string sql = SqlQueryBuilder.CreateTableCopy(tableInfo.FullTableName, tableInfo.FullTempTableName, tableInfo);
            context.Database.ExecuteSqlRaw(sql);
            try
            {
                Insert(context, type, entities, tableInfo, progress);

                tableInfo.PropertyColumnNamesDict = tableInfo.OutputPropertyColumnNamesDict;

                var sqlQuery = SqlQueryBuilder.SelectJoinTable(tableInfo);

                tableInfo.PropertyColumnNamesDict = previousPropertyColumnNamesDict;

                List<T> existingEntities;
                if (typeof(T) == type)
                {
                    Expression<Func<DbContext, IQueryable<T>>> expression = tableInfo.GetQueryExpression<T>(sqlQuery, false);
                    var compiled = EF.CompileQuery(expression); // instead using Compiled queries
                    existingEntities = compiled(context).ToList();
                }
                else
                {
                    Expression<Func<DbContext, IEnumerable>> expression = tableInfo.GetQueryExpression(type, sqlQuery, false);
                    var compiled = EF.CompileQuery(expression); // instead using Compiled queries
                    existingEntities = compiled(context).Cast<T>().ToList();
                }

                tableInfo.UpdateReadEntities(type, entities, existingEntities);
            }
            finally
            {
                if (!tableInfo.BulkConfig.UseTempDB)
                    context.Database.ExecuteSqlRaw(SqlQueryBuilder.DropTable(tableInfo.FullTempTableName, tableInfo.BulkConfig.UseTempDB));
            }
        }

        public async Task ReadAsync<T>(DbContext context, Type type, IList<T> entities, TableInfo tableInfo, Action<decimal> progress, CancellationToken cancellationToken) where T : class
        {
            Dictionary<string, string> previousPropertyColumnNamesDict = tableInfo.ConfigureBulkReadTableInfo(context);
            
            await context.Database.ExecuteSqlRawAsync(SqlQueryBuilder.CreateTableCopy(tableInfo.FullTableName, tableInfo.FullTempTableName, tableInfo), cancellationToken).ConfigureAwait(false);
            try
            {
                await InsertAsync(context, type, entities, tableInfo, progress, cancellationToken).ConfigureAwait(false);

                tableInfo.PropertyColumnNamesDict = tableInfo.OutputPropertyColumnNamesDict;

                var sqlQuery = SqlQueryBuilder.SelectJoinTable(tableInfo);

                tableInfo.PropertyColumnNamesDict = previousPropertyColumnNamesDict;

                List<T> existingEntities;
                if (typeof(T) == type)
                {
                    Expression<Func<DbContext, IQueryable<T>>> expression = tableInfo.GetQueryExpression<T>(sqlQuery, false);
                    var compiled = EF.CompileQuery(expression); // instead using Compiled queries
                    existingEntities = compiled(context).ToList();
                }
                else
                {
                    Expression<Func<DbContext, IEnumerable>> expression = tableInfo.GetQueryExpression(type, sqlQuery, false);
                    var compiled = EF.CompileQuery(expression); // instead using Compiled queries
                    existingEntities = compiled(context).Cast<T>().ToList();
                }

                tableInfo.UpdateReadEntities(type, entities, existingEntities);
            }
            finally
            {
                if (!tableInfo.BulkConfig.UseTempDB)
                    await context.Database.ExecuteSqlRawAsync(SqlQueryBuilder.DropTable(tableInfo.FullTempTableName, tableInfo.BulkConfig.UseTempDB), cancellationToken).ConfigureAwait(false);
            }
        }

        public void Truncate(DbContext context, TableInfo tableInfo)
        {
            context.Database.ExecuteSqlRaw(SqlQueryBuilder.TruncateTable(tableInfo.FullTableName));
        }

        public async Task TruncateAsync(DbContext context, TableInfo tableInfo)
        {
            await context.Database.ExecuteSqlRawAsync(SqlQueryBuilder.TruncateTable(tableInfo.FullTableName));
        }
        
        #region Connection
        internal static DbConnection OpenAndGetSqlConnection(DbContext context, BulkConfig config)
        {
            context.Database.OpenConnection();

            return context.GetUnderlyingConnection(config);
        }

        internal static async Task<DbConnection> OpenAndGetSqlConnectionAsync(DbContext context, BulkConfig config, CancellationToken cancellationToken)
        {
            await context.Database.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);

            return context.GetUnderlyingConnection(config);
        }
        
        private static Microsoft.Data.SqlClient.SqlBulkCopy GetSqlBulkCopy(Microsoft.Data.SqlClient.SqlConnection sqlConnection, IDbContextTransaction transaction, BulkConfig config)
        {
            var sqlBulkCopyOptions = config.SqlBulkCopyOptions;
            if (transaction == null)
            {
                return new Microsoft.Data.SqlClient.SqlBulkCopy(sqlConnection, sqlBulkCopyOptions, null);
            }
            else
            {
                var sqlTransaction = (Microsoft.Data.SqlClient.SqlTransaction)transaction.GetUnderlyingTransaction(config);
                return new Microsoft.Data.SqlClient.SqlBulkCopy(sqlConnection, sqlBulkCopyOptions, sqlTransaction);
            }
        }

        private static System.Data.SqlClient.SqlBulkCopy GetSqlBulkCopy(System.Data.SqlClient.SqlConnection sqlConnection, IDbContextTransaction transaction, BulkConfig config)
        {
            var sqlBulkCopyOptions = (System.Data.SqlClient.SqlBulkCopyOptions)config.SqlBulkCopyOptions;
            if (transaction == null)
            {
                return new System.Data.SqlClient.SqlBulkCopy(sqlConnection, sqlBulkCopyOptions, null);
            }
            
            var sqlTransaction = (System.Data.SqlClient.SqlTransaction)transaction.GetUnderlyingTransaction(config);
            return new System.Data.SqlClient.SqlBulkCopy(sqlConnection, sqlBulkCopyOptions, sqlTransaction);
        }
        #endregion
        
        #region DataTable
        /// <summary>
        /// Supports <see cref="Microsoft.Data.SqlClient.SqlBulkCopy"/>
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="context"></param>
        /// <param name="type"></param>
        /// <param name="entities"></param>
        /// <param name="sqlBulkCopy"></param>
        /// <param name="tableInfo"></param>
        /// <returns></returns>
        internal static DataTable GetDataTable<T>(DbContext context, Type type, IList<T> entities, Microsoft.Data.SqlClient.SqlBulkCopy sqlBulkCopy, TableInfo tableInfo)
        {
            DataTable dataTable = InnerGetDataTable(context, ref type, entities, tableInfo);

            foreach (DataColumn item in dataTable.Columns)  //Add mapping
            {
                sqlBulkCopy.ColumnMappings.Add(item.ColumnName, item.ColumnName);
            }
            return dataTable;
        }

        /// <summary>
        /// Supports <see cref="System.Data.SqlClient.SqlBulkCopy"/>
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="context"></param>
        /// <param name="type"></param>
        /// <param name="entities"></param>
        /// <param name="sqlBulkCopy"></param>
        /// <param name="tableInfo"></param>
        /// <returns></returns>
        internal static DataTable GetDataTable<T>(DbContext context, Type type, IList<T> entities, System.Data.SqlClient.SqlBulkCopy sqlBulkCopy, TableInfo tableInfo)
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
            var sqlServerBytesWriter = new SqlServerBytesWriter
            {
                IsGeography = true
            };

            type = tableInfo.HasAbstractList ? entities[0].GetType() : type;
            var entityType = context.Model.FindEntityType(type);
            var entityTypeProperties = entityType.GetProperties();
            var entityPropertiesDict = entityTypeProperties.Where(a => tableInfo.PropertyColumnNamesDict.ContainsKey(a.Name) || a.Name == tableInfo.TimeStampPropertyName).ToDictionary(a => a.Name, a => a);
            var entityNavigationOwnedDict = entityType.GetNavigations().Where(a => a.GetTargetType().IsOwned()).ToDictionary(a => a.Name, a => a);
            var entityShadowFkPropertiesDict = entityTypeProperties.Where(a => 
                a.IsShadowProperty() && 
                a.IsForeignKey() &&
                a.GetContainingForeignKeys().FirstOrDefault()?.DependentToPrincipal?.Name != null)
                .ToDictionary(x => x.Name, a => a);

            var properties = type.GetProperties(BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic);
            var discriminatorColumn = GetDiscriminatorColumn(tableInfo);

            foreach (var property in properties)
            {
                if (entityPropertiesDict.ContainsKey(property.Name))
                {
                    var propertyEntityType = entityPropertiesDict[property.Name];
                    string columnName = propertyEntityType.GetColumnName();

                    var isConvertible = tableInfo.ConvertibleProperties.ContainsKey(columnName);
                    var propertyType = isConvertible ? tableInfo.ConvertibleProperties[columnName].ProviderClrType : property.PropertyType;

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

                    if (!columnsDict.ContainsKey(property.Name))
                    {
                        dataTable.Columns.Add(columnName, propertyType);
                        columnsDict.Add(property.Name, null);
                    }
                }
                else if (entityShadowFkPropertiesDict.ContainsKey(property.Name))
                {
                    var fk = entityShadowFkPropertiesDict[property.Name];
                    entityPropertiesDict.TryGetValue(fk.GetColumnName(), out var entityProperty);
                    if (entityProperty == null) // BulkRead
                        continue;

                    var columnName = entityProperty.GetColumnName();
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

                    if (!columnsDict.ContainsKey(property.Name))
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
                            string columnName = ownedEntityProperty.GetColumnName();
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

                                if (tableInfo.ConvertibleProperties.ContainsKey(propertyName))
                                {
                                    var convertor = tableInfo.ConvertibleProperties[propertyName];
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
                foreach (var sp in entityPropertiesDict.Values.Where(y => y.IsShadowProperty()))
                {
                    var columnName = sp.GetColumnName();

                    // If a model has an entity which has a relationship without an explicity defined FK, the data table will already contain the foreign key shadow property
                    if (dataTable.Columns.Contains(columnName))
                        continue;
                    
                    var isConvertible = tableInfo.ConvertibleProperties.ContainsKey(columnName);
                    var propertyType = isConvertible ? tableInfo.ConvertibleProperties[columnName].ProviderClrType : sp.ClrType;

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
                    columnsDict.Add(sp.Name, null);
                }
            }

            if (discriminatorColumn != null)
            {
                dataTable.Columns.Add(discriminatorColumn, typeof(string));
                columnsDict.Add(discriminatorColumn, type.Name);
            }

            foreach (var entity in entities)
            {
                foreach (var property in properties)
                {
                    var propertyValue = tableInfo.FastPropertyDict.ContainsKey(property.Name) ? tableInfo.FastPropertyDict[property.Name].Get(entity) : null;

                    if (entityPropertiesDict.ContainsKey(property.Name))
                    {
                        string columnName = entityPropertiesDict[property.Name].GetColumnName();
                        if (tableInfo.ConvertibleProperties.ContainsKey(columnName))
                        {
                            propertyValue = tableInfo.ConvertibleProperties[columnName].ConvertToProvider.Invoke(propertyValue);
                        }
                    }

                    if (propertyValue is Geometry geometryValue && isSqlServer)
                    {
                        geometryValue.SRID = tableInfo.BulkConfig.SRID;
                        propertyValue = sqlServerBytesWriter.Write(geometryValue);
                    }

                    if (entityPropertiesDict.ContainsKey(property.Name))
                    {
                        columnsDict[property.Name] = propertyValue;
                    }
                    else if (entityShadowFkPropertiesDict.ContainsKey(property.Name))
                    {
                        var fk = entityShadowFkPropertiesDict[property.Name];
                        var columnName = fk.GetColumnName();
                        entityPropertiesDict.TryGetValue(fk.GetColumnName(), out var entityProperty);
                        if (entityProperty == null) // BulkRead
                            continue;

                        columnsDict[columnName] = propertyValue == null ? null : fk.FindFirstPrincipal().PropertyInfo.GetValue(propertyValue);
                    }
                    else if (entityNavigationOwnedDict.ContainsKey(property.Name) && !tableInfo.LoadOnlyPKColumn)
                    {
                        var ownedProperties = property.PropertyType.GetProperties().Where(a => ownedEntitiesMappedProperties.Contains(property.Name + "_" + a.Name));
                        foreach (var ownedProperty in ownedProperties)
                        {
                            var columnName = $"{property.Name}_{ownedProperty.Name}";
                            var ownedPropertyValue = propertyValue == null ? null : tableInfo.FastPropertyDict[columnName].Get(propertyValue);

                            if (tableInfo.ConvertibleProperties.ContainsKey(columnName))
                            {
                                var converter = tableInfo.ConvertibleProperties[columnName];
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
                    foreach (var sp in entityPropertiesDict.Values.Where(y => y.IsShadowProperty()))
                    {
                        var propertyValue = context.Entry(entity).Property(sp.Name).CurrentValue;
                        var columnName = sp.GetColumnName();

                        if (tableInfo.ConvertibleProperties.ContainsKey(columnName))
                        {
                            propertyValue = tableInfo.ConvertibleProperties[columnName].ConvertToProvider.Invoke(propertyValue);
                        }

                        columnsDict[sp.Name] = propertyValue;
                    }
                }

                var record = columnsDict.Values.ToArray();
                dataTable.Rows.Add(record);
            }

            return dataTable;
        }

        private static string GetDiscriminatorColumn(TableInfo tableInfo)
        {
            string discriminatorColumn;

            if (!tableInfo.BulkConfig.EnableShadowProperties)
            {
                discriminatorColumn = tableInfo.ShadowProperties.Count == 0 ? null : tableInfo.ShadowProperties.ElementAt(0);
            }
            else
            {
                discriminatorColumn = null;
            }

            return discriminatorColumn;
        }

       #endregion
    }
}
