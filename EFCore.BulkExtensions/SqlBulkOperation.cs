using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;

namespace EFCore.BulkExtensions
{
    public enum OperationType
    {
        Insert,
        InsertOrUpdate,
        InsertOrUpdateDelete,
        Update,
        Delete,
        Read
    }

    internal static class SqlBulkOperation
    {
        internal static string ColumnMappingExceptionMessage => "The given ColumnMapping does not match up with any column in the source or destination";

        #region MainOps
        public static void Insert<T>(DbContext context, IList<T> entities, TableInfo tableInfo, Action<decimal> progress)
        {
            var sqlConnection = OpenAndGetSqlConnection(context, tableInfo.BulkConfig);
            var transaction = context.Database.CurrentTransaction;
            try
            {
                using (var sqlBulkCopy = GetSqlBulkCopy(sqlConnection, transaction, tableInfo.BulkConfig))
                {
                    bool useFastMember = tableInfo.HasOwnedTypes == false                      // With OwnedTypes DataTable is used since library FastMember can not (https://github.com/mgravell/fast-member/issues/21)
                                         && tableInfo.ColumnNameContainsSquareBracket == false // FastMember does not support escaped columnNames  ] -> ]]
                                         && tableInfo.ShadowProperties.Count == 0              // With Shadow prop. Discriminator (TPH inheritance) also not used because FastMember is slow for Update (https://github.com/borisdj/EFCore.BulkExtensions/pull/17)
                                         && !tableInfo.ConvertibleProperties.Any()             // With ConvertibleProperties FastMember is slow as well
                                         && !tableInfo.HasAbstractList                         // AbstractList not working with FastMember
                                         && !tableInfo.BulkConfig.UseOnlyDataTable;
                    bool setColumnMapping = useFastMember;
                    tableInfo.SetSqlBulkCopyConfig(sqlBulkCopy, entities, setColumnMapping, progress);
                    try
                    {
                        if (useFastMember)
                        {
                            using (var reader = ObjectReaderEx.Create(entities, tableInfo.ShadowProperties, tableInfo.ConvertibleProperties, context, tableInfo.PropertyColumnNamesDict.Keys.ToArray()))
                            {
                                sqlBulkCopy.WriteToServer(reader);
                            }
                        }
                        else
                        {
                            var dataTable = GetDataTable<T>(context, entities, sqlBulkCopy, tableInfo);
                            sqlBulkCopy.WriteToServer(dataTable);
                        }
                    }
                    catch (InvalidOperationException ex)
                    {
                        if (ex.Message.Contains(ColumnMappingExceptionMessage))
                        {
                            if (!tableInfo.CheckTableExist(context, tableInfo))
                            {
                                context.Database.ExecuteSqlCommand(SqlQueryBuilder.CreateTableCopy(tableInfo.FullTableName, tableInfo.FullTempTableName, tableInfo)); // Will throw Exception specify missing db column: Invalid column name ''
                                context.Database.ExecuteSqlCommand(SqlQueryBuilder.DropTable(tableInfo.FullTempTableName));
                            }
                        }
                        throw ex;
                    }
                }
            }
            finally
            {
                if (transaction == null)
                {
                    sqlConnection.Close();
                }
            }
        }

        public static async Task InsertAsync<T>(DbContext context, IList<T> entities, TableInfo tableInfo, Action<decimal> progress, CancellationToken cancellationToken = default(CancellationToken))
        {
            var sqlConnection = await OpenAndGetSqlConnectionAsync(context, tableInfo.BulkConfig, cancellationToken).ConfigureAwait(false);
            var transaction = context.Database.CurrentTransaction;
            try
            {
                using (var sqlBulkCopy = GetSqlBulkCopy(sqlConnection, transaction, tableInfo.BulkConfig))
                {
                    bool useFastMember = tableInfo.HasOwnedTypes == false
                                         && tableInfo.ColumnNameContainsSquareBracket == false
                                         && tableInfo.ShadowProperties.Count == 0
                                         && !tableInfo.ConvertibleProperties.Any()
                                         && !tableInfo.HasAbstractList
                                         && !tableInfo.BulkConfig.UseOnlyDataTable;
                    bool setColumnMapping = useFastMember;
                    tableInfo.SetSqlBulkCopyConfig(sqlBulkCopy, entities, setColumnMapping, progress);
                    try
                    {
                        if (useFastMember)
                        {
                            using (var reader = ObjectReaderEx.Create(entities, tableInfo.ShadowProperties, tableInfo.ConvertibleProperties, context, tableInfo.PropertyColumnNamesDict.Keys.ToArray()))
                            {
                                await sqlBulkCopy.WriteToServerAsync(reader, cancellationToken).ConfigureAwait(false);
                            }
                        }
                        else
                        {
                            var dataTable = GetDataTable<T>(context, entities, sqlBulkCopy, tableInfo);
                            await sqlBulkCopy.WriteToServerAsync(dataTable, cancellationToken).ConfigureAwait(false);
                        }
                    }
                    catch (InvalidOperationException ex)
                    {
                        if (ex.Message.Contains(ColumnMappingExceptionMessage))
                        {
                            if (!await tableInfo.CheckTableExistAsync(context, tableInfo, cancellationToken).ConfigureAwait(false))
                            {
                                await context.Database.ExecuteSqlCommandAsync(SqlQueryBuilder.CreateTableCopy(tableInfo.FullTableName, tableInfo.FullTempTableName, tableInfo), cancellationToken).ConfigureAwait(false);
                                await context.Database.ExecuteSqlCommandAsync(SqlQueryBuilder.DropTable(tableInfo.FullTempTableName), cancellationToken).ConfigureAwait(false);
                            }
                        }
                        throw ex;
                    }
                }
            }
            finally
            {
                if (transaction == null)
                {
                    sqlConnection.Close();
                }
            }
        }

        public static void Merge<T>(DbContext context, IList<T> entities, TableInfo tableInfo, OperationType operationType, Action<decimal> progress) where T : class
        {
            tableInfo.InsertToTempTable = true;
            tableInfo.CheckHasIdentity(context);

            context.Database.ExecuteSqlCommand(SqlQueryBuilder.CreateTableCopy(tableInfo.FullTableName, tableInfo.FullTempTableName, tableInfo));
            if (tableInfo.CreatedOutputTable)
            {
                context.Database.ExecuteSqlCommand(SqlQueryBuilder.CreateTableCopy(tableInfo.FullTableName, tableInfo.FullTempOutputTableName, tableInfo, true));
                if (tableInfo.TimeStampColumnName != null)
                {
                    context.Database.ExecuteSqlCommand(SqlQueryBuilder.AddColumn(tableInfo.FullTempOutputTableName, tableInfo.TimeStampColumnName, tableInfo.TimeStampOutColumnType));
                }
            }

            bool keepIdentity = tableInfo.BulkConfig.SqlBulkCopyOptions.HasFlag(SqlBulkCopyOptions.KeepIdentity);
            try
            {
                Insert(context, entities, tableInfo, progress);

                if (keepIdentity && tableInfo.HasIdentity)
                {
                    context.Database.OpenConnection();
                    context.Database.ExecuteSqlCommand(SqlQueryBuilder.SetIdentityInsert(tableInfo.FullTableName, true));
                }

                context.Database.ExecuteSqlCommand(SqlQueryBuilder.MergeTable(tableInfo, operationType));

                if (tableInfo.CreatedOutputTable)
                {
                    tableInfo.LoadOutputData(context, entities);
                }
            }
            finally
            {
                if (!tableInfo.BulkConfig.UseTempDB)
                {
                    if (tableInfo.CreatedOutputTable)
                    {
                        context.Database.ExecuteSqlCommand(SqlQueryBuilder.DropTable(tableInfo.FullTempOutputTableName));
                    }
                    context.Database.ExecuteSqlCommand(SqlQueryBuilder.DropTable(tableInfo.FullTempTableName));
                }

                if (keepIdentity && tableInfo.HasIdentity)
                {
                    context.Database.ExecuteSqlCommand(SqlQueryBuilder.SetIdentityInsert(tableInfo.FullTableName, false));
                    context.Database.CloseConnection();
                }
            }
        }

        public static async Task MergeAsync<T>(DbContext context, IList<T> entities, TableInfo tableInfo, OperationType operationType, Action<decimal> progress, CancellationToken cancellationToken = default(CancellationToken)) where T : class
        {
            tableInfo.InsertToTempTable = true;
            await tableInfo.CheckHasIdentityAsync(context, cancellationToken).ConfigureAwait(false);

            await context.Database.ExecuteSqlCommandAsync(SqlQueryBuilder.CreateTableCopy(tableInfo.FullTableName, tableInfo.FullTempTableName, tableInfo), cancellationToken).ConfigureAwait(false);
            if (tableInfo.CreatedOutputTable)
            {
                await context.Database.ExecuteSqlCommandAsync(SqlQueryBuilder.CreateTableCopy(tableInfo.FullTableName, tableInfo.FullTempOutputTableName, tableInfo, true), cancellationToken).ConfigureAwait(false);
                if (tableInfo.TimeStampColumnName != null)
                {
                    await context.Database.ExecuteSqlCommandAsync(SqlQueryBuilder.AddColumn(tableInfo.FullTempOutputTableName, tableInfo.TimeStampColumnName, tableInfo.TimeStampOutColumnType), cancellationToken).ConfigureAwait(false);
                }
            }

            bool keepIdentity = tableInfo.BulkConfig.SqlBulkCopyOptions.HasFlag(SqlBulkCopyOptions.KeepIdentity);
            try
            {
                await InsertAsync(context, entities, tableInfo, progress, cancellationToken).ConfigureAwait(false);

                if (keepIdentity && tableInfo.HasIdentity)
                {
                    await context.Database.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
                    await context.Database.ExecuteSqlCommandAsync(SqlQueryBuilder.SetIdentityInsert(tableInfo.FullTableName, true), cancellationToken).ConfigureAwait(false);
                }

                await context.Database.ExecuteSqlCommandAsync(SqlQueryBuilder.MergeTable(tableInfo, operationType), cancellationToken).ConfigureAwait(false);

                if (tableInfo.CreatedOutputTable)
                {
                    await tableInfo.LoadOutputDataAsync(context, entities, cancellationToken).ConfigureAwait(false);
                }
            }
            finally
            {
                if (!tableInfo.BulkConfig.UseTempDB)
                {
                    if (tableInfo.CreatedOutputTable)
                    {
                        await context.Database.ExecuteSqlCommandAsync(SqlQueryBuilder.DropTable(tableInfo.FullTempOutputTableName), cancellationToken).ConfigureAwait(false);
                    }
                    await context.Database.ExecuteSqlCommandAsync(SqlQueryBuilder.DropTable(tableInfo.FullTempTableName), cancellationToken).ConfigureAwait(false);
                }

                if (keepIdentity && tableInfo.HasIdentity)
                {
                    await context.Database.ExecuteSqlCommandAsync(SqlQueryBuilder.SetIdentityInsert(tableInfo.FullTableName, false), cancellationToken).ConfigureAwait(false);
                    context.Database.CloseConnection();
                }
            }
        }

        public static void Read<T>(DbContext context, IList<T> entities, TableInfo tableInfo, Action<decimal> progress) where T : class
        {
            Dictionary<string, string> previousPropertyColumnNamesDict = tableInfo.ConfigureBulkReadTableInfo(context);

            context.Database.ExecuteSqlCommand(SqlQueryBuilder.CreateTableCopy(tableInfo.FullTableName, tableInfo.FullTempTableName, tableInfo));

            try
            {
                Insert(context, entities, tableInfo, progress);

                tableInfo.PropertyColumnNamesDict = previousPropertyColumnNamesDict;

                var sqlQuery = SqlQueryBuilder.SelectJoinTable(tableInfo);

                //var existingEntities = context.Set<T>().FromSql(q).AsNoTracking().ToList(); // Not used because of EF Memory leak bug
                Expression<Func<DbContext, IQueryable<T>>> expression = null;
                if (tableInfo.BulkConfig.TrackingEntities)
                {
                    expression = (ctx) => ctx.Set<T>().FromSql(sqlQuery);
                }
                else
                {
                    expression = (ctx) => ctx.Set<T>().FromSql(sqlQuery).AsNoTracking();
                }

                var compiled = EF.CompileQuery(expression); // instead using Compiled queries
                var existingEntities = compiled(context).ToList();

                tableInfo.UpdateReadEntities(entities, existingEntities);
            }
            finally
            {
                if (!tableInfo.BulkConfig.UseTempDB)
                    context.Database.ExecuteSqlCommand(SqlQueryBuilder.DropTable(tableInfo.FullTempTableName));
            }
        }

        public static async Task ReadAsync<T>(DbContext context, IList<T> entities, TableInfo tableInfo, Action<decimal> progress, CancellationToken cancellationToken = default(CancellationToken)) where T : class
        {
            Dictionary<string, string> previousPropertyColumnNamesDict = tableInfo.ConfigureBulkReadTableInfo(context);

            await context.Database.ExecuteSqlCommandAsync(SqlQueryBuilder.CreateTableCopy(tableInfo.FullTableName, tableInfo.FullTempTableName, tableInfo), cancellationToken).ConfigureAwait(false);

            try
            {
                await InsertAsync(context, entities, tableInfo, progress, cancellationToken).ConfigureAwait(false);

                tableInfo.PropertyColumnNamesDict = previousPropertyColumnNamesDict;

                var sqlQuery = SqlQueryBuilder.SelectJoinTable(tableInfo);

                //var existingEntities = await context.Set<T>().FromSql(sqlQuery).ToListAsync(cancellationToken);
                Expression<Func<DbContext, IQueryable<T>>> expression = null;
                if (tableInfo.BulkConfig.TrackingEntities)
                {
                    expression = (ctx) => ctx.Set<T>().FromSql(sqlQuery);
                }
                else
                {
                    expression = (ctx) => ctx.Set<T>().FromSql(sqlQuery).AsNoTracking();
                }
                var compiled = EF.CompileAsyncQuery(expression);
                var existingEntities = (await compiled(context).ToListAsync(cancellationToken).ConfigureAwait(false));

                tableInfo.UpdateReadEntities(entities, existingEntities);
            }
            finally
            {
                if (!tableInfo.BulkConfig.UseTempDB)
                    await context.Database.ExecuteSqlCommandAsync(SqlQueryBuilder.DropTable(tableInfo.FullTempTableName), cancellationToken).ConfigureAwait(false);
            }
        }
        #endregion

        #region DataTable
        internal static DataTable GetDataTable<T>(DbContext context, IList<T> entities, SqlBulkCopy sqlBulkCopy, TableInfo tableInfo)
        {
            var dataTable = new DataTable();
            var columnsDict = new Dictionary<string, object>();
            var ownedEntitiesMappedProperties = new HashSet<string>();

            var type = tableInfo.HasAbstractList ? entities[0].GetType() : typeof(T);
            var entityType = context.Model.FindEntityType(type);
            var entityPropertiesDict = entityType.GetProperties().Where(a => tableInfo.PropertyColumnNamesDict.ContainsKey(a.Name)).ToDictionary(a => a.Name, a => a);
            var entityNavigationOwnedDict = entityType.GetNavigations().Where(a => a.GetTargetType().IsOwned()).ToDictionary(a => a.Name, a => a);
            var properties = type.GetProperties();
            var discriminatorColumn = tableInfo.ShadowProperties.Count == 0 ? null : tableInfo.ShadowProperties.ElementAt(0);

            foreach (var property in properties)
            {
                if (entityPropertiesDict.ContainsKey(property.Name))
                {
                    var relational = entityPropertiesDict[property.Name].Relational();
                    string columnName = relational.ColumnName;
                    var propertyType = Nullable.GetUnderlyingType(property.PropertyType) ?? property.PropertyType;

                    if (tableInfo.ConvertibleProperties.ContainsKey(columnName))
                    {
                        propertyType = tableInfo.ConvertibleProperties[columnName].ProviderClrType;
                    }

                    dataTable.Columns.Add(columnName, propertyType);
                    columnsDict.Add(property.Name, null);
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
                            string columnName = ownedEntityProperty.Relational().ColumnName;
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
                                string columnName = ownedEntityPropertyNameColumnNameDict[innerProperty.Name];
                                var ownedPropertyType = Nullable.GetUnderlyingType(innerProperty.PropertyType) ?? innerProperty.PropertyType;
                                dataTable.Columns.Add(columnName, ownedPropertyType);
                                columnsDict.Add(property.Name + "_" + innerProperty.Name, null);
                            }
                        }
                    }
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
                    var propertyValue = property.GetValue(entity, null);

                    if (entityPropertiesDict.ContainsKey(property.Name))
                    {
                        string columnName = entityPropertiesDict[property.Name].Relational().ColumnName;
                        if (tableInfo.ConvertibleProperties.ContainsKey(columnName))
                        {
                            propertyValue = tableInfo.ConvertibleProperties[columnName].ConvertToProvider.Invoke(propertyValue);
                        }
                    }

                    if (entityPropertiesDict.ContainsKey(property.Name))
                    {
                        columnsDict[property.Name] = propertyValue;
                    }
                    else if (entityNavigationOwnedDict.ContainsKey(property.Name) && !tableInfo.LoadOnlyPKColumn)
                    {
                        var ownedProperties = property.PropertyType.GetProperties().Where(a => ownedEntitiesMappedProperties.Contains(property.Name + "_" + a.Name));
                        foreach (var ownedProperty in ownedProperties)
                        {
                            columnsDict[property.Name + "_" + ownedProperty.Name] = propertyValue == null ? null : ownedProperty.GetValue(propertyValue, null);
                        }
                    }
                }
                var record = columnsDict.Values.ToArray();
                dataTable.Rows.Add(record);
            }

            foreach (DataColumn item in dataTable.Columns)  //Add mapping
            {
                sqlBulkCopy.ColumnMappings.Add(item.ColumnName, item.ColumnName);
            }
            return dataTable;
        }
        #endregion

        #region Connection
        internal static SqlConnection OpenAndGetSqlConnection(DbContext context, BulkConfig config)
        {
            var connection = context.GetUnderlyingConnection(config);
            if (connection.State != ConnectionState.Open)
            {
                connection.Open();
            }
            return (SqlConnection)connection;
        }

        internal static async Task<SqlConnection> OpenAndGetSqlConnectionAsync(DbContext context, BulkConfig config, CancellationToken cancellationToken = default(CancellationToken))
        {
            var connection = context.GetUnderlyingConnection(config);
            if (connection.State != ConnectionState.Open)
            {
                await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
            }
            return (SqlConnection)connection;
        }

        private static SqlBulkCopy GetSqlBulkCopy(SqlConnection sqlConnection, IDbContextTransaction transaction, BulkConfig config)
        {
            var sqlBulkCopyOptions = config.SqlBulkCopyOptions;
            if (transaction == null)
            {
                return new SqlBulkCopy(sqlConnection, sqlBulkCopyOptions, null);
            }
            else
            {
                var sqlTransaction = (SqlTransaction)transaction.GetUnderlyingTransaction(config);
                return new SqlBulkCopy(sqlConnection, sqlBulkCopyOptions, sqlTransaction);
            }
        }
        #endregion
    }
}
