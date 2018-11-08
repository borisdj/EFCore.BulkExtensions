using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;
using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading.Tasks;

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
            var sqlConnection = OpenAndGetSqlConnection(context);
            var transaction = context.Database.CurrentTransaction;
            try
            {
                using (var sqlBulkCopy = GetSqlBulkCopy(sqlConnection, transaction, tableInfo.BulkConfig.SqlBulkCopyOptions))
                {
                    bool setColumnMapping = !tableInfo.HasOwnedTypes;
                    tableInfo.SetSqlBulkCopyConfig(sqlBulkCopy, entities, setColumnMapping, progress);
                    try
                    {
                        if (!tableInfo.HasOwnedTypes)
                        {
                            using (var reader = ObjectReaderEx.Create(entities, tableInfo.ShadowProperties, tableInfo.ConvertibleProperties, context, tableInfo.PropertyColumnNamesDict.Keys.ToArray()))
                            {
                                sqlBulkCopy.WriteToServer(reader);
                            }
                        }
                        else // With OwnedTypes DataTable is used since library FastMember can not (https://github.com/mgravell/fast-member/issues/21)
                        {
                            var dataTable = GetDataTable<T>(context, entities, sqlBulkCopy);
                            sqlBulkCopy.WriteToServer(dataTable);
                        }
                    }
                    catch (InvalidOperationException ex)
                    {
                        if (!ex.Message.Contains(ColumnMappingExceptionMessage))
                            throw ex;
                        context.Database.ExecuteSqlCommand(SqlQueryBuilder.CreateTableCopy(tableInfo.FullTableName, tableInfo.FullTempTableName, tableInfo)); // Exception specify missing db column: Invalid column name ''
                        if (!tableInfo.BulkConfig.UseTempDB)
                            context.Database.ExecuteSqlCommand(SqlQueryBuilder.DropTable(tableInfo.FullTempOutputTableName));
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

        public static async Task InsertAsync<T>(DbContext context, IList<T> entities, TableInfo tableInfo, Action<decimal> progress)
        {
            var sqlConnection = await OpenAndGetSqlConnectionAsync(context);
            var transaction = context.Database.CurrentTransaction;
            try
            {
                using (var sqlBulkCopy = GetSqlBulkCopy(sqlConnection, transaction, tableInfo.BulkConfig.SqlBulkCopyOptions))
                {
                    bool setColumnMapping = !tableInfo.HasOwnedTypes;
                    tableInfo.SetSqlBulkCopyConfig(sqlBulkCopy, entities, setColumnMapping, progress);
                    try
                    {
                        if (!tableInfo.HasOwnedTypes)
                        {
                            using (var reader = ObjectReaderEx.Create(entities, tableInfo.ShadowProperties, tableInfo.ConvertibleProperties, context, tableInfo.PropertyColumnNamesDict.Keys.ToArray()))
                            {
                                await sqlBulkCopy.WriteToServerAsync(reader).ConfigureAwait(false);
                            }
                        }
                        else
                        {
                            var dataTable = GetDataTable<T>(context, entities, sqlBulkCopy);
                            await sqlBulkCopy.WriteToServerAsync(dataTable);
                        }
                    }
                    catch (InvalidOperationException ex)
                    {
                        if (!ex.Message.Contains(ColumnMappingExceptionMessage))
                            throw ex;
                        await context.Database.ExecuteSqlCommandAsync(SqlQueryBuilder.CreateTableCopy(tableInfo.FullTableName, tableInfo.FullTempTableName, tableInfo));
                        if (!tableInfo.BulkConfig.UseTempDB)
                            await context.Database.ExecuteSqlCommandAsync(SqlQueryBuilder.DropTable(tableInfo.FullTempOutputTableName));
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
            if (tableInfo.BulkConfig.UpdateByProperties == null || tableInfo.BulkConfig.UpdateByProperties.Count() == 0)
                tableInfo.CheckHasIdentity(context);

            context.Database.ExecuteSqlCommand(SqlQueryBuilder.CreateTableCopy(tableInfo.FullTableName, tableInfo.FullTempTableName, tableInfo));
            if (tableInfo.CreatedOutputTable)
            {
                context.Database.ExecuteSqlCommand(SqlQueryBuilder.CreateTableCopy(tableInfo.FullTableName, tableInfo.FullTempOutputTableName, tableInfo, true));
            }
            try
            {
                Insert(context, entities, tableInfo, progress);
                context.Database.ExecuteSqlCommand(SqlQueryBuilder.MergeTable(tableInfo, operationType));

                if (tableInfo.CreatedOutputTable)
                {
                    try
                    {
                        tableInfo.LoadOutputData(context, entities);
                    }
                    finally
                    {
                        if (!tableInfo.BulkConfig.UseTempDB)
                            context.Database.ExecuteSqlCommand(SqlQueryBuilder.DropTable(tableInfo.FullTempOutputTableName));
                    }
                }
            }
            finally
            {
                if (!tableInfo.BulkConfig.UseTempDB)
                    context.Database.ExecuteSqlCommand(SqlQueryBuilder.DropTable(tableInfo.FullTempTableName));
            }
        }

        public static async Task MergeAsync<T>(DbContext context, IList<T> entities, TableInfo tableInfo, OperationType operationType, Action<decimal> progress) where T : class
        {
            tableInfo.InsertToTempTable = true;
            await tableInfo.CheckHasIdentityAsync(context).ConfigureAwait(false);

            await context.Database.ExecuteSqlCommandAsync(SqlQueryBuilder.CreateTableCopy(tableInfo.FullTableName, tableInfo.FullTempTableName, tableInfo)).ConfigureAwait(false);
            if (tableInfo.CreatedOutputTable)
            {
                await context.Database.ExecuteSqlCommandAsync(SqlQueryBuilder.CreateTableCopy(tableInfo.FullTableName, tableInfo.FullTempOutputTableName, tableInfo, true)).ConfigureAwait(false);
            }
            try
            {
                await InsertAsync(context, entities, tableInfo, progress).ConfigureAwait(false);
                await context.Database.ExecuteSqlCommandAsync(SqlQueryBuilder.MergeTable(tableInfo, operationType)).ConfigureAwait(false);

                if (tableInfo.CreatedOutputTable)
                {
                    try
                    {

                        await tableInfo.LoadOutputDataAsync(context, entities).ConfigureAwait(false);
                    }
                    finally
                    {
                        if (!tableInfo.BulkConfig.UseTempDB)
                            await context.Database.ExecuteSqlCommandAsync(SqlQueryBuilder.DropTable(tableInfo.FullTempOutputTableName)).ConfigureAwait(false);
                    }
                }
            }
            finally
            {
                if (!tableInfo.BulkConfig.UseTempDB)
                    await context.Database.ExecuteSqlCommandAsync(SqlQueryBuilder.DropTable(tableInfo.FullTempTableName)).ConfigureAwait(false);
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

        public static async Task ReadAsync<T>(DbContext context, IList<T> entities, TableInfo tableInfo, Action<decimal> progress) where T : class
        {
            Dictionary<string, string> previousPropertyColumnNamesDict = tableInfo.ConfigureBulkReadTableInfo(context);

            await context.Database.ExecuteSqlCommandAsync(SqlQueryBuilder.CreateTableCopy(tableInfo.FullTableName, tableInfo.FullTempTableName, tableInfo));

            try
            {
                await InsertAsync(context, entities, tableInfo, progress);

                tableInfo.PropertyColumnNamesDict = previousPropertyColumnNamesDict;

                var sqlQuery = SqlQueryBuilder.SelectJoinTable(tableInfo);

                //var existingEntities = await context.Set<T>().FromSql(sqlQuery).ToListAsync();
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
                var existingEntities = (await compiled(context).ToListAsync().ConfigureAwait(false));

                tableInfo.UpdateReadEntities(entities, existingEntities);
            }
            finally
            {
                if (!tableInfo.BulkConfig.UseTempDB)
                    await context.Database.ExecuteSqlCommandAsync(SqlQueryBuilder.DropTable(tableInfo.FullTempTableName));
            }
        }
        #endregion

        #region DataTable

        internal static DataTable GetDataTable<T>(DbContext context, IList<T> entities, SqlBulkCopy sqlBulkCopy)
        {
            var dataTable = new DataTable();
            var columnsDict = new Dictionary<string, object>();
            var ownedPropertiesDict = new Dictionary<string, PropertyInfo>();

            var type = typeof(T);
            var entityType = context.Model.FindEntityType(type);
            var entityPropertiesDict = entityType.GetProperties().ToDictionary(a => a.Name, a => a);
            var entityNavigationOwnedDict = entityType.GetNavigations().Where(a => a.GetTargetType().IsOwned()).ToDictionary(a => a.Name, a => a);
            var properties = type.GetProperties();

            foreach (var property in properties)
            {
                if (entityPropertiesDict.ContainsKey(property.Name))
                {
                    string columnName = entityPropertiesDict[property.Name].Relational().ColumnName;
                    var propertyType = Nullable.GetUnderlyingType(property.PropertyType) ?? property.PropertyType;
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

                    var ownedProperties = property.PropertyType.GetProperties().Where(prop => !Attribute.IsDefined(prop, typeof(NotMappedAttribute)));
                    ownedPropertiesDict = ownedProperties.ToList().ToDictionary(a => a.Name, a => a);

                    var ownedEntityProperties = ownedEntityType.GetProperties().Where(a => ownedPropertiesDict.Keys.Contains(a.Name)).ToList();
                    var ownedEntityPropertyNameColumnNameDict = new Dictionary<string, string>();

                    foreach (var ownedEntityProperty in ownedEntityProperties)
                    {
                        if (!ownedEntityProperty.IsPrimaryKey())
                        {
                            string columnName = ownedEntityProperty.Relational().ColumnName;
                            ownedEntityPropertyNameColumnNameDict.Add(ownedEntityProperty.Name, columnName);
                        }
                    }

                    foreach (var ownedProperty in ownedProperties)
                    {
                        if (ownedEntityPropertyNameColumnNameDict.ContainsKey(ownedProperty.Name))
                        {
                            string columnName = ownedEntityPropertyNameColumnNameDict[ownedProperty.Name];
                            var ownedPropertyType = Nullable.GetUnderlyingType(ownedProperty.PropertyType) ?? ownedProperty.PropertyType;
                            dataTable.Columns.Add(columnName, ownedPropertyType);
                            columnsDict.Add(property.Name + "_" + ownedProperty.Name, null);
                        }
                    }
                }
            }

            foreach (var entity in entities)
            {
                foreach (var property in properties)
                {
                    var propertyValue = property.GetValue(entity, null);
                    if (entityPropertiesDict.ContainsKey(property.Name))
                    {
                        columnsDict[property.Name] = propertyValue;
                    }
                    else if (entityNavigationOwnedDict.ContainsKey(property.Name))
                    {
                        var ownedProperties = property.PropertyType.GetProperties().Where(a => ownedPropertiesDict.Keys.Contains(a.Name));
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
        internal static SqlConnection OpenAndGetSqlConnection(DbContext context)
        {
            if (context.Database.GetDbConnection().State != ConnectionState.Open)
            {
                context.Database.GetDbConnection().Open();
            }
            return context.Database.GetDbConnection() as SqlConnection;
        }

        internal static async Task<SqlConnection> OpenAndGetSqlConnectionAsync(DbContext context)
        {
            if (context.Database.GetDbConnection().State != ConnectionState.Open)
            {
                await context.Database.GetDbConnection().OpenAsync().ConfigureAwait(false);
            }
            return context.Database.GetDbConnection() as SqlConnection;
        }

        private static SqlBulkCopy GetSqlBulkCopy(SqlConnection sqlConnection, IDbContextTransaction transaction, SqlBulkCopyOptions sqlBulkCopyOptions = SqlBulkCopyOptions.Default)
        {
            if (transaction == null)
            {
                return new SqlBulkCopy(sqlConnection, sqlBulkCopyOptions, null);
            }
            else
            {
                var sqlTransaction = (SqlTransaction)transaction.GetDbTransaction();
                return new SqlBulkCopy(sqlConnection, sqlBulkCopyOptions, sqlTransaction);
            }
        }
        #endregion
    }
}
