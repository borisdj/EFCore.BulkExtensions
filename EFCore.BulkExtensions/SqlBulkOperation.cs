using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;

namespace EFCore.BulkExtensions
{
    public enum OperationType
    {
        Insert,
        InsertOrUpdate,
        Update,
        Delete,
    }

    internal static class SqlBulkOperation
    {
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
                    if (!tableInfo.HasOwnedTypes)
                    {
                        using (var reader = ObjectReaderEx.Create(entities, tableInfo.ShadowProperties, tableInfo.ConvertibleProperties, context, tableInfo.PropertyColumnNamesDict.Keys.ToArray()))
                        {
                            sqlBulkCopy.WriteToServer(reader);
                        }
                    }
                    else // With OwnedTypes DataTable is used since library FastMember can not (https://github.com/mgravell/fast-member/issues/21)
                    {
                        var dataTable = GetDataTable<T>(context, entities);
                        sqlBulkCopy.WriteToServer(dataTable);
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
                    if (!tableInfo.HasOwnedTypes)
                    {
                        using (var reader = ObjectReaderEx.Create(entities, tableInfo.ShadowProperties, tableInfo.ConvertibleProperties, context, tableInfo.PropertyColumnNamesDict.Keys.ToArray()))
                        {
                            await sqlBulkCopy.WriteToServerAsync(reader).ConfigureAwait(false);
                        }
                    }
                    else
                    {
                        var dataTable = GetDataTable<T>(context, entities);
                        await sqlBulkCopy.WriteToServerAsync(dataTable);
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
            if(tableInfo.BulkConfig.UpdateByProperties == null || tableInfo.BulkConfig.UpdateByProperties.Count() == 0)
                tableInfo.CheckHasIdentity(context);

            context.Database.ExecuteSqlCommand(SqlQueryBuilder.CreateTableCopy(tableInfo.FullTableName, tableInfo.FullTempTableName, tableInfo));
            if (tableInfo.BulkConfig.SetOutputIdentity)
            {
                context.Database.ExecuteSqlCommand(SqlQueryBuilder.CreateTableCopy(tableInfo.FullTableName, tableInfo.FullTempOutputTableName, tableInfo, true));
            }
            try
            {
                Insert(context, entities, tableInfo, progress);
                context.Database.ExecuteSqlCommand(SqlQueryBuilder.MergeTable(tableInfo, operationType));

                if (tableInfo.BulkConfig.SetOutputIdentity && tableInfo.HasSinglePrimaryKey)
                {
                    try
                    {
                        tableInfo.UpdateOutputIdentity(context, entities);
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
            if (tableInfo.BulkConfig.SetOutputIdentity && tableInfo.HasIdentity)
            {
                await context.Database.ExecuteSqlCommandAsync(SqlQueryBuilder.CreateTableCopy(tableInfo.FullTableName, tableInfo.FullTempOutputTableName, tableInfo, true)).ConfigureAwait(false);
            }
            try
            {
                await InsertAsync(context, entities, tableInfo, progress).ConfigureAwait(false);
                await context.Database.ExecuteSqlCommandAsync(SqlQueryBuilder.MergeTable(tableInfo, operationType)).ConfigureAwait(false);

                if (tableInfo.BulkConfig.SetOutputIdentity && tableInfo.HasIdentity)
                {
                    try
                    {
                        await tableInfo.UpdateOutputIdentityAsync(context, entities).ConfigureAwait(false);
                    }
                    finally
                    {
                        await context.Database.ExecuteSqlCommandAsync(SqlQueryBuilder.DropTable(tableInfo.FullTempOutputTableName)).ConfigureAwait(false);
                    }
                }
            }
            finally
            {
                await context.Database.ExecuteSqlCommandAsync(SqlQueryBuilder.DropTable(tableInfo.FullTempTableName)).ConfigureAwait(false);
            }
        }

        // IMPORTANT: works only if Properties of Entity are in the same order as Columns in Db
        internal static DataTable GetDataTable<T>(DbContext context, IList<T> entities)
        {
            var dataTable = new DataTable();
            var columnsDict = new Dictionary<string, object>();

            var type = typeof(T);
            var entityType = context.Model.FindEntityType(type);
            var entityPropertiesDict = entityType.GetProperties().ToDictionary(a => a.Name, a => a);
            var entityNavigationOwnedDict = entityType.GetNavigations().Where(a => a.GetTargetType().IsOwned()).ToDictionary(a => a.Name, a => a);
            var properties = type.GetProperties().Where(a => !a.GetGetMethod().IsVirtual);

            foreach (var property in properties)
            {
                if (entityPropertiesDict.ContainsKey(property.Name))
                {
                    string columnName = entityPropertiesDict[property.Name].Relational().ColumnName;
                    dataTable.Columns.Add(columnName, property.PropertyType);
                    columnsDict.Add(property.Name, null);
                }
                else if (entityNavigationOwnedDict.ContainsKey(property.Name)) // isOWned
                {
                    Type navOwnedType = type.Assembly.GetType(property.PropertyType.FullName);

                    var ownedEntityType = context.Model.FindEntityType(property.PropertyType);
                    if (ownedEntityType == null) // when single entity has more then one ownedType of same type (e.g. Address HomeAddress, Address WorkAddress)
                    {
                        ownedEntityType = context.Model.GetEntityTypes().SingleOrDefault(a => a.DefiningNavigationName == property.Name);
                    }
                    var ownedEntityProperties = ownedEntityType.GetProperties().ToList();
                    var ownedEntityPropertyNameColumnNameDict = new Dictionary<string, string>();

                    foreach (var ownedEntityProperty in ownedEntityProperties)
                    {
                        if (!ownedEntityProperty.IsPrimaryKey())
                        {
                            string columnName = ownedEntityProperty.Relational().ColumnName;
                            ownedEntityPropertyNameColumnNameDict.Add(ownedEntityProperty.Name, columnName);
                        }
                    }

                    var ownedProperties = property.PropertyType.GetProperties();
                    foreach (var ownedProperty in ownedProperties)
                    {
                        if (ownedEntityPropertyNameColumnNameDict.ContainsKey(ownedProperty.Name))
                        {
                            string columnName = ownedEntityPropertyNameColumnNameDict[ownedProperty.Name];
                            dataTable.Columns.Add(columnName, ownedProperty.PropertyType);
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
                        var ownedProperties = property.PropertyType.GetProperties();
                        foreach (var ownedProperty in ownedProperties)
                        {
                            columnsDict[property.Name + "_" + ownedProperty.Name] = ownedProperty.GetValue(propertyValue, null);
                        }
                    }
                }
                var record = columnsDict.Values.ToArray();
                dataTable.Rows.Add(record);
            }
            return dataTable;
        }

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
    }
}
