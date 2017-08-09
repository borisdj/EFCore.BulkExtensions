using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Threading.Tasks;
using FastMember;
using Microsoft.EntityFrameworkCore;

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
        public static void Insert<T>(DbContext context, IList<T> entities, TableInfo tableInfo, Action<double> progress = null)
        {
            var sqlConnection = (SqlConnection)context.Database.GetDbConnection();
            try
            {
                sqlConnection.Open();
                using (var sqlBulkCopy = new SqlBulkCopy(sqlConnection))
                {
                    tableInfo.SetSqlBulkCopyConfig(sqlBulkCopy, entities, progress);
                    using (var reader = ObjectReader.Create(entities, tableInfo.PropertyColumnNamesDict.Keys.ToArray()))
                    {
                        sqlBulkCopy.WriteToServer(reader);
                    }
                }
            }
            finally
            {
                sqlConnection.Close();
            }
        }

        public static async Task InsertAsync<T>(DbContext context, IList<T> entities, TableInfo tableInfo, Action<double> progress = null)
        {
            var sqlConnection = (SqlConnection)context.Database.GetDbConnection();
            try
            {
                await sqlConnection.OpenAsync();
                using (var sqlBulkCopy = new SqlBulkCopy(sqlConnection))
                {
                    tableInfo.SetSqlBulkCopyConfig(sqlBulkCopy, entities, progress);
                    using (var reader = ObjectReader.Create(entities, tableInfo.PropertyColumnNamesDict.Keys.ToArray()))
                    {
                        await sqlBulkCopy.WriteToServerAsync(reader);
                    }
                }
            }
            finally
            {
                sqlConnection.Close();
            }
        }
        
        public static void Merge<T>(DbContext context, IList<T> entities, TableInfo tableInfo, OperationType operationType) where T : class
        {
            tableInfo.InsertToTempTable = true;
            tableInfo.CheckHasIdentity(context);

            context.Database.ExecuteSqlCommand(SqlQueryBuilder.CreateTableCopy(tableInfo.FullTableName, tableInfo.FullTempTableName));
            if (tableInfo.BulkConfig.SetOutputIdentity)
            {
                context.Database.ExecuteSqlCommand(SqlQueryBuilder.CreateTableCopy(tableInfo.FullTableName, tableInfo.FullTempOutputTableName));
            }
            try
            {
                SqlBulkOperation.Insert<T>(context, entities, tableInfo);
                context.Database.ExecuteSqlCommand(SqlQueryBuilder.MergeTable(tableInfo, operationType));
                context.Database.ExecuteSqlCommand(SqlQueryBuilder.DropTable(tableInfo.FullTempTableName));

                if (tableInfo.BulkConfig.SetOutputIdentity)
                {
                    tableInfo.UpdateOutputIdentity(context, entities);
                    context.Database.ExecuteSqlCommand(SqlQueryBuilder.DropTable(tableInfo.FullTempOutputTableName));
                }
            }
            catch (Exception ex)
            {
                if (tableInfo.BulkConfig.SetOutputIdentity)
                {
                    context.Database.ExecuteSqlCommand(SqlQueryBuilder.DropTable(tableInfo.FullTempOutputTableName));
                }
                context.Database.ExecuteSqlCommand(SqlQueryBuilder.DropTable(tableInfo.FullTempTableName));
                throw ex;
            }
        }

        public static async Task MergeAsync<T>(DbContext context, IList<T> entities, TableInfo tableInfo, OperationType operationType) where T : class
        {
            tableInfo.InsertToTempTable = true;
            await tableInfo.CheckHasIdentityAsync(context);

            await context.Database.ExecuteSqlCommandAsync(SqlQueryBuilder.CreateTableCopy(tableInfo.FullTableName, tableInfo.FullTempTableName));
            if (tableInfo.BulkConfig.SetOutputIdentity)
            {
                await context.Database.ExecuteSqlCommandAsync(SqlQueryBuilder.CreateTableCopy(tableInfo.FullTableName, tableInfo.FullTempOutputTableName));
            }
            try
            {
                await SqlBulkOperation.InsertAsync<T>(context, entities, tableInfo);
                await context.Database.ExecuteSqlCommandAsync(SqlQueryBuilder.MergeTable(tableInfo, operationType));
                await context.Database.ExecuteSqlCommandAsync(SqlQueryBuilder.DropTable(tableInfo.FullTempTableName));

                if (tableInfo.BulkConfig.SetOutputIdentity)
                {
                    tableInfo.UpdateOutputIdentity(context, entities);
                    await context.Database.ExecuteSqlCommandAsync(SqlQueryBuilder.DropTable(tableInfo.FullTempOutputTableName));
                }
            }
            catch (Exception ex)
            {
                if (tableInfo.BulkConfig.SetOutputIdentity)
                {
                    await context.Database.ExecuteSqlCommandAsync(SqlQueryBuilder.DropTable(tableInfo.FullTempOutputTableName));
                }
                await context.Database.ExecuteSqlCommandAsync(SqlQueryBuilder.DropTable(tableInfo.FullTempTableName));
                throw ex;
            }
        }
    }
}
