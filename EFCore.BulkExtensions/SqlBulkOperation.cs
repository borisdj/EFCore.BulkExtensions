using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
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
        public static void Insert<T>(DbContext context, IList<T> entities, TableInfo tableInfo, Action<double> progress = null, int batchSize = 2000)
        {
            var sqlConnection = (SqlConnection)context.Database.GetDbConnection();
            try
            {
                sqlConnection.Open();
                using (var sqlBulkCopy = new SqlBulkCopy(sqlConnection))
                {
                    sqlBulkCopy.DestinationTableName = tableInfo.InsertToTempTable ? tableInfo.FullTempTableName : tableInfo.FullTableName;
                    sqlBulkCopy.BatchSize = batchSize;
                    sqlBulkCopy.NotifyAfter = batchSize;
                    sqlBulkCopy.SqlRowsCopied += (sender, e) => { progress?.Invoke(e.RowsCopied / entities.Count); };

                    foreach (var element in tableInfo.PropertyColumnNamesDict)
                    {
                        sqlBulkCopy.ColumnMappings.Add(element.Key, element.Value);
                    }

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
                    var entitiesWithOutputIdentity = context.Set<T>().FromSql(SqlQueryBuilder.SelectFromTable(tableInfo.FullTempOutputTableName, tableInfo.PrimaryKeyFormated)).ToList();
                    if (tableInfo.BulkConfig.PreserveInsertOrder) // Updates PK in entityList
                    {
                        var accessor = TypeAccessor.Create(typeof(T));
                        for (int i = 0; i < tableInfo.NumberOfEntities; i++)
                            accessor[entities[i], tableInfo.PrimaryKey] = accessor[entitiesWithOutputIdentity[i], tableInfo.PrimaryKey];
                    }
                    else // Clears entityList and then refill it with loaded entites from Db
                    {
                        entities.Clear();
                        ((List<T>)entities).AddRange(entitiesWithOutputIdentity);
                    }
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
    }
}
