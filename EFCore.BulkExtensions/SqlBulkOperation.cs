using System;
using System.Collections.Generic;
using System.Data.Common;
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
            var sqlBulkCopy = new SqlBulkCopy(context.Database.GetDbConnection().ConnectionString)
            {
                DestinationTableName = tableInfo.InsertToTempTable ? tableInfo.FullTempTableName : tableInfo.FullTableName,
                BatchSize = batchSize,
                NotifyAfter = batchSize
            };
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

        public static void Merge<T>(DbContext context, IList<T> entities, TableInfo tableInfo, OperationType operationType) where T : class
        {
            context.Database.ExecuteSqlCommand(SqlQueryBuilder.CreateTableCopy(tableInfo.FullTableName, tableInfo.FullTempTableName));
            if (tableInfo.SetOutputIdentity)
            {
                context.Database.ExecuteSqlCommand(SqlQueryBuilder.CreateTableCopy(tableInfo.FullTableName, tableInfo.FullTempOutputTableName));
            }
            try
            {
                tableInfo.InsertToTempTable = true;

                int hasIdentity = 0;
                var conn = context.Database.GetDbConnection();
                try
                {
                    conn.OpenAsync();
                    using (var command = conn.CreateCommand())
                    {
                        string query = SqlQueryBuilder.SelectIsIdentity(tableInfo.FullTableName, tableInfo.PrimaryKey);
                        command.CommandText = query;
                        DbDataReader reader = command.ExecuteReader();

                        if (reader.HasRows)
                        {
                            while (reader.Read())
                            {
                                hasIdentity = (int)reader[0];
                            }
                        }
                        reader.Dispose();
                    }
                }
                finally
                {
                    conn.Close();
                }


                tableInfo.HasIdentity = hasIdentity == 1;
                SqlBulkOperation.Insert<T>(context, entities, tableInfo);
                context.Database.ExecuteSqlCommand(SqlQueryBuilder.MergeTable(tableInfo, operationType));

                context.Database.ExecuteSqlCommand(SqlQueryBuilder.DropTable(tableInfo.FullTempTableName));
                if (tableInfo.SetOutputIdentity)
                {
                    entities.Clear();
                    var entitiesWithOutputIdentity = context.Set<T>().FromSql(SqlQueryBuilder.SelectFromTable(tableInfo.FullTempOutputTableName, tableInfo.PrimaryKey)).ToList();
                    ((List<T>)entities).AddRange(entitiesWithOutputIdentity);
                    context.Database.ExecuteSqlCommand(SqlQueryBuilder.DropTable(tableInfo.FullTempOutputTableName));
                }
            }
            catch (Exception ex)
            {
                if (tableInfo.SetOutputIdentity)
                {
                    context.Database.ExecuteSqlCommand(SqlQueryBuilder.DropTable(tableInfo.FullTempOutputTableName));
                }
                context.Database.ExecuteSqlCommand(SqlQueryBuilder.DropTable(tableInfo.FullTempTableName));
                throw ex;
            }
        }
    }
}
