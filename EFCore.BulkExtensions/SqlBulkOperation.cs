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
        public static void Insert<T>(DbContext context, IList<T> entities, TableInfo tableInfo, bool useTempTable, Action<double> progress = null, int batchSize = 2000)
        {
            using (SqlBulkCopy sqlBulkCopy = new SqlBulkCopy(context.Database.GetDbConnection().ConnectionString))
            {
                sqlBulkCopy.DestinationTableName = useTempTable ? tableInfo.FullTempTableName : tableInfo.FullTableName;
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

        public static void Merge<T>(DbContext context, IList<T> entities, TableInfo tableInfo, OperationType operationType)
        {
            context.Database.ExecuteSqlCommand(SqlQueryBuilder.CreateTable(tableInfo));
            SqlBulkOperation.Insert<T>(context, entities, tableInfo, true);
            context.Database.ExecuteSqlCommand(SqlQueryBuilder.MergeTable(tableInfo, operationType));
            context.Database.ExecuteSqlCommand(SqlQueryBuilder.DropTable(tableInfo));
        }
    }
}
