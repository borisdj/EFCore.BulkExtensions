using Microsoft.Data.SqlClient;
using System.Collections.Generic;
using System.Linq;

namespace EFCore.BulkExtensions
{
    public static class SqlQueryBuilderSqlite
    {
        public static string SelectLastInsertRowId()
        {
            return "SELECT last_insert_rowid();";
        }

        public static string InsertIntoTable(TableInfo tableInfo, OperationType operationType, string tableName = null)
        {
            tableName = tableName ?? tableInfo.TableName;
            List<string> columnsList = tableInfo.PropertyColumnNamesDict.Values.ToList();

            bool keepIdentity = tableInfo.BulkConfig.SqlBulkCopyOptions.HasFlag(SqlBulkCopyOptions.KeepIdentity);
            if(operationType == OperationType.Insert && !keepIdentity)
            {
                columnsList = columnsList.Where(a => a != tableInfo.IdentityColumnName).ToList();
            }

            var commaSeparatedColumns = SqlQueryBuilder.GetCommaSeparatedColumns(columnsList);
            var commaSeparatedColumnsParams = SqlQueryBuilder.GetCommaSeparatedColumns(columnsList, "@").Replace("[", "").Replace("]", "");

            var q = $"INSERT INTO [{tableName}] " +
                    $"({commaSeparatedColumns}) " +
                    $"VALUES ({commaSeparatedColumnsParams})";

            if (operationType == OperationType.InsertOrUpdate)
            {
                List<string> primaryKeys = tableInfo.PrimaryKeys.Select(k => tableInfo.PropertyColumnNamesDict[k]).ToList();
                var commaSeparatedPrimaryKeys = SqlQueryBuilder.GetCommaSeparatedColumns(primaryKeys);
                var commaANDSeparatedPrimaryKeys = SqlQueryBuilder.GetANDSeparatedColumns(primaryKeys, equalsTable: "@").Replace("]", "").Replace(" = @[", "] = @");
                var commaSeparatedColumnsEquals = SqlQueryBuilder.GetCommaSeparatedColumns(columnsList, equalsTable: "").Replace("]", "").Replace(" = .[", "] = @");

                q += $" ON CONFLICT({commaSeparatedPrimaryKeys}) DO UPDATE" +
                     $" SET {commaSeparatedColumnsEquals}" +
                     $" WHERE {commaANDSeparatedPrimaryKeys}";
            }
            return q + ";";
        }

        public static string UpdateSetTable(TableInfo tableInfo, string tableName = null)
        {
            tableName = tableName ?? tableInfo.TableName;
            List<string> columnsList = tableInfo.PropertyColumnNamesDict.Values.ToList();
            List<string> primaryKeys = tableInfo.PrimaryKeys.Select(k => tableInfo.PropertyColumnNamesDict[k]).ToList();
            var commaSeparatedColumns = SqlQueryBuilder.GetCommaSeparatedColumns(columnsList, equalsTable: "@").Replace("]", "").Replace(" = @[", "] = @");
            var commaSeparatedPrimaryKeys = SqlQueryBuilder.GetANDSeparatedColumns(primaryKeys, equalsTable: "@").Replace("]", "").Replace(" = @[", "] = @");

            var q = $"UPDATE [{tableName}] " +
                    $"SET {commaSeparatedColumns} " +
                    $"WHERE {commaSeparatedPrimaryKeys};";
            return q;
        }

        public static string DeleteFromTable(TableInfo tableInfo, string tableName = null)
        {
            tableName = tableName ?? tableInfo.TableName;
            List<string> primaryKeys = tableInfo.PrimaryKeys.Select(k => tableInfo.PropertyColumnNamesDict[k]).ToList();
            var commaSeparatedPrimaryKeys = SqlQueryBuilder.GetANDSeparatedColumns(primaryKeys, equalsTable: "@").Replace("]", "").Replace(" = @[", "] = @");

            var q = $"DELETE FROM [{tableName}] " +
                    $"WHERE {commaSeparatedPrimaryKeys};";
            return q;
        }
    }
}
