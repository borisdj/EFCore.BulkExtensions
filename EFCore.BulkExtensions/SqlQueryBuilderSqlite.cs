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
            List<string> propertiesList = tableInfo.PropertyColumnNamesDict.Keys.ToList();

            bool keepIdentity = tableInfo.BulkConfig.SqlBulkCopyOptions.HasFlag(SqlBulkCopyOptions.KeepIdentity);
            if(operationType == OperationType.Insert && !keepIdentity && tableInfo.HasIdentity)
            {
                var identityPropertyName = tableInfo.PropertyColumnNamesDict.SingleOrDefault(a => a.Value == tableInfo.IdentityColumnName).Key;
                columnsList = columnsList.Where(a => a != tableInfo.IdentityColumnName).ToList();
                propertiesList = propertiesList.Where(a => a != identityPropertyName).ToList();
            }

            var commaSeparatedColumns = SqlQueryBuilder.GetCommaSeparatedColumns(columnsList);
            var commaSeparatedColumnsParams = SqlQueryBuilder.GetCommaSeparatedColumns(propertiesList, "@").Replace("[", "").Replace("]", "").Replace(".", "_");

            var q = $"INSERT INTO [{tableName}] " +
                    $"({commaSeparatedColumns}) " +
                    $"VALUES ({commaSeparatedColumnsParams})";

            if (operationType == OperationType.InsertOrUpdate)
            {
                List<string> primaryKeys = tableInfo.PrimaryKeys.Select(k => tableInfo.PropertyColumnNamesDict[k]).ToList();
                var commaSeparatedPrimaryKeys = SqlQueryBuilder.GetCommaSeparatedColumns(primaryKeys);
                var commaSeparatedColumnsEquals = SqlQueryBuilder.GetCommaSeparatedColumns(columnsList, equalsTable: "", propertColumnsNamesDict: tableInfo.PropertyColumnNamesDict).Replace("]", "").Replace(" = .[", "] = @").Replace(".", "_");
                var commaANDSeparatedPrimaryKeys = SqlQueryBuilder.GetANDSeparatedColumns(primaryKeys, equalsTable: "@", propertColumnsNamesDict: tableInfo.PropertyColumnNamesDict).Replace("]", "").Replace(" = @[", "] = @").Replace(".", "_");

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
            var commaSeparatedColumns = SqlQueryBuilder.GetCommaSeparatedColumns(columnsList, equalsTable: "@", propertColumnsNamesDict: tableInfo.PropertyColumnNamesDict).Replace("]", "").Replace(" = @[", "] = @").Replace(".", "_"); ;
            var commaSeparatedPrimaryKeys = SqlQueryBuilder.GetANDSeparatedColumns(primaryKeys, equalsTable: "@", propertColumnsNamesDict: tableInfo.PropertyColumnNamesDict).Replace("]", "").Replace(" = @[", "] = @").Replace(".", "_"); ;

            var q = $"UPDATE [{tableName}] " +
                    $"SET {commaSeparatedColumns} " +
                    $"WHERE {commaSeparatedPrimaryKeys};";
            return q;
        }

        public static string DeleteFromTable(TableInfo tableInfo, string tableName = null)
        {
            tableName = tableName ?? tableInfo.TableName;
            List<string> primaryKeys = tableInfo.PrimaryKeys.Select(k => tableInfo.PropertyColumnNamesDict[k]).ToList();
            var commaSeparatedPrimaryKeys = SqlQueryBuilder.GetANDSeparatedColumns(primaryKeys, equalsTable: "@", propertColumnsNamesDict: tableInfo.PropertyColumnNamesDict).Replace("]", "").Replace(" = @[", "] = @").Replace(".", "_");

            var q = $"DELETE FROM [{tableName}] " +
                    $"WHERE {commaSeparatedPrimaryKeys};";
            return q;
        }
    }
}
