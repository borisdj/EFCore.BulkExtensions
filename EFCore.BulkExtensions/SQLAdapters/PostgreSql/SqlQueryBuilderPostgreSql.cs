using EFCore.BulkExtensions.SQLAdapters.SQLServer;
using Microsoft.Data.SqlClient;
using System;
using System.Collections.Generic;
using System.Linq;

namespace EFCore.BulkExtensions.SQLAdapters.PostgreSql
{
    public static class SqlQueryBuilderPostgreSql
    {
        public static string InsertIntoTable(TableInfo tableInfo, OperationType operationType, string tableName = null)
        {
            tableName ??= tableInfo.InsertToTempTable ? tableInfo.TempTableName : tableInfo.TableName;

            var tempDict = tableInfo.PropertyColumnNamesDict;
            if (operationType == OperationType.Insert && tableInfo.PropertyColumnNamesDict.Any()) // Only OnInsert omit colums with Default values
            {
                tableInfo.PropertyColumnNamesDict = tableInfo.PropertyColumnNamesDict.Where(a => !tableInfo.DefaultValueProperties.Contains(a.Key)).ToDictionary(a => a.Key, a => a.Value);
            }

            List<string> columnsList = tableInfo.PropertyColumnNamesDict.Values.ToList();
            List<string> propertiesList = tableInfo.PropertyColumnNamesDict.Keys.ToList();

            tableInfo.PropertyColumnNamesDict = tempDict;

            bool keepIdentity = tableInfo.BulkConfig.SqlBulkCopyOptions.HasFlag(SqlBulkCopyOptions.KeepIdentity);
            if (operationType == OperationType.Insert && !keepIdentity && tableInfo.HasIdentity)
            {
                var identityPropertyName = tableInfo.PropertyColumnNamesDict.SingleOrDefault(a => a.Value == tableInfo.IdentityColumnName).Key;
                columnsList = columnsList.Where(a => a != tableInfo.IdentityColumnName).ToList();
                propertiesList = propertiesList.Where(a => a != identityPropertyName).ToList();
            }

            var commaSeparatedColumns = SqlQueryBuilder.GetCommaSeparatedColumns(columnsList).Replace("[", @"""").Replace("]", @"""");

            var q = $@"COPY ""{tableName}"" " +
                    $"({commaSeparatedColumns}) " +
                    $"FROM STDIN (FORMAT BINARY)";

            return q + ";";
        }

        public static string TruncateTable(string tableName)
        {
            var q = $"TRUNCATE {tableName} RESTART IDENTITY;";
            q = q.Replace("[", @"""").Replace("]", @"""");
            return q;
        }

        /*public static string UpdateSetTable(TableInfo tableInfo, string tableName = null)
        {
            tableName ??= tableInfo.TableName;
            List<string> columnsList = tableInfo.PropertyColumnNamesDict.Values.ToList();
            List<string> primaryKeys = tableInfo.PrimaryKeysPropertyColumnNameDict.Select(k => tableInfo.PropertyColumnNamesDict[k.Key]).ToList();
            var commaSeparatedColumns = SqlQueryBuilder.GetCommaSeparatedColumns(columnsList, equalsTable: "@", propertColumnsNamesDict: tableInfo.PropertyColumnNamesDict).Replace("]", "").Replace(" = @[", "] = @").Replace(".", "_"); ;
            var commaSeparatedPrimaryKeys = SqlQueryBuilder.GetANDSeparatedColumns(primaryKeys, equalsTable: "@", propertColumnsNamesDict: tableInfo.PropertyColumnNamesDict).Replace("]", "").Replace(" = @[", "] = @").Replace(".", "_"); ;

            var q = $"UPDATE [{tableName}] " +
                    $"SET {commaSeparatedColumns} " +
                    $"WHERE {commaSeparatedPrimaryKeys};";
            return q;
        }

        public static string DeleteFromTable(TableInfo tableInfo, string tableName = null)
        {
            tableName ??= tableInfo.TableName;
            List<string> primaryKeys = tableInfo.PrimaryKeysPropertyColumnNameDict.Select(k => tableInfo.PropertyColumnNamesDict[k.Key]).ToList();
            var commaSeparatedPrimaryKeys = SqlQueryBuilder.GetANDSeparatedColumns(primaryKeys, equalsTable: "@", propertColumnsNamesDict: tableInfo.PropertyColumnNamesDict).Replace("]", "").Replace(" = @[", "] = @").Replace(".", "_");

            var q = $"DELETE FROM [{tableName}] " +
                    $"WHERE {commaSeparatedPrimaryKeys};";
            return q;
        }

        public static string CreateTableCopy(string existingTableName, string newTableName) // Used for BulkRead
        {
            var q = $"CREATE TABLE {newTableName} AS SELECT * FROM {existingTableName} WHERE 0;";
            return q;
        }

        public static string DropTable(string tableName)
        {
            string q =  $"DROP TABLE IF EXISTS {tableName}";
            return q;
        }*/

        //NpgsqlCommand command = connection.CreateCommand();

        //context.Database.ExecuteSqlRaw("DROP TABLE data");
        //context.Database.ExecuteSqlRaw("CREATE TABLE data (field_text TEXT, field_int2 SMALLINT, field_int4 INTEGER)");
        //command.ExecuteNonQuery();

        //command.CommandText = @"SELECT COUNT(*) FROM ""Item""";
        //var count = command.ExecuteScalar();
    }
}
