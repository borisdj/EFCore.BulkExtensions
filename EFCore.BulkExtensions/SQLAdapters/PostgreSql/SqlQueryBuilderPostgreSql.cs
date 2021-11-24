using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using System;
using System.Collections.Generic;
using System.Linq;

namespace EFCore.BulkExtensions.SQLAdapters.PostgreSql
{
    public static class SqlQueryBuilderPostgreSql
    {
        public static string CreateTableCopy(string existingTableName, string newTableName, TableInfo tableInfo, bool isOutputTable = false)
        {
            var q = $"CREATE TABLE {newTableName} " +
                    $"AS TABLE {existingTableName} " +
                    $"WITH NO DATA;";
            q = q.Replace("[", @"""").Replace("]", @"""");
            return q;
        }

        public static string InsertIntoTable(TableInfo tableInfo, OperationType operationType, string tableName = null)
        {
            tableName ??= tableInfo.InsertToTempTable ? tableInfo.FullTempTableName : tableInfo.FullTableName;
            tableName = tableName.Replace("[", @"""").Replace("]", @"""");

            var columnsList = GetColumnList(tableInfo, operationType);

            var commaSeparatedColumns = SqlQueryBuilder.GetCommaSeparatedColumns(columnsList).Replace("[", @"""").Replace("]", @"""");

            var q = $"COPY {tableName} " +
                    $"({commaSeparatedColumns}) " +
                    $"FROM STDIN (FORMAT BINARY)";

            return q + ";";
        }

        public static string MergeTable<T>(DbContext context, TableInfo tableInfo, OperationType operationType, IEnumerable<string> entityPropertyWithDefaultValue = default) where T : class
        {
            var columnsList = GetColumnList(tableInfo, operationType);

            if (operationType == OperationType.InsertOrUpdateOrDelete)
            {
                throw new NotImplementedException($"For Postgres method {OperationType.InsertOrUpdateOrDelete} is not yet supported. Use combination of InsertOrUpdate with Read and Delete");
            }

            string q;
            if (operationType == OperationType.Read)
            {
                var readByColumns = SqlQueryBuilder.GetCommaSeparatedColumns(tableInfo.PrimaryKeysPropertyColumnNameDict.Values.ToList(), tableInfo.FullTableName, tableInfo.FullTempTableName);

                q = $"SELECT {tableInfo.FullTableName}.* FROM {tableInfo.FullTableName} " +
                    $"JOIN {tableInfo.FullTempTableName} " +
                    $"USING ({readByColumns})"; //$"ON ({tableInfo.FullTableName}.readByColumns = {tableInfo.FullTempTableName}.readByColumns);";
            }
            if (operationType == OperationType.Delete)
            {
                var deleteByColumns = SqlQueryBuilder.GetCommaSeparatedColumns(tableInfo.PrimaryKeysPropertyColumnNameDict.Values.ToList(), tableInfo.FullTableName, tableInfo.FullTempTableName);
                deleteByColumns = deleteByColumns.Replace("[", @"""").Replace("]", @"""");

                q = $"DELETE FROM {tableInfo.FullTableName} " +
                    $"USING {tableInfo.FullTempTableName} " +
                    $@"WHERE {deleteByColumns}";
            }
            else
            {
                var commaSeparatedColumns = SqlQueryBuilder.GetCommaSeparatedColumns(columnsList).Replace("[", @"""").Replace("]", @"""");

                var updateByColumns = SqlQueryBuilder.GetCommaSeparatedColumns(tableInfo.PrimaryKeysPropertyColumnNameDict.Values.ToList()).Replace("[", @"""").Replace("]", @"""");

                var columnsListEquals = GetColumnList(tableInfo, OperationType.Insert);
                var equalsColumns = SqlQueryBuilder.GetCommaSeparatedColumns(columnsListEquals, equalsTable: "EXCLUDED").Replace("[", @"""").Replace("]", @"""");

                q = $"INSERT INTO {tableInfo.FullTableName} ({commaSeparatedColumns}) " +
                    $"(SELECT {commaSeparatedColumns} FROM {tableInfo.FullTempTableName}) " +
                    $"ON CONFLICT ({updateByColumns}) " +
                    $"DO UPDATE SET {equalsColumns}";

                if (tableInfo.CreatedOutputTable)
                {
                    var allColumnsList = tableInfo.PropertyColumnNamesDict.Values.ToList();
                    string commaSeparatedColumnsNames = SqlQueryBuilder.GetCommaSeparatedColumns(allColumnsList).Replace("[", @"""").Replace("]", @"""");
                    q += $" RETURNING {commaSeparatedColumnsNames}";
                }
            }

            q = q.Replace("[", @"""").Replace("]", @"""");
            q += ";";
            return q;
        }

        public static List<string> GetColumnList(TableInfo tableInfo, OperationType operationType)
        {
            var tempDict = tableInfo.PropertyColumnNamesDict;
            if (operationType == OperationType.Insert && tableInfo.PropertyColumnNamesDict.Any()) // Only OnInsert omit colums with Default values
            {
                tableInfo.PropertyColumnNamesDict = tableInfo.PropertyColumnNamesDict.Where(a => !tableInfo.DefaultValueProperties.Contains(a.Key)).ToDictionary(a => a.Key, a => a.Value);
            }

            List<string> columnsList = tableInfo.PropertyColumnNamesDict.Values.ToList();
            List<string> propertiesList = tableInfo.PropertyColumnNamesDict.Keys.ToList();

            tableInfo.PropertyColumnNamesDict = tempDict;

            bool keepIdentity = tableInfo.BulkConfig.SqlBulkCopyOptions.HasFlag(SqlBulkCopyOptions.KeepIdentity);
            var uniquColumnName = tableInfo.PrimaryKeysPropertyColumnNameDict.Values.ToList().FirstOrDefault();
            if (!keepIdentity && tableInfo.HasIdentity && (operationType == OperationType.Insert || tableInfo.IdentityColumnName != uniquColumnName))
            {
                var identityPropertyName = tableInfo.PropertyColumnNamesDict.SingleOrDefault(a => a.Value == tableInfo.IdentityColumnName).Key;
                columnsList = columnsList.Where(a => a != tableInfo.IdentityColumnName).ToList();
                propertiesList = propertiesList.Where(a => a != identityPropertyName).ToList();
            }

            return columnsList;
        }

        public static string TruncateTable(string tableName)
        {
            var q = $"TRUNCATE {tableName} RESTART IDENTITY;";
            q = q.Replace("[", @"""").Replace("]", @"""");
            return q;
        }

        public static string DropTable(string tableName, bool isTempTable)
        {
            string q = $"DROP TABLE IF EXISTS {tableName}";
            q = q.Replace("[", @"""").Replace("]", @"""");
            return q;
        }

        public static string CountUniqueConstrain(TableInfo tableInfo)
        {
            var q = $"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc " +
                    $"INNER JOIN INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE cu " +
                    $"ON cu.CONSTRAINT_NAME = tc.CONSTRAINT_NAME " +
                    $"WHERE tc.CONSTRAINT_TYPE = 'UNIQUE' " +
                    $"AND tc.TABLE_NAME = '{tableInfo.TableName}' " +
                    $"AND cu.COLUMN_NAME = '{tableInfo.PrimaryKeysPropertyColumnNameDict.Values.ToList().FirstOrDefault()}'";
            return q;
        }

        public static string CreateUniqueIndex(TableInfo tableInfo)
        {
            var tableName = tableInfo.TableName;
            var uniquColumnName = tableInfo.PrimaryKeysPropertyColumnNameDict.Values.ToList().FirstOrDefault();
            var q = $@"CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS ""tempUniqueIndex_{tableName}_{uniquColumnName}"" " +
                    $@"ON ""{tableName}"" (""{uniquColumnName}"")";
            return q;
        }

        public static string CreateUniqueConstrain(TableInfo tableInfo)
        {
            var tableName = tableInfo.TableName;
            var uniquColumnName = tableInfo.PrimaryKeysPropertyColumnNameDict.Values.ToList().FirstOrDefault();
            var uniquConstrainName = $"tempUniqueIndex_{tableName}_{uniquColumnName}";
            var q = $@"ALTER TABLE ""{tableName}""" +
                    $@"ADD CONSTRAINT ""{uniquConstrainName}"" " +
                    $@"UNIQUE USING INDEX ""{uniquConstrainName}""";
            return q;
        }

        public static string DropUniqueConstrain(TableInfo tableInfo)
        {
            var tableName = tableInfo.TableName;
            var uniquColumnName = tableInfo.PrimaryKeysPropertyColumnNameDict.Values.ToList().FirstOrDefault();
            var uniquConstrainName = $"tempUniqueIndex_{tableName}_{uniquColumnName}";
            var q = $@"ALTER TABLE ""{tableName}""" +
                    $@"DROP CONSTRAINT ""{uniquConstrainName}"";";
            return q;
        }

        public static string RestructureForBatch(string sql, bool isDelete = false)
        {
            sql = sql.Replace("[", @"""").Replace("]", @"""");

            if (isDelete)
            {
                //FROM
                // DELETE i FROM "Item" AS i WHERE i."ItemId" <= 1"
                //TO
                // DELETE FROM "Item" AS i WHERE i."ItemId" <= 1"
                //WOULD ALSO WORK
                // DELETE FROM "Item" WHERE "ItemId" <= 1

                sql = sql.Replace("DELETE i", "DELETE ");
            }
            else
            {
                //FROM
                // UPDATE i SET "Description" = @Description, "Price\" = @Price FROM "Item" AS i WHERE i."ItemId" <= 1
                //TO
                // UPDATE "Item" AS i SET "Description" = 'Update N', "Price" = 1.5 FROM "Item" WHERE i."ItemId" <= 1
                //WOULD ALSO WORK
                // UPDATE "Item" SET "Description" = 'Update N', "Price" = 1.5 FROM "Item" WHERE "ItemId" <= 1

                string tableAS = sql.Substring(sql.IndexOf("FROM") + 4, sql.IndexOf("AS i") - sql.IndexOf("FROM"));
                sql = sql.Replace("AS i", "");
                sql = sql.Replace("UPDATE i", "UPDATE " + tableAS);
            }
            return sql;
        }
    }
}
