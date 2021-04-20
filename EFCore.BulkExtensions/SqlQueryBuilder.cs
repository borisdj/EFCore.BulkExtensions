using Microsoft.Data.SqlClient;
using System;
using System.Collections.Generic;
using System.Linq;

namespace EFCore.BulkExtensions
{
    public static class SqlQueryBuilder
    {
        public static string CreateTableCopy(string existingTableName, string newTableName, TableInfo tableInfo, bool isOutputTable = false)
        {
            // TODO: (optionaly) if CalculateStats = True but SetOutputIdentity = False then Columns could be ommited from Create and from MergeOutput
            List<string> columnsNames = (isOutputTable ? tableInfo.OutputPropertyColumnNamesDict : tableInfo.PropertyColumnNamesDict).Values.ToList();
            if (tableInfo.TimeStampColumnName != null)
            {
                columnsNames.Remove(tableInfo.TimeStampColumnName);
            }
            string statsColumns = (tableInfo.BulkConfig.CalculateStats && isOutputTable) ? ",[IsUpdate] = CAST(0 AS bit),[IsDelete] = CAST(0 AS bit)" : "";

            var q = $"SELECT TOP 0 {GetCommaSeparatedColumns(columnsNames, "T")} " + statsColumns +
                    $"INTO {newTableName} FROM {existingTableName} AS T " +
                    $"LEFT JOIN {existingTableName} AS Source ON 1 = 0;"; // removes Identity constrain
            return q;
        }

        public static string AlterTableColumnsToNullable(string tableName, TableInfo tableInfo, bool isOutputTable = false)
        {
            string q = "";
            foreach (var column in tableInfo.ColumnNamesTypesDict)
            {
                string columnName = column.Key;
                string columnType = column.Value;
                if (columnName == tableInfo.TimeStampColumnName)
                    columnType = tableInfo.TimeStampOutColumnType;
                q += $"ALTER TABLE {tableName} ALTER COLUMN [{columnName}] {columnType}; ";
            }
            return q;
        }

        // Not used for TableCopy since order of columns is not the same as of original table, that is required for the MERGE (instead after creation, columns are Altered to Nullable)
        public static string CreateTable(string newTableName, TableInfo tableInfo, bool isOutputTable = false)
        {
            List<string> columnsNames = (isOutputTable ? tableInfo.OutputPropertyColumnNamesDict : tableInfo.PropertyColumnNamesDict).Values.ToList();
            if (tableInfo.TimeStampColumnName != null)
            {
                columnsNames.Remove(tableInfo.TimeStampColumnName);
            }
            var columnsNamesAndTypes = new List<Tuple<string, string>>();
            foreach (var columnName in columnsNames)
            {
                if (!tableInfo.ColumnNamesTypesDict.TryGetValue(columnName, out string columnType))
                {
                    throw new InvalidOperationException($"Column Type not found in ColumnNamesTypesDict for column: '{columnName}'");
                }
                columnsNamesAndTypes.Add(new Tuple<string, string>(columnName, columnType));
            }
            if (tableInfo.BulkConfig.CalculateStats && isOutputTable)
            {
                columnsNamesAndTypes.Add(new Tuple<string, string>("[IsUpdate]", "bit"));
                columnsNamesAndTypes.Add(new Tuple<string, string>("[IsDelete]", "bit"));
            }
            var q = $"CREATE TABLE {newTableName} ({GetCommaSeparatedColumnsAndTypes(columnsNamesAndTypes)});";
            return q;
        }

        public static string AddColumn(string fullTableName, string columnName, string columnType)
        {
            var q = $"ALTER TABLE {fullTableName} ADD [{columnName}] {columnType};";
            return q;
        }

        public static string SelectFromOutputTable(TableInfo tableInfo)
        {
            List<string> columnsNames = tableInfo.OutputPropertyColumnNamesDict.Values.ToList();
            var q = $"SELECT {GetCommaSeparatedColumns(columnsNames)} FROM {tableInfo.FullTempOutputTableName} WHERE [{tableInfo.PrimaryKeysPropertyColumnNameDict.Select(x => x.Value).FirstOrDefault()}] IS NOT NULL";
            return q;
        }

        public static string SelectCountIsUpdateFromOutputTable(TableInfo tableInfo)
        {
            return SelectCountColumnFromOutputTable(tableInfo, "IsUpdate");
        }

        public static string SelectCountIsDeleteFromOutputTable(TableInfo tableInfo)
        {
            return SelectCountColumnFromOutputTable(tableInfo, "IsDelete");
        }

        public static string SelectCountColumnFromOutputTable(TableInfo tableInfo, string columnName)
        {
            var q = $"SELECT COUNT(*) FROM {tableInfo.FullTempOutputTableName} WHERE [{columnName}] = 1";
            return q;
        }

        public static string DropTable(string tableName, bool isTempTable)
        {
            string q = "";
            if (isTempTable)
            {
                q = $"IF OBJECT_ID ('tempdb..[#{tableName.Split('#')[1]}', 'U') IS NOT NULL DROP TABLE {tableName}";
            }
            else
            {
                q = $"IF OBJECT_ID ('{tableName}', 'U') IS NOT NULL DROP TABLE {tableName}";
            }
            return q;
        }

        public static string SelectIdentityColumnName(string tableName, string schemaName) // No longer used
        {
            var q = $"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS " +
                    $"WHERE COLUMNPROPERTY(OBJECT_ID(TABLE_SCHEMA + '.' + TABLE_NAME), COLUMN_NAME, 'IsIdentity') = 1 " +
                    $"AND TABLE_NAME = '{tableName}' AND TABLE_SCHEMA = '{schemaName}'";
            return q;
        }

        public static string CheckTableExist(string fullTableName, bool isTempTable)
        {
            string q;
            if (isTempTable)
            {
                q = $"IF OBJECT_ID ('tempdb..[#{fullTableName.Split('#')[1]}', 'U') IS NOT NULL SELECT 1 AS res ELSE SELECT 0 AS res;";
            }
            else
            {
                q = $"IF OBJECT_ID ('{fullTableName}', 'U') IS NOT NULL SELECT 1 AS res ELSE SELECT 0 AS res;";
            }
            return q;
        }

        public static string SelectJoinTable(TableInfo tableInfo)
        {
            string sourceTable = tableInfo.FullTableName;
            string joinTable = tableInfo.FullTempTableName;
            List<string> columnsNames = tableInfo.PropertyColumnNamesDict.Values.ToList();
            List<string> selectByPropertyNames = tableInfo.PropertyColumnNamesDict.Where(a => tableInfo.PrimaryKeysPropertyColumnNameDict.ContainsKey(a.Key)).Select(a => a.Value).ToList();

            var q = $"SELECT {GetCommaSeparatedColumns(columnsNames, "S")} FROM {sourceTable} AS S " +
                    $"JOIN {joinTable} AS J " +
                    $"ON {GetANDSeparatedColumns(selectByPropertyNames, "S", "J", tableInfo.UpdateByPropertiesAreNullable)}";
            return q;
        }

        public static string SetIdentityInsert(string tableName, bool identityInsert)
        {
            string ON_OFF = identityInsert ? "ON" : "OFF";
            var q = $"SET IDENTITY_INSERT {tableName} {ON_OFF};";
            return q;
        }

        public static string MergeTable(TableInfo tableInfo, OperationType operationType)
        {
            string targetTable = tableInfo.FullTableName;
            string sourceTable = tableInfo.FullTempTableName;
            bool keepIdentity = tableInfo.BulkConfig.SqlBulkCopyOptions.HasFlag(SqlBulkCopyOptions.KeepIdentity);
            List<string> primaryKeys = tableInfo.PrimaryKeysPropertyColumnNameDict.Where(a => tableInfo.PropertyColumnNamesDict.ContainsKey(a.Key)).Select(a => a.Value).ToList();
            List<string> columnsNames = tableInfo.PropertyColumnNamesDict.Values.ToList();
            List<string> columnsNamesOnCompare = tableInfo.PropertyColumnNamesCompareDict.Values.ToList();
            List<string> columnsNamesOnUpdate = tableInfo.PropertyColumnNamesUpdateDict.Values.ToList();
            List<string> outputColumnsNames = tableInfo.OutputPropertyColumnNamesDict.Values.ToList();
            List<string> nonIdentityColumnsNames = columnsNames.Where(a => !a.Equals(tableInfo.IdentityColumnName, StringComparison.OrdinalIgnoreCase)).ToList();
            List<string> compareColumnNames = columnsNamesOnCompare.Where(a => !a.Equals(tableInfo.IdentityColumnName, StringComparison.OrdinalIgnoreCase)).ToList();
            List<string> updateColumnNames = columnsNamesOnUpdate.Where(a => !a.Equals(tableInfo.IdentityColumnName, StringComparison.OrdinalIgnoreCase)).ToList();
            List<string> insertColumnsNames = (tableInfo.HasIdentity && !keepIdentity) ? nonIdentityColumnsNames : columnsNames;
            if (tableInfo.DefaultValueProperties.Any()) // Properties with DefaultValue exclude OnInsert but keep OnUpdate
            {
                insertColumnsNames = insertColumnsNames.Where(a => !tableInfo.DefaultValueProperties.Contains(a)).ToList();
            }

            string isUpdateStatsValue = (tableInfo.BulkConfig.CalculateStats) ? ",(CASE $action WHEN 'UPDATE' THEN 1 Else 0 END),(CASE $action WHEN 'DELETE' THEN 1 Else 0 END)" : "";

            if (tableInfo.BulkConfig.PreserveInsertOrder)
            {
                var orderBy = (primaryKeys.Count() == 0) ? "" : $"ORDER BY {GetCommaSeparatedColumns(primaryKeys)}";
                sourceTable = $"(SELECT TOP {tableInfo.NumberOfEntities} * FROM {sourceTable} {orderBy})";
            }

            string textWITH_HOLDLOCK = tableInfo.BulkConfig.WithHoldlock ? " WITH (HOLDLOCK)" : "";

            var q = $"MERGE {targetTable}{textWITH_HOLDLOCK} AS T " +
                    $"USING {sourceTable} AS S " +
                    $"ON {GetANDSeparatedColumns(primaryKeys, "T", "S", tableInfo.UpdateByPropertiesAreNullable)}";
            q += (primaryKeys.Count() == 0) ? "1=0" : "";

            if (operationType == OperationType.Insert || operationType == OperationType.InsertOrUpdate || operationType == OperationType.InsertOrUpdateDelete)
            {
                q += $" WHEN NOT MATCHED BY TARGET THEN INSERT ({GetCommaSeparatedColumns(insertColumnsNames)})" +
                     $" VALUES ({GetCommaSeparatedColumns(insertColumnsNames, "S")})";
            }

            if ((operationType == OperationType.Update || operationType == OperationType.InsertOrUpdate ||
                 operationType == OperationType.InsertOrUpdateDelete) & updateColumnNames.Count > 0)
            {

                q += $" WHEN MATCHED" +
                     (tableInfo.BulkConfig.OmitClauseExistsExcept || tableInfo.HasSpatialType ? "" : // The data type Geography (Spatial) cannot be used as an operand to the UNION, INTERSECT or EXCEPT operators because it is not comparable
                      $" AND EXISTS (SELECT {GetCommaSeparatedColumns(compareColumnNames, "S")}" + // EXISTS better handles nulls
                      $" EXCEPT SELECT {GetCommaSeparatedColumns(compareColumnNames, "T")})"       // EXCEPT does not update if all values are same
                     ) +
                     (!tableInfo.BulkConfig.DoNotUpdateIfTimeStampChanged || tableInfo.TimeStampColumnName == null ? "" :
                      $" AND S.[{tableInfo.TimeStampColumnName}] = T.[{tableInfo.TimeStampColumnName}]"
                     ) +
                     $" THEN UPDATE SET {GetCommaSeparatedColumns(updateColumnNames, "T", "S")}";
            }

            if (operationType == OperationType.InsertOrUpdateDelete)
            {
                q += $" WHEN NOT MATCHED BY SOURCE THEN DELETE";
            }
            if (operationType == OperationType.Delete)
            {
                q += " WHEN MATCHED THEN DELETE";
            }
            if (tableInfo.CreatedOutputTable)
            {
                string commaSeparatedColumnsNames;
                if (operationType == OperationType.InsertOrUpdateDelete || operationType == OperationType.Delete)
                {
                    commaSeparatedColumnsNames = string.Join(", ", outputColumnsNames.Select(x => $"COALESCE(INSERTED.[{x}], DELETED.[{x}])"));
                }
                else
                {
                    commaSeparatedColumnsNames = GetCommaSeparatedColumns(outputColumnsNames, "INSERTED");
                }
                q += $" OUTPUT {commaSeparatedColumnsNames}" + isUpdateStatsValue +
                     $" INTO {tableInfo.FullTempOutputTableName}";
            }
            q += ";";
            return q;
        }

        public static string TruncateTable(string tableName)
        {
            var q = $"TRUNCATE TABLE {tableName};";
            return q;
        }


        /// <summary>
        /// Used for Sqlite, Truncate table 
        /// </summary>
        public static string DeleteTable(string tableName)
        {
            var q = $"DELETE FROM {tableName};" +
                    $"VACUUM;";
            return q;
        }

        // propertColumnsNamesDict used with Sqlite for @parameter to be save from non valid charaters ('', '!', ...) that are allowed as column Names in Sqlite
        public static string GetCommaSeparatedColumns(List<string> columnsNames, string prefixTable = null, string equalsTable = null, Dictionary<string, string> propertColumnsNamesDict = null)
        {
            prefixTable += (prefixTable != null && prefixTable != "@") ? "." : "";
            equalsTable += (equalsTable != null && equalsTable != "@") ? "." : "";

            string commaSeparatedColumns = "";
            foreach (var columnName in columnsNames)
            {
                var equalsParameter = propertColumnsNamesDict == null ? columnName : propertColumnsNamesDict.SingleOrDefault(a => a.Value == columnName).Key;
                commaSeparatedColumns += prefixTable != "" ? $"{prefixTable}[{columnName}]" : $"[{columnName}]";
                commaSeparatedColumns += equalsTable != "" ? $" = {equalsTable}[{equalsParameter}]" : "";
                commaSeparatedColumns += ", ";
            }
            if (commaSeparatedColumns != "")
            {
                commaSeparatedColumns = commaSeparatedColumns.Remove(commaSeparatedColumns.Length - 2, 2); // removes last excess comma and space: ", "
            }
            return commaSeparatedColumns;
        }

        public static string GetCommaSeparatedColumnsAndTypes(List<Tuple<string, string>> columnsNamesAndTypes)
        {
            string commaSeparatedColumns = "";
            foreach (var columnNameAndType in columnsNamesAndTypes)
            {
                commaSeparatedColumns += $"[{columnNameAndType.Item1}] {columnNameAndType.Item2}, ";
            }
            if (commaSeparatedColumns != "")
            {
                commaSeparatedColumns = commaSeparatedColumns.Remove(commaSeparatedColumns.Length - 2, 2); // removes last excess comma and space: ", "
            }
            return commaSeparatedColumns;
        }

        public static string GetANDSeparatedColumns(List<string> columnsNames, string prefixTable = null, string equalsTable = null, bool updateByPropertiesAreNullable = false, Dictionary<string, string> propertColumnsNamesDict = null)
        {
            string commaSeparatedColumns = GetCommaSeparatedColumns(columnsNames, prefixTable, equalsTable, propertColumnsNamesDict);

            if (updateByPropertiesAreNullable)
            {
                string[] columns = commaSeparatedColumns.Split(',');
                string commaSeparatedColumnsNullable = String.Empty;
                foreach (var column in columns)
                {
                    string[] columnTS = column.Split('=');
                    string columnT = columnTS[0].Trim();
                    string columnS = columnTS[1].Trim();
                    string columnNullable = $"({column.Trim()} OR ({columnT} IS NULL AND {columnS} IS NULL))";
                    commaSeparatedColumnsNullable += columnNullable + ", ";
                }
                if (commaSeparatedColumns != "")
                {
                    commaSeparatedColumnsNullable = commaSeparatedColumnsNullable.Remove(commaSeparatedColumnsNullable.Length - 2, 2);
                }
                commaSeparatedColumns = commaSeparatedColumnsNullable;
            }

            string ANDSeparatedColumns = commaSeparatedColumns.Replace(",", " AND");
            return ANDSeparatedColumns;
        }
    }
}
