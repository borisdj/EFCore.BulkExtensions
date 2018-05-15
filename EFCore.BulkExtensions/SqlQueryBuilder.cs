using System.Collections.Generic;
using System.Linq;

namespace EFCore.BulkExtensions
{
    public static class SqlQueryBuilder
    {
        public static string CreateTableCopy(string existingTableName, string newTableName, TableInfo tableInfo)
        {
            List<string> columnsNames = tableInfo.PropertyColumnNamesDict.Values.ToList();
            var q = $"SELECT TOP 0 {GetCommaSeparatedColumns(columnsNames, "T")} " +
                    $"INTO {newTableName} FROM {existingTableName} AS T " +
                    $"LEFT JOIN {existingTableName} AS Source ON 1 = 0;"; // removes Identity constrain
            return q;
        }
        
        public static string SelectFromOutputTable(TableInfo tableInfo)
        {
            List<string> columnsNames = tableInfo.PropertyColumnNamesDict.Values.ToList();
            string timeStampColumnNull = tableInfo.TimeStampColumn != null ? $", {tableInfo.TimeStampColumn} = NULL" : "";
            return $"SELECT {GetCommaSeparatedColumns(columnsNames)}{timeStampColumnNull} FROM {tableInfo.FullTempOutputTableName}";
        }

        public static string DropTable(string tableName)
        {
            return $"DROP TABLE {tableName}";
        }

        public static string SelectIsIdentity(string tableName, string idColumnName)
        {
            return $"SELECT columnproperty(object_id('{tableName}'),'{idColumnName}','IsIdentity');";
        }

        public static string MergeTable(TableInfo tableInfo, OperationType operationType)
        {
            string targetTable = tableInfo.FullTableName;
            string sourceTable = tableInfo.FullTempTableName;
            List<string> primaryKeys = tableInfo.PrimaryKeys.Select(k => tableInfo.PropertyColumnNamesDict[k]).ToList();
            List<string> columnsNames = tableInfo.PropertyColumnNamesDict.Values.ToList();
            List<string> nonIdentityColumnsNames = columnsNames.Where(a => !primaryKeys.Contains(a)).ToList();
            List<string> insertColumnsNames = tableInfo.HasIdentity ? nonIdentityColumnsNames : columnsNames;

            if (tableInfo.BulkConfig.PreserveInsertOrder)
                sourceTable = $"(SELECT TOP {tableInfo.NumberOfEntities} * FROM {sourceTable} ORDER BY {GetCommaSeparatedColumns(primaryKeys)})";

            string textWITH_HOLDLOCK = tableInfo.BulkConfig.WithHoldlock ? " WITH (HOLDLOCK)" : "";

            var q = $"MERGE {targetTable}{textWITH_HOLDLOCK} AS T " +
                    $"USING {sourceTable} AS S " +
                    $"ON {GetANDSeparatedColumns(primaryKeys, "T", "S")}";

            if (operationType == OperationType.Insert || operationType == OperationType.InsertOrUpdate)
            {
                q += $" WHEN NOT MATCHED THEN INSERT ({GetCommaSeparatedColumns(insertColumnsNames)})" +
                     $" VALUES ({GetCommaSeparatedColumns(insertColumnsNames, "S")})";
            }
            if (operationType == OperationType.Update || (operationType == OperationType.InsertOrUpdate && nonIdentityColumnsNames.Count > 0))
            {
                q += $" WHEN MATCHED THEN UPDATE SET {GetCommaSeparatedColumns(nonIdentityColumnsNames, "T", "S")}";
            }
            if (operationType == OperationType.Delete)
            {
                q += " WHEN MATCHED THEN DELETE";
            }

            if (tableInfo.BulkConfig.SetOutputIdentity)
            {
                q += $" OUTPUT {GetCommaSeparatedColumns(columnsNames, "INSERTED")}" +
                     $" INTO {tableInfo.FullTempOutputTableName}";
            }

            return q + ";";
        }

        public static string GetCommaSeparatedColumns(List<string> columnsNames, string prefixTable = null, string equalsTable = null)
        {
            string commaSeparatedColumns = "";
            foreach (var columnName in columnsNames)
            {
                commaSeparatedColumns += prefixTable != null ? $"{prefixTable}.[{columnName}]" : $"[{columnName}]";
                commaSeparatedColumns += equalsTable != null ? $" = {equalsTable}.[{columnName}]" : "";
                commaSeparatedColumns += ", ";
            }
            commaSeparatedColumns = commaSeparatedColumns.Remove(commaSeparatedColumns.Length - 2, 2); // removes last excess comma and space: ", "
            return commaSeparatedColumns;
        }

        public static string GetANDSeparatedColumns(List<string> columnsNames, string prefixTable = null, string equalsTable = null)
        {
            string commaSeparatedColumns = GetCommaSeparatedColumns(columnsNames, prefixTable, equalsTable);
            string andSeparatedColumns = commaSeparatedColumns.Replace(",", " AND");
            return andSeparatedColumns;
        }
    }
}
