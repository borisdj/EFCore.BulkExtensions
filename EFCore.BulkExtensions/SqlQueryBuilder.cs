using System;
using System.Collections.Generic;
using System.Linq;

namespace EFCore.BulkExtensions
{
    public static class SqlQueryBuilder
    {
        public static string CreateTable(TableInfo tableInfo)
        {
            string existingTableName = tableInfo.FullTableName;
            string newTableName = tableInfo.FullTempTableName;

            var q = $"SELECT TOP 0 Source.* INTO {newTableName} FROM {existingTableName} ";
            q += $"LEFT JOIN {existingTableName} AS Source ON 1 = 0;"; // removes Identity constrain and makes all columns nullable
            return q;
        }

        public static string DropTable(TableInfo tableInfo)
        {
            return $"DROP TABLE {tableInfo.FullTempTableName};";
        }

        public static string MergeTable(TableInfo tableInfo, OperationType operationType)
        {
            string destinationTable = tableInfo.FullTableName;
            string sourceTable = tableInfo.FullTempTableName;
            string primaryKey = tableInfo.PrimaryKey;
            List<string> columnsNames = tableInfo.PropertyColumnNamesDict.Values.ToList();

            var q = $"MERGE {destinationTable} WITH (HOLDLOCK) USING {sourceTable} " +
                    $"ON {destinationTable}.{primaryKey} = {sourceTable}.{primaryKey} " +
                    $"WHEN MATCHED THEN ";

            if (operationType == OperationType.Update || operationType == OperationType.InsertOrUpdate)
            {
                var nonIdentityColumnsNames = columnsNames.Where(a => a != primaryKey).ToList();
                q += $"UPDATE SET {GetCommaSeparatedColumns(nonIdentityColumnsNames, destinationTable, sourceTable)}";

                if (operationType == OperationType.InsertOrUpdate) // InsertOrUpdate does not work if Table has Identity Column 
                {
                    q += $" WHEN NOT MATCHED THEN INSERT ({GetCommaSeparatedColumns(columnsNames)}) VALUES ({GetCommaSeparatedColumns(columnsNames, sourceTable)})";
                }
            }
            else if (operationType == OperationType.Delete)
            {
                q += "DELETE";
            }
            else
            {
                throw new InvalidOperationException($"Merge does not support {operationType.ToString()} operation.");
            }
            return q + ";";
        }

        public static string GetCommaSeparatedColumns(List<string> columnsNames, string prefixTable = null, string equalsTable = null)
        {
            string commaSeparatedColumns = "";
            foreach (var columnName in columnsNames)
            {
                commaSeparatedColumns += prefixTable != null ? $"{prefixTable}.{columnName}" : columnName;
                commaSeparatedColumns += equalsTable != null ? $" = {equalsTable}.{columnName}" : "";
                commaSeparatedColumns += ",";
            }
            commaSeparatedColumns = commaSeparatedColumns.Remove(commaSeparatedColumns.Length - 1, 1); // removes last excess comma: ","
            return commaSeparatedColumns;
        }
    }
}
