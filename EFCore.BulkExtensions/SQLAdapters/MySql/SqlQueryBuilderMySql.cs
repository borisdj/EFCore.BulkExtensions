using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Data.SqlClient;

namespace EFCore.BulkExtensions.SqlAdapters.MySql;

/// <summary>
///  Contains a list of methods to generate SQL queries required by EFCore
/// </summary>
public static class SqlQueryBuilderMySql
{
    /// <summary>
    /// Generates SQL query to create table copy
    /// </summary>
    /// <param name="existingTableName"></param>
    /// <param name="newTableName"></param>
    /// <param name="useTempDb"></param>
    public static string CreateTableCopy(string existingTableName, string newTableName, bool useTempDb)
    {
        string keywordTemp = useTempDb ? "TEMPORARY " : ""; 
        var query = $"CREATE {keywordTemp}TABLE {newTableName} " +
                $"SELECT * FROM {existingTableName} " +
                "LIMIT 0;";
        query = query.Replace("[", "").Replace("]", "");
        return query;
    }
    /// <summary>
    /// Generates SQL query to drop table
    /// </summary>
    /// <param name="tableName"></param>
    /// <param name="isTempTable"></param>
    /// <returns></returns>
    public static string DropTable(string tableName, bool isTempTable)
    {
        string keywordTemp = isTempTable ? "TEMPORARY " : "";
        var query = $"DROP {keywordTemp}TABLE IF EXISTS {tableName}";
        query = query.Replace("[", "").Replace("]", "");
        return query;
    }
    /// <summary>
    /// Returns a list of columns for the given table
    /// </summary>
    /// <param name="tableInfo"></param>
    /// <param name="operationType"></param>
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

    /// <summary>
    /// Generates SQL merge statement
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <param name="tableInfo"></param>
    /// <param name="operationType"></param>
    /// <exception cref="NotImplementedException"></exception>
    public static string MergeTable<T>(TableInfo tableInfo, OperationType operationType) where T : class
    {
        var columnsList = GetColumnList(tableInfo, operationType);
        if (operationType == OperationType.InsertOrUpdateOrDelete)
        {
            throw new NotImplementedException($"For MySql method {OperationType.InsertOrUpdateOrDelete} is not yet supported. Use combination of InsertOrUpdate with Read and Delete");
        }

        string query;
        var commaSeparatedColumns = SqlQueryBuilder.GetCommaSeparatedColumns(columnsList).Replace("[", "").Replace("]", "");
        var columnsListEquals = GetColumnList(tableInfo, OperationType.Insert);
        var columnsToUpdate = columnsListEquals.Where(c => tableInfo.PropertyColumnNamesUpdateDict.ContainsValue(c)).ToList();
        var equalsColumns = SqlQueryBuilder.GetCommaSeparatedColumns(columnsToUpdate, equalsTable: "EXCLUDED").Replace("[", "").Replace("]", "");
        
        query = $"INSERT INTO {tableInfo.FullTableName} ({commaSeparatedColumns}) " +
                $"SELECT {commaSeparatedColumns} FROM {tableInfo.FullTempTableName} AS EXCLUDED " +
                "ON DUPLICATE KEY UPDATE " +
                $"{equalsColumns};";
        if (tableInfo.CreatedOutputTable)
        {
            //TODO: in case of updating, make sure to respond correctly of condition LAST_INSERT_ID();
            query += $" INSERT INTO {tableInfo.FullTempOutputTableName} " +
                     $"SELECT * FROM {tableInfo.FullTableName} " +
                     $"WHERE {tableInfo.PrimaryKeysPropertyColumnNameDict.FirstOrDefault().Key} >= LAST_INSERT_ID(); ";
        }
        query = query.Replace("[", "").Replace("]", "");

        Dictionary<string, string>? sourceDestinationMappings = tableInfo.BulkConfig.CustomSourceDestinationMappingColumns;
        if (tableInfo.BulkConfig.CustomSourceTableName != null && sourceDestinationMappings != null && sourceDestinationMappings.Count > 0)
        {
            var textSelect = "SELECT ";
            var textFrom = " FROM";
            int startIndex = query.IndexOf(textSelect);
            var qSegment = query[startIndex..query.IndexOf(textFrom)];
            var qSegmentUpdated = qSegment;
            foreach (var mapping in sourceDestinationMappings)
            {
                var propertyFormated = $"{mapping.Value}";
                var sourceProperty = mapping.Key;

                if (qSegment.Contains(propertyFormated))
                {
                    qSegmentUpdated = qSegmentUpdated.Replace(propertyFormated, $"{sourceProperty}");
                }
            }
            if (qSegment != qSegmentUpdated)
            {
                query = query.Replace(qSegment, qSegmentUpdated);
            }
        }
        return query;
    }
    /// <summary>
    /// Generates SQL query to select output from a table
    /// </summary>
    /// <param name="tableInfo"></param>
    /// <returns></returns>
    public static string SelectFromOutputTable(TableInfo tableInfo)
    {
        List<string> columnsNames = tableInfo.OutputPropertyColumnNamesDict.Values.ToList();
        var query = $"SELECT {SqlQueryBuilder.GetCommaSeparatedColumns(columnsNames)} FROM {tableInfo.FullTempOutputTableName} WHERE [{tableInfo.PrimaryKeysPropertyColumnNameDict.Select(x => x.Value).FirstOrDefault()}] IS NOT NULL";
        query = query.Replace("[", "").Replace("]", "");
        return query;
    }
}
