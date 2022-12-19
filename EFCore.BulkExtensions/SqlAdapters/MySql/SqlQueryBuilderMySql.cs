using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Data.SqlClient;

namespace EFCore.BulkExtensions.SqlAdapters.MySql;

/// <summary>
///  Contains a list of methods to generate SQL queries required by EFCore
/// </summary>
public class SqlQueryBuilderMySql : QueryBuilderExtensions
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
        var firstPrimaryKey = tableInfo.PrimaryKeysPropertyColumnNameDict.FirstOrDefault().Key;
        if(operationType == OperationType.Delete)
        {
            query = "delete A " +
                    $"FROM {tableInfo.FullTableName} AS A " +
                    $"INNER JOIN {tableInfo.FullTempTableName} B on A.{firstPrimaryKey} = B.{firstPrimaryKey}; ";
        }
        else
        {
            var commaSeparatedColumns = SqlQueryBuilder.GetCommaSeparatedColumns(columnsList).Replace("[", "").Replace("]", "");
            var columnsListEquals = GetColumnList(tableInfo, OperationType.Insert);
            var columnsToUpdate = columnsListEquals.Where(c => tableInfo.PropertyColumnNamesUpdateDict.ContainsValue(c)).ToList();
            var equalsColumns = SqlQueryBuilder.GetCommaSeparatedColumns(columnsToUpdate, equalsTable: "EXCLUDED").Replace("[", "").Replace("]", "");
        
            query = $"INSERT INTO {tableInfo.FullTableName} ({commaSeparatedColumns}) " +
                    $"SELECT {commaSeparatedColumns} FROM {tableInfo.FullTempTableName} AS EXCLUDED " +
                    "ON DUPLICATE KEY UPDATE " +
                    $"{equalsColumns}; ";
            if (tableInfo.CreatedOutputTable)
            {
                if (operationType == OperationType.Insert || operationType == OperationType.InsertOrUpdate)
                {
                    query += $"INSERT INTO {tableInfo.FullTempOutputTableName} " +
                             $"SELECT * FROM {tableInfo.FullTableName} " +
                             $"WHERE {firstPrimaryKey} >= LAST_INSERT_ID() " +
                             $"AND {firstPrimaryKey} < LAST_INSERT_ID() + row_count(); ";
                }

                if (operationType == OperationType.Update)
                {
                    query += $"INSERT INTO {tableInfo.FullTempOutputTableName} " +
                             $"SELECT * FROM {tableInfo.FullTempTableName} ";
                }

                if (operationType == OperationType.InsertOrUpdate)
                {
                    query += $"INSERT INTO {tableInfo.FullTempOutputTableName} " +
                             $"SELECT A.* FROM {tableInfo.FullTempTableName} A " +
                             $"LEFT OUTER JOIN {tableInfo.FullTempOutputTableName} B " +
                             $" ON A.{firstPrimaryKey} = B.{firstPrimaryKey} " +
                             $"WHERE  B.{firstPrimaryKey} IS NULL; ";
                }
            
            }
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
    public override string SelectFromOutputTable(TableInfo tableInfo)
    {
        List<string> columnsNames = tableInfo.OutputPropertyColumnNamesDict.Values.ToList();
        var query = $"SELECT {SqlQueryBuilder.GetCommaSeparatedColumns(columnsNames)} FROM {tableInfo.FullTempOutputTableName} WHERE [{tableInfo.PrimaryKeysPropertyColumnNameDict.Select(x => x.Value).FirstOrDefault()}] IS NOT NULL";
        query = query.Replace("[", "").Replace("]", "");
        return query;
    }

    /// <summary>
    /// Generates SQL query to create a unique constraint
    /// </summary>
    /// <param name="tableInfo"></param>
    public static string CreateUniqueConstrain(TableInfo tableInfo)
    {
        var tableName = tableInfo.TableName;
        var schemaFormated = tableInfo.Schema == null ? "" : $@"`{tableInfo.Schema}`.";
        var fullTableNameFormated = $@"{schemaFormated}`{tableName}`";

        var uniqueColumnNames = tableInfo.PrimaryKeysPropertyColumnNameDict.Values.ToList();
        var uniqueColumnNamesDash = string.Join("_", uniqueColumnNames);
        var schemaDash = tableInfo.Schema == null ? "" : $"{tableInfo.Schema}_";
        var uniqueConstrainName = $"tempUniqueIndex_{schemaDash}{tableName}_{uniqueColumnNamesDash}";

        var uniqueColumnNamesComma = string.Join(",", uniqueColumnNames); // TODO When Column is string without defined max length, it should be UNIQUE (`Name`(255)); otherwise exception: BLOB/TEXT column 'Name' used in key specification without a key length'
        uniqueColumnNamesComma = "`" + uniqueColumnNamesComma;
        uniqueColumnNamesComma = uniqueColumnNamesComma.Replace(",", "`, `");
        var uniqueColumnNamesFormated = uniqueColumnNamesComma.TrimEnd(',');
        uniqueColumnNamesFormated = uniqueColumnNamesFormated + "`";

        var q = $@"ALTER TABLE {fullTableNameFormated} " +
                $@"ADD CONSTRAINT `{uniqueConstrainName}` " +
                $@"UNIQUE ({uniqueColumnNamesFormated})";
        return q;
    }

    /// <summary>
    /// Generates SQL query to drop a unique contstraint
    /// </summary>
    /// <param name="tableInfo"></param>
    public static string DropUniqueConstrain(TableInfo tableInfo)
    {
        var tableName = tableInfo.TableName;
        var schemaFormated = tableInfo.Schema == null ? "" : $@"`{tableInfo.Schema}`.";
        var fullTableNameFormated = $@"{schemaFormated}`{tableName}`";

        var uniqueColumnNames = tableInfo.PrimaryKeysPropertyColumnNameDict.Values.ToList();
        var uniqueColumnNamesDash = string.Join("_", uniqueColumnNames);
        var schemaDash = tableInfo.Schema == null ? "" : $"{tableInfo.Schema}_";
        var uniqueConstrainName = $"tempUniqueIndex_{schemaDash}{tableName}_{uniqueColumnNamesDash}";

        var q = $@"ALTER TABLE {fullTableNameFormated} " +
                $@"DROP INDEX `{uniqueConstrainName}`;";
        return q;
    }

    /// <summary>
    /// Generates SQL query to chaeck if a unique constrain exist
    /// </summary>
    /// <param name="tableInfo"></param>
    public static string HasUniqueConstrain(TableInfo tableInfo)
    {
        var tableName = tableInfo.TableName;

        var uniqueColumnNames = tableInfo.PrimaryKeysPropertyColumnNameDict.Values.ToList();
        var uniqueColumnNamesDash = string.Join("_", uniqueColumnNames);
        var schemaDash = tableInfo.Schema == null ? "" : $"{tableInfo.Schema}_";
        var uniqueConstrainName = $"tempUniqueIndex_{schemaDash}{tableName}_{uniqueColumnNamesDash}";

        var q = $@"SELECT DISTINCT CONSTRAINT_NAME FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS WHERE " +
                $@"CONSTRAINT_TYPE = 'UNIQUE' AND CONSTRAINT_NAME = '{uniqueConstrainName}';";
        return q;
    }


    /// <summary>
    /// Restructures a sql query for batch commands
    /// </summary>
    /// <param name="sql"></param>
    /// <param name="isDelete"></param>
    public override string RestructureForBatch(string sql, bool isDelete = false)
    {
        return sql;
    }

    /// <summary>
    /// Returns a DbParameters intanced per provider
    /// </summary>
    /// <param name="sqlParameter"></param>
    /// <returns></returns>
    public override object CreateParameter(SqlParameter sqlParameter)
    {
        return sqlParameter;
    }

    /// <inheritdoc/>
    public override object Dbtype()
    {
        throw new NotImplementedException();
    }

    /// <inheritdoc/>
    public override void SetDbTypeParam(object npgsqlParameter, object dbType)
    {
        throw new NotImplementedException();
    }
}
