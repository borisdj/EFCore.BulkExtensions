using GBS.Data.GBasedbt;
using Microsoft.EntityFrameworkCore.Metadata;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;

namespace EFCore.BulkExtensions.SqlAdapters.GBase;

/// <summary>
/// Contains a compilation of SQL queries used in EFCore.
/// </summary>
public class GBaseQueryBuilder : SqlQueryBuilder
{
    /// <summary>
    /// Generates SQL query to retrieve the last inserted row id
    /// </summary>
    public static string SelectLastInsertRowId(IProperty key)
    {
        var dbinfoOption = key.ClrType.FullName == "System.Int64" ? "bigserial" : "sqlca.sqlerrd1";
        return $"SELECT DBINFO('{dbinfoOption}')::BIGINT FROM dual";
    }

    /// <summary>
    /// Generates SQL query to insert data into table
    /// </summary>
    /// <param name="tableInfo"></param>
    /// <param name="operationType"></param>
    /// <param name="tableName"></param>
    public static string InsertIntoTable(TableInfo tableInfo, OperationType operationType, string? tableName = null)
    {
        tableName ??= tableInfo.InsertToTempTable ? tableInfo.TempTableName : tableInfo.TableName;
        //if (operationType == OperationType.InsertOrUpdate)
        //{
        //    // for insert or update, use merge into statement,
        //    // insert data into temp table name.
        //    tableName = tableInfo.TempTableName;
        //}

        var tempDict = tableInfo.PropertyColumnNamesDict;
        if (operationType == OperationType.Insert && tableInfo.PropertyColumnNamesDict.Any()) // Only OnInsert omit colums with Default values
        {
            tableInfo.PropertyColumnNamesDict = tableInfo.PropertyColumnNamesDict.Where(a => !tableInfo.DefaultValueProperties.Contains(a.Key)).ToDictionary(a => a.Key, a => a.Value);
        }

        List<string> columnsList = tableInfo.PropertyColumnNamesDict.Values.ToList();
        List<string> propertiesList = new List<string>();
        foreach (var key in tableInfo.PropertyColumnNamesDict.Keys)
        {
            propertiesList.Add("?");
        }

        var commaSeparatedColumns = SqlQueryBuilder.GetCommaSeparatedColumns(columnsList).Replace("[", "").Replace("]", "").Replace(".", "_");
        var commaSeparatedColumnsParams = SqlQueryBuilder.GetCommaSeparatedColumns(propertiesList).Replace("[", "").Replace("]", "").Replace(".", "_"); ;

        var q = $"INSERT INTO {tableName} " +
                $"({commaSeparatedColumns}) " +
                $"VALUES ({commaSeparatedColumnsParams})";

        tableInfo.PropertyColumnNamesDict = tempDict;

        return q + ";";
    }

    /// <summary>
    /// Generates SQL query to merge into table.
    /// </summary>
    /// <param name="tableInfo"></param>
    /// <returns></returns>
    public static string MergeIntoTable(TableInfo tableInfo)
    {
        var q = $"MERGE INTO {tableInfo.TableName} AS TARGET " +
                $"USING {tableInfo.TempTableName} AS SOURCE " +
                $"ON (";

        // on conditions
        List<string> primaryKeys = tableInfo.EntityPKPropertyColumnNameDict.Values.ToList();
        for (int i = 0; i < primaryKeys.Count; i ++)
        {
            if (i == 0)
            { 
                q += $"TARGET.{primaryKeys[i]} = SOURCE.{primaryKeys[i]}";
            }
            else
            {
                q += $" AND TARGET.{primaryKeys[i]} = SOURCE.{primaryKeys[i]}";
            }
        }

        // update
        q += ") ";

        List<string> updateColumnsList = tableInfo.PropertyColumnNamesUpdateDict.Where(a => !tableInfo.DefaultValueProperties.Contains(a.Key)).ToDictionary(a => a.Key, a => a.Value).Values.Where(o => !tableInfo.EntityPKPropertyColumnNameDict.Values.Contains(o)).ToList();
        for (int i = 0; i < updateColumnsList.Count; i ++)
        {
            if (i== 0)
            {
                q += "WHEN MATCHED THEN UPDATE SET ";
                q += $"TARGET.{updateColumnsList[i]} = SOURCE.{updateColumnsList[i]}";
            }
            else
            {
                q += $", TARGET.{updateColumnsList[i]} = SOURCE.{updateColumnsList[i]}";
            }
        }

        // insert
        string insertKeys = string.Empty;
        string insertValues = string.Empty;

        List<string> insertColumnsList = tableInfo.PropertyColumnNamesDict.Where(a => !tableInfo.DefaultValueProperties.Contains(a.Key)).ToDictionary(a => a.Key, a => a.Value).Values.ToList();
        for (int i = 0; i < insertColumnsList.Count; i ++)
        {
            if (i == insertColumnsList.Count - 1)
            {
                insertKeys += $"{insertColumnsList[i]}";
                insertValues += $"SOURCE.{insertColumnsList[i]}";
            }
            else
            {
                insertKeys += $"{insertColumnsList[i]},";
                insertValues += $"SOURCE.{insertColumnsList[i]},";
            }
        }
        q += $" WHEN NOT MATCHED THEN INSERT ( {insertKeys} ) VALUES ( {insertValues} ); ";

        return q;
    }

    internal static string GetCommaSeparatedColumns(List<string> columnsNames, bool inWhereCondition = false)
    {
        string commaSeparatedColumns = "";
        foreach (var columnName in columnsNames)
        {
            commaSeparatedColumns += $"{columnName}";
            commaSeparatedColumns += $" = ?";
            if (inWhereCondition)
            {
                commaSeparatedColumns += " AND ";
            }
            else
            {
                commaSeparatedColumns += ", ";
            }
        }
        if (commaSeparatedColumns != "")
        {
            if (inWhereCondition)
            {
                commaSeparatedColumns = commaSeparatedColumns.Remove(commaSeparatedColumns.Length - 5, 5); // removes last excess comma and space: ", "
            }
            else
            {
                commaSeparatedColumns = commaSeparatedColumns.Remove(commaSeparatedColumns.Length - 2, 2); // removes last excess comma and space: ", "
            }
        }
        return commaSeparatedColumns;
    }

    /// <summary>
    /// Generates SQL query to update table record data
    /// </summary>
    /// <param name="tableInfo"></param>
    /// <param name="tableName"></param>
    public static string UpdateSetTable(TableInfo tableInfo, string? tableName = null)
    {
        tableName ??= tableInfo.TableName;
        List<string> columnsList = tableInfo.PropertyColumnNamesDict.Values.Where(o => !tableInfo.PrimaryKeysPropertyColumnNameDict.Values.Contains(o)).ToList();
        List<string> primaryKeys = tableInfo.PrimaryKeysPropertyColumnNameDict.Select(k => tableInfo.PropertyColumnNamesDict[k.Key]).ToList();
        var commaSeparatedColumns = GBaseQueryBuilder.GetCommaSeparatedColumns(columnsList);
        var commaSeparatedPrimaryKeys = GBaseQueryBuilder.GetCommaSeparatedColumns(primaryKeys, true);

        var q = $"UPDATE {tableName} " +
                $"SET {commaSeparatedColumns} " +
                $"WHERE {commaSeparatedPrimaryKeys};";
        return q;
    }

    /// <summary>
    /// Generates SQL query to delete from table
    /// </summary>
    /// <param name="tableInfo"></param>
    /// <param name="tableName"></param>
    public static string DeleteFromTable(TableInfo tableInfo, string? tableName = null)
    {
        tableName ??= tableInfo.TableName;
        List<string> primaryKeys = tableInfo.PrimaryKeysPropertyColumnNameDict.Select(k => tableInfo.PropertyColumnNamesDict[k.Key]).ToList();
        var commaSeparatedPrimaryKeys = GBaseQueryBuilder.GetCommaSeparatedColumns(primaryKeys, true);

        var q = $"DELETE FROM {tableName} " +
                $"WHERE {commaSeparatedPrimaryKeys};";

        return q;
    }
    /// <summary>
    /// Generates SQL query to delete from table
    /// </summary>
    /// <param name="tableInfo"></param>
    /// <param name="tableName"></param>
    public static string DeleteFromTempTable(TableInfo tableInfo, string? tableName = null)
    {
        tableName ??= tableInfo.TableName;
        List<string> primaryKeys = tableInfo.PrimaryKeysPropertyColumnNameDict.Select(k => tableInfo.PropertyColumnNamesDict[k.Key]).ToList();

        var q = $"DELETE FROM {tableName} " +
                $"WHERE EXISTS(SELECT 1 FROM {tableInfo.TempTableName} WHERE ";
        for (int i = 0; i < primaryKeys.Count; i++)
        {
            if (i == 0)
            {
                q += $"{tableName}.{primaryKeys[i]} = {tableInfo.TempTableName}.{primaryKeys[i]}";
            }
            else
            {
                q += $" AND {tableName}.{primaryKeys[i]} = {tableInfo.TempTableName}.{primaryKeys[i]}";
            }
        }
        q += ");";

        return q;
    }
    /// <summary>
    /// Generates SQL query to create table copy
    /// </summary>
    /// <param name="existingTableName"></param>
    /// <param name="newTableName"></param>
    public static string CreateTableCopy(string? existingTableName, string? newTableName) // Used for BulkRead
    {
        var q = $"CREATE TABLE {newTableName} AS SELECT * FROM {existingTableName} WHERE 1=0;".Replace("[", "").Replace("]", "");
        return q;
    }

    /// <summary>
    /// Generates SQL query to drop table
    /// </summary>
    /// <param name="tableName"></param>
    public static string DropTable(string? tableName)
    {
        string q = $"DROP TABLE IF EXISTS {tableName}".Replace("[", "").Replace("]", "");
        return q;
    }

    /// <summary>
    /// Generates SQL query to truncate table
    /// </summary>
    /// <param name="tableName"></param>
    /// <returns></returns>
    public override string TruncateTable(string tableName)
    {
        var q = $"TRUNCATE TABLE {tableName};".Replace("[", "").Replace("]", "");
        return q;
    }

    /// <inheritdoc/>
    public override DbParameter CreateParameter(string parameterName, object? parameterValue = null)
    {
        return new GbsParameter();
    }

    /// <inheritdoc/>
    public override DbCommand CreateCommand()
    {
        return new GbsCommand();
    }

    /// <inheritdoc/>
    public override DbType Dbtype()
    {
        throw new NotSupportedException();
    }

    /// <inheritdoc/>
    public override string RestructureForBatch(string sql, bool isDelete = false)
    {
        throw new NotSupportedException();
    }

    /// <inheritdoc/>
    public override void SetDbTypeParam(DbParameter parameter, DbType dbType)
    {
        throw new NotSupportedException();
    }
}
