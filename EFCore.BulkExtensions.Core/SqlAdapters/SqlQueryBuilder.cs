﻿using Microsoft.EntityFrameworkCore;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Linq.Expressions;

namespace EFCore.BulkExtensions.SqlAdapters;

/// <summary>
/// Contains a list of methods to generate Adpaters and helpers instances
/// </summary>
public abstract class SqlQueryBuilder
{
    /// <summary>
    /// Restructures a sql query for batch commands
    /// </summary>
    /// <param name="sql"></param>
    /// <param name="isDelete"></param>
    public abstract string RestructureForBatch(string sql, bool isDelete = false);

    /// <summary>
    /// Returns a DbParameter instantiated per provider
    /// </summary>
    public abstract DbParameter CreateParameter(string parameterName, object? parameterValue = null);

    /// <summary>
    /// Returns a DbCommand instantiated per provider
    /// </summary>
    public abstract DbCommand CreateCommand();

    /// <summary>
    /// Returns NpgsqlDbType for PostgreSql parameters. Throws <see cref="NotImplementedException"/> for anothers providers
    /// </summary>
    /// <returns></returns>
    public abstract DbType Dbtype();

    /// <summary>
    /// Returns void. Throws <see cref="NotImplementedException"/> for anothers providers
    /// </summary>
    /// <exception cref="NotImplementedException"></exception>
    public abstract void SetDbTypeParam(DbParameter parameter, DbType dbType);

    /// <summary>
    /// Generates SQL query to select output from a table
    /// </summary>
    /// <param name="tableInfo"></param>
    /// <returns></returns>
    public virtual string SelectFromOutputTable(TableInfo tableInfo)
    {
        List<string> columnsNames = tableInfo.OutputPropertyColumnNamesDict.Values.ToList();
        var q = $"SELECT {GetCommaSeparatedColumns(columnsNames)} " +
                $"FROM {tableInfo.FullTempOutputTableName} " +
                $"WHERE [{tableInfo.PrimaryKeysPropertyColumnNameDict.Select(x => x.Value).FirstOrDefault()}] IS NOT NULL";
        return q;
    }

    /// <summary>
    /// Generates SQL query to create a table copy
    /// </summary>
    /// <param name="existingTableName"></param>
    /// <param name="newTableName"></param>
    /// <param name="tableInfo"></param>
    /// <param name="isOutputTable"></param>
    /// <returns></returns>
    public virtual string CreateTableCopy(string existingTableName, string newTableName, TableInfo tableInfo, bool isOutputTable = false)
    {
        // TODO: (optionaly) if CalculateStats = True but SetOutputIdentity = False then Columns could be ommited from Create and from MergeOutput
        List<string> columnsNames = (isOutputTable ? tableInfo.OutputPropertyColumnNamesDict
                                                   : tableInfo.PropertyColumnNamesDict
                                                   ).Values.ToList();
        string timeStampColumn = "";
        if (tableInfo.TimeStampColumnName != null)
        {
            columnsNames.Remove(tableInfo.TimeStampColumnName);
            timeStampColumn = $", [{tableInfo.TimeStampColumnName}] = CAST('' AS {TableInfo.TimeStampOutColumnType})"; // tsType:varbinary(8)
        }
        string temporalTableColumns = "";
        if (tableInfo.HasTemporalColumns && isOutputTable == true)
        {
            tableInfo.BulkConfig.TemporalColumns.ForEach(columnName => {
                columnsNames.Remove(columnName);
                temporalTableColumns += $", T.[{columnName}]";
            });
        }

        string statsColumn = (tableInfo.BulkConfig.CalculateStats && isOutputTable) ? $", [{tableInfo.SqlActionIUD}] = CAST('' AS char(1))" : "";

        var q = $"SELECT TOP 0 {GetCommaSeparatedColumns(columnsNames, "T")}" + timeStampColumn + temporalTableColumns + statsColumn + " " +
                $"INTO {newTableName} FROM {existingTableName} AS T " +
                $"LEFT JOIN {existingTableName} AS Source ON 1 = 0;"; // removes Identity constrain
        return q;
    }

    /// <summary>
    /// Generates SQL query to alter table columns to nullables
    /// </summary>
    /// <param name="tableName"></param>
    /// <param name="tableInfo"></param>
    /// <returns></returns>
    public static string AlterTableColumnsToNullable(string tableName, TableInfo tableInfo)
    {
        string q = "";
        foreach (var column in tableInfo.ColumnNamesTypesDict)
        {
            string columnName = column.Key;
            string columnType = column.Value;
            if (columnName == tableInfo.TimeStampColumnName)
            {
                columnType = TableInfo.TimeStampOutColumnType;
            }
            columnName = columnName.Replace("]", "]]");
            q += $"ALTER TABLE {tableName} ALTER COLUMN [{columnName}] {columnType}; ";
        }
        return q;
    }

    // Not used for TableCopy since order of columns is not the same as of original table, that is required for the MERGE
    // (instead after creation, columns are Altered to Nullable)
    /// <summary>
    /// Generates SQL query to create table
    /// </summary>
    /// <param name="newTableName"></param>
    /// <param name="tableInfo"></param>
    /// <param name="isOutputTable"></param>
    /// <returns></returns>
    /// <exception cref="InvalidOperationException"></exception>
    public static string CreateTable(string newTableName, TableInfo tableInfo, bool isOutputTable = false)
    {
        List<string> columnsNames = (isOutputTable ? tableInfo.OutputPropertyColumnNamesDict
                                                   : tableInfo.PropertyColumnNamesDict
                                                   ).Values.ToList();
        if (tableInfo.TimeStampColumnName != null)
        {
            columnsNames.Remove(tableInfo.TimeStampColumnName);
        }
        var columnsNamesAndTypes = new List<Tuple<string, string>>();
        foreach (var columnName in columnsNames)
        {
            if (!tableInfo.ColumnNamesTypesDict.TryGetValue(columnName, out string? columnType))
            {
                throw new InvalidOperationException($"Column Type not found in ColumnNamesTypesDict for column: '{columnName}'");
            }
            columnsNamesAndTypes.Add(new Tuple<string, string>(columnName, columnType));
        }
        if (tableInfo.BulkConfig.CalculateStats && isOutputTable)
        {
            columnsNamesAndTypes.Add(new Tuple<string, string>("[SqlActionIUD]", "char(1)"));
        }
        var q = $"CREATE TABLE {newTableName} ({GetCommaSeparatedColumnsAndTypes(columnsNamesAndTypes)});";
        return q;
    }

    /// <summary>
    /// Generates SQL query to add a column
    /// </summary>
    /// <param name="fullTableName"></param>
    /// <param name="columnName"></param>
    /// <param name="columnType"></param>
    /// <returns></returns>
    public static string AddColumn(string fullTableName, string columnName, string columnType)
    {
        var q = $"ALTER TABLE {fullTableName} ADD [{columnName}] {columnType};";
        return q;
    }

    /// <summary>
    /// Generates SQL query to select count updated from output table
    /// </summary>
    /// <param name="tableInfo"></param>
    /// <returns></returns>
    public static string SelectCountIsUpdateFromOutputTable(TableInfo tableInfo)
    {
        return SelectCountColumnFromOutputTable(tableInfo, "SqlActionIUD", "U");
    }

    /// <summary>
    /// Generates SQL query to select count deleted from output table
    /// </summary>
    /// <param name="tableInfo"></param>
    /// <returns></returns>
    public static string SelectCountIsDeleteFromOutputTable(TableInfo tableInfo)
    {
        return SelectCountColumnFromOutputTable(tableInfo, "SqlActionIUD", "D");
    }

    /// <summary>
    /// Generates SQL query to select column count from output table
    /// </summary>
    /// <param name="tableInfo"></param>
    /// <param name="columnName"></param>
    /// <param name="columnValue"></param>
    /// <returns></returns>
    public static string SelectCountColumnFromOutputTable(TableInfo tableInfo, string columnName, string columnValue)
    {
        var q = $"SELECT COUNT(*) FROM {tableInfo.FullTempOutputTableName} WHERE [{columnName}] = '{columnValue}'";
        return q;
    }

    /// <summary>
    /// Generates SQL query to drop table
    /// </summary>
    /// <param name="tableName"></param>
    /// <param name="isTempTable"></param>
    /// <returns></returns>
    public virtual string DropTable(string tableName, bool isTempTable)
    {
        string q;
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

    /// <summary>
    /// Generates SQL query to to select identity column
    /// </summary>
    /// <param name="tableName"></param>
    /// <param name="schemaName"></param>
    /// <returns></returns>
    public static string SelectIdentityColumnName(string tableName, string schemaName) // No longer used
    {
        var q = $"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS " +
                $"WHERE COLUMNPROPERTY(OBJECT_ID(TABLE_SCHEMA + '.' + TABLE_NAME), COLUMN_NAME, 'IsIdentity') = 1 " +
                $"AND TABLE_NAME = '{tableName}' AND TABLE_SCHEMA = '{schemaName}'";
        return q;
    }

    /// <summary>
    /// Generates SQL query to check whether a table exists
    /// </summary>
    /// <param name="fullTableName"></param>
    /// <param name="isTempTable"></param>
    /// <returns></returns>
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

    /// <summary>
    /// Generates SQL query to join table
    /// </summary>
    /// <param name="tableInfo"></param>
    /// <returns></returns>
    public static string SelectJoinTable(TableInfo tableInfo)
    {
        string sourceTable = tableInfo.FullTableName;
        string joinTable = tableInfo.FullTempTableName;
        List<string> columnsNames = tableInfo.PropertyColumnNamesDict.Values.ToList();
        List<string> selectByPropertyNames = tableInfo.PropertyColumnNamesDict.Where(a => tableInfo.PrimaryKeysPropertyColumnNameDict.ContainsKey(a.Key)).Select(a => a.Value).ToList();

        var q = $"SELECT {GetCommaSeparatedColumns(columnsNames, "S")} " +
                $"FROM {sourceTable} AS S " +
                $"JOIN {joinTable} AS J " +
                $"ON {GetANDSeparatedColumns(selectByPropertyNames, "S", "J", tableInfo.UpdateByPropertiesAreNullable)}";
        return q;
    }

    /// <summary>
    /// Generates SQL query to set identity insert
    /// </summary>
    /// <param name="tableName"></param>
    /// <param name="identityInsert"></param>
    /// <returns></returns>
    public static string SetIdentityInsert(string tableName, bool identityInsert)
    {
        string ON_OFF = identityInsert ? "ON" : "OFF";
        var q = $"SET IDENTITY_INSERT {tableName} {ON_OFF};";
        return q;
    }

    /// <summary>
    /// Generates SQL query to merge table
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <param name="context"></param>
    /// <param name="tableInfo"></param>
    /// <param name="operationType"></param>
    /// <param name="entityPropertyWithDefaultValue"></param>
    /// <returns></returns>
    /// <exception cref="InvalidBulkConfigException"></exception>
    public static (string sql, IEnumerable<object> parameters) MergeTable<T>(BulkContext context, TableInfo tableInfo, OperationType operationType,
                                                                             IEnumerable<string>? entityPropertyWithDefaultValue = default) where T : class
    {
        List<object> parameters = new();
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
            var defaults = insertColumnsNames.Where(a => tableInfo.DefaultValueProperties.Contains(a)).ToList();
            //If the entities assign value to properties with default value, don't skip this property 
            if (entityPropertyWithDefaultValue != default)
                defaults = defaults.Where(x => entityPropertyWithDefaultValue.Contains(x)).ToList();
            insertColumnsNames = insertColumnsNames.Where(a => !defaults.Contains(a)).ToList();
        }

        string isUpdateStatsValue = "";
        if (tableInfo.BulkConfig.CalculateStats)
            isUpdateStatsValue = ",SUBSTRING($action, 1, 1)";

        if (tableInfo.BulkConfig.PreserveInsertOrder)
        {
            int numberOfEntities = tableInfo.BulkConfig.CustomSourceTableName == null ? tableInfo.NumberOfEntities
                                                                                      : int.MaxValue;
            var orderBy = (primaryKeys.Count == 0) ? string.Empty
                                                   : $"ORDER BY {GetCommaSeparatedColumns(primaryKeys)}";
            sourceTable = $"(SELECT TOP {numberOfEntities} * FROM {sourceTable} {orderBy})";
        }

        string textWITH_HOLDLOCK = tableInfo.BulkConfig.WithHoldlock ? " WITH (HOLDLOCK)" : string.Empty;

        var q = $"MERGE {targetTable}{textWITH_HOLDLOCK} AS T " +
                $"USING {sourceTable} AS S " +
                $"ON {GetANDSeparatedColumns(primaryKeys, "T", "S", tableInfo.UpdateByPropertiesAreNullable)}";
        q += (primaryKeys.Count == 0) ? "1=0" : string.Empty;

        if (operationType == OperationType.Insert || operationType == OperationType.InsertOrUpdate || operationType == OperationType.InsertOrUpdateOrDelete)
        {
            q += $" WHEN NOT MATCHED BY TARGET " +
                 $"THEN INSERT ({GetCommaSeparatedColumns(insertColumnsNames)}) " +
                 $"VALUES ({GetCommaSeparatedColumns(insertColumnsNames, "S")})";
        }

        q = q.Replace("INSERT () VALUES ()", "INSERT DEFAULT VALUES"); // case when table has only one column that is Identity

        if (operationType == OperationType.Update || operationType == OperationType.InsertOrUpdate || operationType == OperationType.InsertOrUpdateOrDelete)
        {
            if (updateColumnNames.Count == 0 && operationType == OperationType.Update)
            {
                throw new InvalidBulkConfigException($"'Bulk{operationType}' operation can not have zero columns to update.");
            }
            else if (updateColumnNames.Count > 0)
            {
                q += $" WHEN MATCHED" +
                     (tableInfo.BulkConfig.OmitClauseExistsExcept || tableInfo.HasSpatialType ? string.Empty : // The data type Geography (Spatial) cannot be used as an operand to the UNION, INTERSECT or EXCEPT operators because it is not comparable
                      $" AND EXISTS (SELECT {GetCommaSeparatedColumns(compareColumnNames, "S")}" + // EXISTS better handles nulls
                      $" EXCEPT SELECT {GetCommaSeparatedColumns(compareColumnNames, "T")})"       // EXCEPT does not update if all values are same
                     ) +
                     (!tableInfo.BulkConfig.DoNotUpdateIfTimeStampChanged || tableInfo.TimeStampColumnName == null ? string.Empty :
                      $" AND S.[{tableInfo.TimeStampColumnName}] = T.[{tableInfo.TimeStampColumnName}]"
                     ) +
                     (tableInfo.BulkConfig.OnConflictUpdateWhereSql != null ? $" AND {tableInfo.BulkConfig.OnConflictUpdateWhereSql("T", "S")}" : string.Empty) +
                     $" THEN UPDATE SET {GetCommaSeparatedColumns(updateColumnNames, "T", "S")}";
            }
        }

        if (operationType == OperationType.InsertOrUpdateOrDelete)
        {
            string syncFilterCondition = string.Empty;
            if (tableInfo.BulkConfig.SynchronizeFilter != null)
            {
                var querable = context.DbContext.Set<T>().IgnoreQueryFilters().IgnoreAutoIncludes()
                                               .Where((Expression<Func<T, bool>>)tableInfo.BulkConfig.SynchronizeFilter);

                var (Sql, TableAlias, TableAliasSufixAs, TopStatement, LeadingComments, InnerParameters) = BatchUtil.GetBatchSql(querable, context, false);
                var whereClause = $"{Environment.NewLine}WHERE ";
                int wherePos = Sql.IndexOf(whereClause, StringComparison.OrdinalIgnoreCase);
                if (wherePos > 0)
                {
                    var sqlWhere = Sql[(wherePos + whereClause.Length)..];
                    sqlWhere = sqlWhere.Replace($"[{TableAlias}].", string.Empty);

                    syncFilterCondition = " AND " + sqlWhere;
                    parameters.AddRange(InnerParameters);
                }
                else
                {
                    throw new InvalidBulkConfigException($"'Bulk{operationType}' SynchronizeFilter expression can not be translated to SQL");
                }
            }

            q += " WHEN NOT MATCHED BY SOURCE" + syncFilterCondition;

            string softDeleteAssignment = string.Empty;
            if (tableInfo.BulkConfig.SynchronizeSoftDelete != null)
            {
                var querable = context.DbContext.Set<T>().IgnoreQueryFilters().IgnoreAutoIncludes();
                var expression = (Expression<Func<T, T>>)tableInfo.BulkConfig.SynchronizeSoftDelete;
                var (sqlOriginal, sqlParameters) = BatchUtil.GetSqlUpdate(querable, context, typeof(T), expression);
                var databaseType = context.Server.Type;
                var (tableAlias, _) = context.Dialect.GetBatchSqlReformatTableAliasAndTopStatement(sqlOriginal, databaseType);

                var sql = sqlOriginal.Replace($"[{tableAlias}]", "T");
                int indexFrom = sql.IndexOf(".") - 1;
                int indexTo = sql.IndexOf(Environment.NewLine) - indexFrom;
                softDeleteAssignment = sql.Substring(indexFrom, indexTo);
                softDeleteAssignment = softDeleteAssignment.TrimEnd();
                parameters.AddRange(sqlParameters);
            }

            q += (softDeleteAssignment != string.Empty) ? $" THEN UPDATE SET {softDeleteAssignment}"
                                                        : $" THEN DELETE";
        }
        if (operationType == OperationType.Delete)
        {
            q += " WHEN MATCHED THEN DELETE";
        }
        if (tableInfo.CreateOutputTable)
        {
            string commaSeparatedColumnsNames;
            if (operationType == OperationType.InsertOrUpdateOrDelete || operationType == OperationType.Delete)
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
        if(tableInfo.BulkConfig.UseOptionLoopJoin)
        {
            q += " OPTION (LOOP JOIN)";
        }
        
        q += ";";

        Dictionary<string, string> sourceDestinationMappings = tableInfo.BulkConfig.CustomSourceDestinationMappingColumns ?? new();
        if (tableInfo.BulkConfig.CustomSourceTableName != null
            && sourceDestinationMappings != null
            && sourceDestinationMappings.Count > 0)
        {
            var textOrderBy = "ORDER BY ";
            var textAsS = " AS S";
            int startIndex = q.IndexOf(textOrderBy);
            var qSegment = q[startIndex..q.IndexOf(textAsS)];
            var qSegmentUpdated = qSegment;
            foreach (var mapping in sourceDestinationMappings)
            {
                var propertySourceFormated = $"S.[{mapping.Value}]";
                var propertyFormated = $"[{mapping.Value}]";
                var sourceProperty = mapping.Key;

                if (qSegment.Contains(propertyFormated))
                {
                    qSegmentUpdated = qSegmentUpdated.Replace(propertyFormated, $"[{sourceProperty}]");
                }
                if (q.Contains(propertySourceFormated))
                {
                    q = q.Replace(propertySourceFormated, $"S.[{sourceProperty}]");
                }
            }
            if (qSegment != qSegmentUpdated)
            {
                q = q.Replace(qSegment, qSegmentUpdated);
            }
        }

        return (sql: q, parameters);
    }

    /// <summary>
    /// Generates SQL query to truncate table
    /// </summary>
    /// <param name="tableName"></param>
    /// <returns></returns>
    public virtual string TruncateTable(string tableName)
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

    /// <summary>
    /// Generates SQL query to get comma seperated column
    /// </summary>
    /// <param name="columnsNames"></param>
    /// <param name="prefixTable"></param>
    /// <param name="equalsTable"></param>
    /// <param name="propertColumnsNamesDict"></param>
    /// <returns></returns>
    public static string GetCommaSeparatedColumns(List<string> columnsNames, string? prefixTable = null, string? equalsTable = null,
                                                  Dictionary<string, string>? propertColumnsNamesDict = null)
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


    /// <summary>
    /// Generates a comma seperated column list with its SQL data type
    /// </summary>
    /// <param name="columnsNamesAndTypes"></param>
    /// <returns></returns>
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

    /// <summary>
    /// Generates SQL query to seperate columns
    /// </summary>
    /// <param name="columnsNames"></param>
    /// <param name="prefixTable"></param>
    /// <param name="equalsTable"></param>
    /// <param name="updateByPropertiesAreNullable"></param>
    /// <param name="propertColumnsNamesDict"></param>
    /// <returns></returns>
    public static string GetANDSeparatedColumns(List<string> columnsNames, string? prefixTable = null, string? equalsTable = null, bool updateByPropertiesAreNullable = false,
                                                Dictionary<string, string>? propertColumnsNamesDict = null)
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
