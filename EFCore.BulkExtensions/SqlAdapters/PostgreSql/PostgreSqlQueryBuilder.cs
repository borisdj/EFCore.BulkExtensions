using Microsoft.Data.SqlClient;
using System;
using System.Collections.Generic;
using System.Linq;

namespace EFCore.BulkExtensions.SqlAdapters.PostgreSql;

/// <summary>
/// Contains a list of methods to generate SQL queries required by EFCore
/// </summary>
public class PostgreSqlQueryBuilder : SqlQueryBuilder
{

    /// <summary>
    /// Generates SQL query to create Output table for Stats
    /// </summary>
    /// <param name="newTableName"></param>
    /// <param name="useTempDb"></param>
    public static string CreateOutputStatsTable(string newTableName, bool useTempDb)
    {
        string keywordTEMP = useTempDb ? "TEMP " : ""; // "TEMP " or "TEMPORARY "
        var q = @$"CREATE {keywordTEMP}TABLE IF NOT EXISTS {newTableName} (""xmaxNumber"" xid)"; // col name can't be just 'xmax' - conflicts with system column
        q = q.Replace("[", @"""").Replace("]", @"""");
        return q;
    }

    /// <summary>
    /// Generates SQL query to create table copy
    /// </summary>
    /// <param name="existingTableName"></param>
    /// <param name="newTableName"></param>
    /// <param name="useTempDb"></param>
    public static string CreateTableCopy(string existingTableName, string newTableName, bool useTempDb)
    {
        string keywordTEMP = useTempDb ? "TEMP " : ""; // "TEMP " or "TEMPORARY "
        var q = $"CREATE {keywordTEMP}TABLE {newTableName} " +
                $"AS TABLE {existingTableName} " +
                $"WITH NO DATA;";
        q = q.Replace("[", @"""").Replace("]", @"""");
        return q;
    }

    /// <summary>
    /// Generates SQL to copy table columns from STDIN 
    /// </summary>
    /// <param name="tableInfo"></param>
    /// <param name="operationType"></param>
    /// <param name="tableName"></param>
    public static string InsertIntoTable(TableInfo tableInfo, OperationType operationType, string? tableName = null)
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
            throw new NotImplementedException($"For Postgres method {OperationType.InsertOrUpdateOrDelete} is not yet supported. Use combination of InsertOrUpdate with Read and Delete");
        }

        string q;
        bool appendReturning = false;
        if (operationType == OperationType.Read)
        {
            var readByColumns = SqlQueryBuilder.GetCommaSeparatedColumns(tableInfo.PrimaryKeysPropertyColumnNameDict.Values.ToList()); //, tableInfo.FullTableName, tableInfo.FullTempTableName

            q = $"SELECT {tableInfo.FullTableName}.* FROM {tableInfo.FullTableName} " +
                $"JOIN {tableInfo.FullTempTableName} " +
                $"USING ({readByColumns})"; //$"ON ({tableInfo.FullTableName}.readByColumns = {tableInfo.FullTempTableName}.readByColumns);";
        }
        else if (operationType == OperationType.Delete)
        {
            var deleteByColumns = SqlQueryBuilder.GetCommaSeparatedColumns(tableInfo.PrimaryKeysPropertyColumnNameDict.Values.ToList(), tableInfo.FullTableName, tableInfo.FullTempTableName);
            deleteByColumns = deleteByColumns.Replace(",", " AND")
                                             .Replace("[", @"""").Replace("]", @"""");

            q = $"DELETE FROM {tableInfo.FullTableName} " +
                $"USING {tableInfo.FullTempTableName} " +
                $"WHERE {deleteByColumns}";
        }
        else if (operationType == OperationType.Update)
        {
            var columnsListEquals = GetColumnList(tableInfo, OperationType.Insert);
            var columnsToUpdate = columnsListEquals.Where(tableInfo.PropertyColumnNamesUpdateDict.ContainsValue).ToList();

            var updateByColumns = SqlQueryBuilder.GetANDSeparatedColumns(tableInfo.PrimaryKeysPropertyColumnNameDict.Values.ToList(),
                prefixTable: tableInfo.FullTableName, equalsTable: tableInfo.FullTempTableName).Replace("[", @"""").Replace("]", @"""");
            var equalsColumns = SqlQueryBuilder.GetCommaSeparatedColumns(columnsToUpdate,
                equalsTable: tableInfo.FullTempTableName).Replace("[", @"""").Replace("]", @"""");

            q = $"UPDATE {tableInfo.FullTableName} SET {equalsColumns} " +
                $"FROM {tableInfo.FullTempTableName} " +
                $"WHERE {updateByColumns}";

            appendReturning = true;
        }
        else
        {
            var columnsListInsert = columnsList;
            var textValueFirstPK = tableInfo.TextValueFirstPK;
            if (textValueFirstPK != null && (textValueFirstPK == "0" || textValueFirstPK.ToString() == Guid.Empty.ToString() || textValueFirstPK.ToString() == ""))
            {
                //  PKs can be all set or all empty in which case DB generates it, can not have it combined in one list when using InsetOrUpdate  
                columnsListInsert = columnsList.Where(tableInfo.PropertyColumnNamesUpdateDict.ContainsValue).ToList();
            }
            var commaSeparatedColumns = SqlQueryBuilder.GetCommaSeparatedColumns(columnsListInsert).Replace("[", @"""").Replace("]", @"""");

            var updateByColumns = SqlQueryBuilder.GetCommaSeparatedColumns(tableInfo.PrimaryKeysPropertyColumnNameDict.Values.ToList()).Replace("[", @"""").Replace("]", @"""");

            var columnsListEquals = GetColumnList(tableInfo, OperationType.Insert);
            var columnsToUpdate = columnsListEquals.Where(c => tableInfo.PropertyColumnNamesUpdateDict.ContainsValue(c)).ToList();
            var equalsColumns = SqlQueryBuilder.GetCommaSeparatedColumns(columnsToUpdate, equalsTable: "EXCLUDED").Replace("[", @"""").Replace("]", @"""");

            int subqueryLimit = tableInfo.BulkConfig.ApplySubqueryLimit;
            var subqueryText = subqueryLimit > 0 ? $"LIMIT {subqueryLimit} " : "";
            bool onUpdateDoNothing = columnsToUpdate.Count == 0 || string.IsNullOrWhiteSpace(equalsColumns);

            q = $"INSERT INTO {tableInfo.FullTableName} ({commaSeparatedColumns}) " +
                $"(SELECT {commaSeparatedColumns} FROM {tableInfo.FullTempTableName}) " + subqueryText +
                $"ON CONFLICT ({updateByColumns}) " +
                (onUpdateDoNothing
                 ? $"DO NOTHING"
                 : $"DO UPDATE SET {equalsColumns}");

            if (tableInfo.BulkConfig.OnConflictUpdateWhereSql != null)
            {
                q += $" WHERE {tableInfo.BulkConfig.OnConflictUpdateWhereSql(tableInfo.FullTableName.Replace("[", @"""").Replace("]", @""""), "EXCLUDED")}";
            }
            appendReturning = true;
        }

        if (appendReturning == true && tableInfo.CreateOutputTable)
        {
            var allColumnsList = tableInfo.OutputPropertyColumnNamesDict.Values.ToList();
            string commaSeparatedColumnsNames = SqlQueryBuilder.GetCommaSeparatedColumns(allColumnsList, tableInfo.FullTableName).Replace("[", @"""").Replace("]", @"""");
            q += $" RETURNING {commaSeparatedColumnsNames}";

            if (tableInfo.BulkConfig.CalculateStats)
            {
                q += ", xmax";
            }
        }

        q = q.Replace("[", @"""").Replace("]", @"""");

        Dictionary<string, string>? sourceDestinationMappings = tableInfo.BulkConfig.CustomSourceDestinationMappingColumns;
        if (tableInfo.BulkConfig.CustomSourceTableName != null && sourceDestinationMappings != null && sourceDestinationMappings.Count > 0)
        {
            var textSelect = "SELECT ";
            var textFrom = " FROM";
            int startIndex = q.IndexOf(textSelect);
            var qSegment = q[startIndex..q.IndexOf(textFrom)];
            var qSegmentUpdated = qSegment;
            foreach (var mapping in sourceDestinationMappings)
            {
                var propertyFormated = $@"""{mapping.Value}""";
                var sourceProperty = mapping.Key;

                if (qSegment.Contains(propertyFormated))
                {
                    qSegmentUpdated = qSegmentUpdated.Replace(propertyFormated, $@"""{sourceProperty}""");
                }
            }
            if (qSegment != qSegmentUpdated)
            {
                q = q.Replace(qSegment, qSegmentUpdated);
            }
        }

        if (tableInfo.BulkConfig.CalculateStats)
        {
            q = $"WITH upserted AS ({q}), " +
                $"NEW AS ( INSERT INTO {tableInfo.FullTempOutputTableName} SELECT xmax FROM upserted ) " +
                $"SELECT * FROM upserted";
        }

        q = q.Replace("[", @"""").Replace("]", @"""");
        q += ";";

        return q;
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
    /// Generates SQL query to truncate a table
    /// </summary>
    /// <param name="tableName"></param>
    public override string TruncateTable(string tableName)
    {
        var q = $"TRUNCATE {tableName} RESTART IDENTITY;";
        q = q.Replace("[", @"""").Replace("]", @"""");
        return q;
    }

    /// <summary>
    /// Generates SQL query to drop a table
    /// </summary>
    /// <param name="tableName"></param>
    public static string DropTable(string tableName)
    {
        string q = $"DROP TABLE IF EXISTS {tableName}";
        q = q.Replace("[", @"""").Replace("]", @"""");
        return q;
    }

    /// <summary>
    /// Generates SQL query to count the unique constranints
    /// </summary>
    /// <param name="tableInfo"></param>
    public static string CountUniqueConstrain(TableInfo tableInfo)
    {
        var primaryKeysColumns = tableInfo.PrimaryKeysPropertyColumnNameDict.Values.ToList();
        string q;

        bool usePG_Catalog = true; // PG_Catalog used instead of Information_Schema
        if (usePG_Catalog)
        {
            q = @"SELECT COUNT(*)
                  FROM pg_catalog.pg_namespace nr,
                      pg_catalog.pg_class r,
                      pg_catalog.pg_attribute a,
                      pg_catalog.pg_namespace nc,
                      pg_catalog.pg_constraint c
                  WHERE nr.oid = r.relnamespace
                  AND r.oid = a.attrelid
                  AND nc.oid = c.connamespace
                  AND r.oid =
                      CASE c.contype
                          WHEN 'f'::""char"" THEN c.confrelid
                      ELSE c.conrelid
                          END
                      AND (a.attnum = ANY (
                          CASE c.contype
                      WHEN 'f'::""char"" THEN c.confkey
                          ELSE c.conkey
                          END))
                      AND NOT a.attisdropped
                      AND (c.contype = ANY (ARRAY ['p'::""char"", 'u'::""char""]))
                      AND (r.relkind = ANY (ARRAY ['r'::""char"", 'p'::""char""]))" +
                $" AND r.relname = '{tableInfo.TableName}' AND nr.NSPNAME = '{tableInfo.Schema}' AND a.attname IN('{string.Join("','", primaryKeysColumns)}')";
        }
        else // Deprecated - Information_Schema no longer used (is available only in default database)
        {
            q = $"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc ";
            foreach (var (pkColumn, index) in primaryKeysColumns.Select((value, i) => (value, i)))
            {
                q += $"INNER JOIN INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE cu{index} " +
                     $"ON cu{index}.CONSTRAINT_NAME = tc.CONSTRAINT_NAME AND cu{index}.COLUMN_NAME = '{pkColumn}' ";
            }

            q += $"WHERE (tc.CONSTRAINT_TYPE = 'UNIQUE' OR tc.CONSTRAINT_TYPE = 'PRIMARY KEY') " +
                 $"AND tc.TABLE_NAME = '{tableInfo.TableName}' AND tc.TABLE_SCHEMA = '{tableInfo.Schema}'";
        }
        return q;
    }

    /// <summary>
    /// Generate SQL query to create a unique index
    /// </summary>
    /// <param name="tableInfo"></param>
    public static string CreateUniqueIndex(TableInfo tableInfo)
    {
        var tableName = tableInfo.TableName;
        var schemaFormated = tableInfo.Schema == null ? "" : $@"""{tableInfo.Schema}"".";
        var fullTableNameFormated = $@"{schemaFormated}""{tableName}""";

        var uniqueConstrainName = GetUniqueConstrainName(tableInfo);

        var uniqueColumnNames = tableInfo.PrimaryKeysPropertyColumnNameDict.Values.ToList();
        var uniqueColumnNamesFormated = @"""" + string.Join(@""", """, uniqueColumnNames) + @"""";

        var q = $@"CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS ""{uniqueConstrainName}"" " +
                $@"ON {fullTableNameFormated} ({uniqueColumnNamesFormated})";
        return q;
    }

    /// <summary>
    /// Generates SQL query to create a unique constraint
    /// </summary>
    /// <param name="tableInfo"></param>
    public static string CreateUniqueConstrain(TableInfo tableInfo)
    {
        var tableName = tableInfo.TableName;
        var schemaFormated = tableInfo.Schema == null ? "" : $@"""{tableInfo.Schema}"".";
        var fullTableNameFormated = $@"{schemaFormated}""{tableName}""";

        var uniqueConstrainName = GetUniqueConstrainName(tableInfo);

        var q = $@"ALTER TABLE {fullTableNameFormated} " +
                $@"ADD CONSTRAINT ""{uniqueConstrainName}"" " +
                $@"UNIQUE USING INDEX ""{uniqueConstrainName}""";
        return q;
    }

    /// <summary>
    /// Generates SQL query to drop a unique contstraint
    /// </summary>
    /// <param name="tableInfo"></param>
    public static string DropUniqueConstrain(TableInfo tableInfo)
    {
        var tableName = tableInfo.TableName;
        var schemaFormated = tableInfo.Schema == null ? "" : $@"""{tableInfo.Schema}"".";
        var fullTableNameFormated = $@"{schemaFormated}""{tableName}""";

        var uniqueConstrainName = GetUniqueConstrainName(tableInfo);

        var q = $@"ALTER TABLE {fullTableNameFormated} " +
                $@"DROP CONSTRAINT ""{uniqueConstrainName}"";";
        return q;
    }

    /// <summary>
    /// Creates UniqueConstrainName
    /// </summary>
    /// <param name="tableInfo"></param>
    public static string GetUniqueConstrainName(TableInfo tableInfo)
    {
        var tableName = tableInfo.TableName;

        var uniqueColumnNames = tableInfo.PrimaryKeysPropertyColumnNameDict.Values.ToList();
        var uniqueColumnNamesDash = string.Join("_", uniqueColumnNames);
        var schemaDash = tableInfo.Schema == null ? "" : $"{tableInfo.Schema}_";
        var uniqueConstrainName = $"tempUniqueIndex_{schemaDash}{tableName}_{uniqueColumnNamesDash}";

        return uniqueConstrainName;
    }

    /// <summary>
    /// Restructures a sql query for batch commands
    /// </summary>
    /// <param name="sql"></param>
    /// <param name="isDelete"></param>
    public override string RestructureForBatch(string sql, bool isDelete = false)
    {
        sql = sql.Replace("[", @"""").Replace("]", @"""");
        string firstLetterOfTable = sql.Substring(7, 1);

        if (isDelete)
        {
            //FROM
            // DELETE i FROM "Item" AS i WHERE i."ItemId" <= 1"
            //TO
            // DELETE FROM "Item" AS i WHERE i."ItemId" <= 1"
            //WOULD ALSO WORK
            // DELETE FROM "Item" WHERE "ItemId" <= 1

            sql = sql.Replace($"DELETE {firstLetterOfTable}", "DELETE ");
        }
        else
        {
            //FROM
            // UPDATE i SET "Description" = @Description, "Price\" = @Price FROM "Item" AS i WHERE i."ItemId" <= 1
            //TO
            // UPDATE "Item" AS i SET "Description" = 'Update N', "Price" = 1.5 WHERE i."ItemId" <= 1
            //WOULD ALSO WORK
            // UPDATE "Item" SET "Description" = 'Update N', "Price" = 1.5 WHERE "ItemId" <= 1

            string tableAS = sql.Substring(sql.IndexOf("FROM") + 4, sql.IndexOf($"AS {firstLetterOfTable}") - sql.IndexOf("FROM"));

            if (!sql.Contains("JOIN"))
            {
                sql = sql.Replace($"AS {firstLetterOfTable}", "");
                //According to postgreDoc sql-update: "Do not repeat the target table as a from_item unless you intend a self-join"
                string fromClause = sql.Substring(sql.IndexOf("FROM"), sql.IndexOf("WHERE") - sql.IndexOf("FROM"));
                sql = sql.Replace(fromClause, "");
            }
            else
            {
                int positionFROM = sql.IndexOf("FROM");
                int positionEndJOIN = sql.IndexOf("JOIN ") + "JOIN ".Length;
                int positionON = sql.IndexOf(" ON");
                int positionEndON = positionON + " ON".Length;
                int positionWHERE = sql.IndexOf("WHERE");
                string oldSqlSegment = sql[positionFROM..positionWHERE];
                string newSqlSegment = "FROM " + sql[positionEndJOIN..positionON];
                string equalsPkFk = sql[positionEndON..positionWHERE];
                sql = sql.Replace(oldSqlSegment, newSqlSegment);
                sql = sql.Replace("WHERE", " WHERE");
                sql = sql + " AND" + equalsPkFk;
            }

            sql = sql.Replace($"UPDATE {firstLetterOfTable}", "UPDATE" + tableAS);
        }

        return sql;
    }

    /// <summary>
    /// Returns a DbParameters intanced per provider
    /// </summary>
    /// <param name="sqlParameter"></param>
    /// <returns></returns>
    public override object CreateParameter(SqlParameter sqlParameter)
    {
        return new Npgsql.NpgsqlParameter(sqlParameter.ParameterName, sqlParameter.Value);
    }

    /// <summary>
    /// Generates SQL query to select output from a table
    /// </summary>
    /// <param name="tableInfo"></param>
    /// <returns></returns>
    public override string SelectFromOutputTable(TableInfo tableInfo)
    {
        return SelectFromOutputTable(tableInfo);
    }

    /// <summary>
    /// Returns NpgsqlDbType for PostgreSql parameters. Throws <see cref="NotImplementedException"/> for anothers providers
    /// </summary>
    /// <returns></returns>
    /// <exception cref="NotImplementedException"></exception>
    public override object Dbtype()
    {
        return NpgsqlTypes.NpgsqlDbType.Jsonb;
    }

    /// <summary>
    /// Returns void. Throws <see cref="NotImplementedException"/> for anothers providers
    /// </summary>
    /// <exception cref="NotImplementedException"></exception>
    public override void SetDbTypeParam(object npgsqlParameter, object dbType)
    {
        ((Npgsql.NpgsqlParameter)npgsqlParameter).NpgsqlDbType = (NpgsqlTypes.NpgsqlDbType)dbType;
    }
}
