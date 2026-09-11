using GaussDB;
using GaussDBTypes;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Text.RegularExpressions;

namespace EFCore.BulkExtensions.SqlAdapters.GaussDB;

/// <summary>
/// Contains methods to generate SQL queries required by the GaussDB provider.
/// </summary>
public class GaussDBQueryBuilder : SqlQueryBuilder
{
    /// <inheritdoc/>
    public override DbCommand CreateCommand()
    {
        return new GaussDBCommand();
    }

    /// <inheritdoc/>
    public override DbParameter CreateParameter(string parameterName, object? parameterValue = null)
    {
        return new GaussDBParameter(parameterName, parameterValue);
    }

    /// <inheritdoc/>
    public override DbType Dbtype()
    {
        return (DbType)GaussDBDbType.Jsonb;
    }

    /// <inheritdoc/>
    public override string SelectFromOutputTable(TableInfo tableInfo)
    {
        var columns = GetCommaSeparatedColumns(tableInfo.OutputPropertyColumnNamesDict.Values.ToList())
            .Replace("[", @"""").Replace("]", @"""");
        var primaryKey = tableInfo.PrimaryKeysPropertyColumnNameDict.Values.FirstOrDefault()
            ?? throw new InvalidOperationException("GaussDB output selection requires a primary key.");
        primaryKey = primaryKey.Replace("[", @"""").Replace("]", @"""");
        if (!primaryKey.StartsWith('"'))
        {
            primaryKey = $@"""{primaryKey}""";
        }
        var outputTable = tableInfo.FullTempOutputTableName.Replace("[", @"""").Replace("]", @"""");
        return $"SELECT {columns} FROM {outputTable} " +
            $"WHERE {primaryKey} IS NOT NULL";
    }

    //public override string RestructureForBatch(string sql, bool isDelete = false)
    //{
    //    throw new NotImplementedException();
    //}

    /// <inheritdoc/>
    public override void SetDbTypeParam(DbParameter parameter, DbType dbType)
    {
        ((GaussDBParameter)parameter).GaussDBDbType = (GaussDBDbType)dbType;
    }

    #region DDL
    /// <inheritdoc/>
    public override string TruncateTable(string tableName)
    {
        var sql = $"TRUNCATE {tableName};";
        return sql.Replace("[", @"""").Replace("]", @"""");
    }

    /// <summary>
    /// Generates SQL query to drop a table.
    /// </summary>
    /// <param name="tableName">The table to drop.</param>
    /// <returns>The SQL statement.</returns>
    public static string DropTable(string tableName)
    {
        var sql = $"DROP TABLE IF EXISTS {tableName}";
        return sql.Replace("[", @"""").Replace("]", @"""");
    }

    /// <inheritdoc/>
    public override string DropTable(string tableName, bool isTempTable) => DropTable(tableName);

    /// <summary>
    /// Generates SQL query to create Output table for Stats
    /// </summary>
    /// <param name="newTableName"></param>
    /// <param name="useTempDb"></param>
    /// <param name="unlogged"></param>
    public static string CreateOutputStatsTable(string newTableName, bool useTempDb, bool unlogged)
    {
        string keywordPrefix = "";
        if (useTempDb == true)
        {
            keywordPrefix = "TEMP "; // "TEMP " or "TEMPORARY "
        }
        else if (unlogged) // can not be combined with TEMP since Temporary tables are not logged by default.
        {
            keywordPrefix = "UNLOGGED ";
        }
        var q = @$"CREATE {keywordPrefix}TABLE IF NOT EXISTS {newTableName} (""xmaxNumber"" xid)"; // col name can't be just 'xmax' - conflicts with system column
        q = q.Replace("[", @"""").Replace("]", @"""");
        return q;
    }

    /// <summary>
    /// Generates SQL query to create table copy
    /// </summary>
    /// <param name="existingTableName"></param>
    /// <param name="newTableName"></param>
    /// <param name="useTempDb"></param>
    /// <param name="unlogged"></param>
    public static string CreateTableCopy(string existingTableName, string newTableName, bool useTempDb, bool unlogged)
    {
        string keywordPrefix = "";
        if (useTempDb == true)
        {
            keywordPrefix = "TEMP "; // "TEMP " or "TEMPORARY "
        }
        else if (unlogged) // can not be combined with TEMP since Temporary tables are not logged by default.
        {
            keywordPrefix = "UNLOGGED ";
        }
        var q = $"CREATE {keywordPrefix}TABLE {newTableName} " +
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
            throw new NotImplementedException($"For GaussDB method {OperationType.InsertOrUpdateOrDelete} is not yet supported. Use combination of InsertOrUpdate with Read and Delete");
        }

        string q;
        bool appendReturning = false;
        if (operationType == OperationType.Read)
        {
            if (!tableInfo.UpdateByPropertiesAreNullable)
            {
                var readByColumns = SqlQueryBuilder.GetCommaSeparatedColumns(tableInfo.PrimaryKeysPropertyColumnNameDict.Values.ToList());
                q = $"SELECT {tableInfo.FullTableName}.* FROM {tableInfo.FullTableName} " +
                    $"JOIN {tableInfo.FullTempTableName} USING ({readByColumns})";
            }
            else
            {
                var readJoin = SqlQueryBuilder.GetANDSeparatedColumns(tableInfo.PrimaryKeysPropertyColumnNameDict.Values.ToList(),
                    prefixTable: tableInfo.FullTableName, equalsTable: tableInfo.FullTempTableName,
                    updateByPropertiesAreNullable: true);
                q = $"SELECT {tableInfo.FullTableName}.* FROM {tableInfo.FullTableName} " +
                    $"JOIN {tableInfo.FullTempTableName} ON {readJoin}";
            }
        }
        else if (operationType == OperationType.Delete)
        {
            var deleteByColumns = SqlQueryBuilder.GetANDSeparatedColumns(
                tableInfo.PrimaryKeysPropertyColumnNameDict.Values.ToList(),
                prefixTable: tableInfo.FullTableName,
                equalsTable: tableInfo.FullTempTableName,
                updateByPropertiesAreNullable: tableInfo.UpdateByPropertiesAreNullable)
                .Replace("[", @"""").Replace("]", @"""");

            q = $"DELETE FROM {tableInfo.FullTableName} " +
                $"USING {tableInfo.FullTempTableName} " +
                $"WHERE {deleteByColumns}";
        }
        else if (operationType == OperationType.Update)
        {
            // Defaults are omitted only for INSERT. Updates must copy explicit CLR values,
            // while key columns remain match columns and are never assigned.
            var columnsListEquals = GetColumnList(tableInfo, OperationType.Update);
            var keyColumns = tableInfo.PrimaryKeysPropertyColumnNameDict.Values.ToHashSet();
            var columnsToUpdate = columnsListEquals
                .Where(c => tableInfo.PropertyColumnNamesUpdateDict.ContainsValue(c) && !keyColumns.Contains(c))
                .ToList();

            var updateByColumns = SqlQueryBuilder.GetANDSeparatedColumns(tableInfo.PrimaryKeysPropertyColumnNameDict.Values.ToList(),
                prefixTable: tableInfo.FullTableName, equalsTable: tableInfo.FullTempTableName,
                updateByPropertiesAreNullable: tableInfo.UpdateByPropertiesAreNullable).Replace("[", @"""").Replace("]", @"""");
            var equalsColumns = SqlQueryBuilder.GetCommaSeparatedColumns(columnsToUpdate,
                equalsTable: tableInfo.FullTempTableName).Replace("[", @"""").Replace("]", @"""");

            q = $"UPDATE {tableInfo.FullTableName} SET {equalsColumns} " +
                $"FROM {tableInfo.FullTempTableName} " +
                $"WHERE {updateByColumns}";

            appendReturning = true;
        }
        else if (operationType == OperationType.Insert)
        {
            // BulkInsert with SetOutputIdentity uses the merge path so RETURNING can hydrate entities.
            var commaSeparatedColumns = SqlQueryBuilder.GetCommaSeparatedColumns(columnsList)
                .Replace("[", @"""").Replace("]", @"""");
            int subqueryLimit = tableInfo.BulkConfig.ApplySubqueryLimit;
            var subqueryText = subqueryLimit > 0 ? $"LIMIT {subqueryLimit} " : "";
            q = $"INSERT INTO {tableInfo.FullTableName} ({commaSeparatedColumns}) " +
                $"(SELECT {commaSeparatedColumns} FROM {tableInfo.FullTempTableName}) " + subqueryText;
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

            var columnsListEquals = GetColumnList(tableInfo, OperationType.Insert);
            var keyColumns = tableInfo.PrimaryKeysPropertyColumnNameDict.Values.ToHashSet();
            var columnsToUpdate = columnsListEquals
                .Where(c => tableInfo.PropertyColumnNamesUpdateDict.ContainsValue(c) && !keyColumns.Contains(c))
                .ToList();
            var equalsColumns = SqlQueryBuilder.GetCommaSeparatedColumns(columnsToUpdate, equalsTable: "EXCLUDED").Replace("[", @"""").Replace("]", @"""");

            int subqueryLimit = tableInfo.BulkConfig.ApplySubqueryLimit;
            var subqueryText = subqueryLimit > 0 ? $"LIMIT {subqueryLimit} " : "";
            bool onUpdateDoNothing = columnsToUpdate.Count == 0 || string.IsNullOrWhiteSpace(equalsColumns);

            if (onUpdateDoNothing)
            {
                var noOpColumn = columnsListInsert.FirstOrDefault(c => !keyColumns.Contains(c))
                    ?? throw new NotSupportedException("GaussDB ON DUPLICATE KEY UPDATE requires at least one non-key column for no-op updates.");
                equalsColumns = SqlQueryBuilder.GetCommaSeparatedColumns([noOpColumn], equalsTable: tableInfo.FullTableName)
                    .Replace("[", @"""").Replace("]", @"""");
            }

            q = $"INSERT INTO {tableInfo.FullTableName} ({commaSeparatedColumns}) " +
                $"(SELECT {commaSeparatedColumns} FROM {tableInfo.FullTempTableName}) " + subqueryText +
                $"ON DUPLICATE KEY UPDATE {equalsColumns}";

            if (tableInfo.BulkConfig.OnConflictUpdateWhereSql != null)
            {
                throw new NotSupportedException($"{nameof(BulkConfig.OnConflictUpdateWhereSql)} is not supported by GaussDB ON DUPLICATE KEY UPDATE.");
            }
            appendReturning = true;
        }

        if (appendReturning == true && tableInfo.CreateOutputTable)
        {
            var allColumnsList = tableInfo.OutputPropertyColumnNamesDict.Values.ToList();
            string commaSeparatedColumnsNames = SqlQueryBuilder.GetCommaSeparatedColumns(allColumnsList, tableInfo.FullTableName).Replace("[", @"""").Replace("]", @"""");
            q += $" RETURNING {commaSeparatedColumnsNames}";

            if (tableInfo.BulkConfig.CalculateStats && operationType is OperationType.Insert or OperationType.InsertOrUpdate)
            {
                q += $", {tableInfo.FullTableName}.xmax";
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

        if (tableInfo.BulkConfig.CalculateStats && operationType is OperationType.Insert or OperationType.InsertOrUpdate)
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
    /// Generates SQL query to count the unique constranints -  Not used, insted used only: CountUniqueIndex
    /// </summary>
    /// <param name="tableInfo"></param>
    public static string CountUniqueConstrain(TableInfo tableInfo)
    {
        var primaryKeysColumns = tableInfo.PrimaryKeysPropertyColumnNameDict.Values.ToList();
        string q;

        bool usePG_Catalog = true; // PG_Catalog used instead of Information_Schema
        if (usePG_Catalog)
        {
            q = @"SELECT COUNT(distinct c.conname)
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
                $" AND r.relname = '{tableInfo.TableName}'" +
                $" AND nr.nspname = '{tableInfo.Schema}'" +
                $" AND a.attname IN('{string.Join("','", primaryKeysColumns)}')";
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
    /// Generates SQL query to count the unique index
    /// </summary>
    /// <param name="tableInfo"></param>
    public static string CountUniqueIndex(TableInfo tableInfo)
    {
        var primaryKeysColumns = tableInfo.PrimaryKeysPropertyColumnNameDict.Values.ToList();
        string q;
        q = @"SELECT COUNT(idx.relname)
              FROM pg_catalog.pg_index pgi
                JOIN pg_catalog.pg_class idx ON idx.oid = pgi.indexrelid
                JOIN pg_catalog.pg_namespace insp ON insp.oid = idx.relnamespace
                JOIN pg_catalog.pg_class tbl ON tbl.oid = pgi.indrelid
                JOIN pg_catalog.pg_namespace tnsp ON tnsp.oid = tbl.relnamespace
                JOIN pg_catalog.pg_attribute at ON at.attrelid = idx.oid
              WHERE pgi.indisunique
                AND not pgi.indisprimary" +
             $" AND tnsp.nspname = '{tableInfo.Schema}'" +
             $" AND tbl.relname = '{tableInfo.TableName}'" +
             $" AND at.attname IN('{string.Join("','", primaryKeysColumns)}')" +
            " GROUP BY idx.relname" +
            " HAVING COUNT(idx.relname) = " + primaryKeysColumns.Count + ";";
        return q;
    }

    /// <summary>
    /// Generate SQL query to create a unique index
    /// </summary>
    /// <param name="tableInfo"></param>
    /// <param name="concurrently">Whether to build the temporary index concurrently.</param>
    public static string CreateUniqueIndex(TableInfo tableInfo, bool concurrently = true)
    {
        var tableName = tableInfo.TableName;
        var schemaFormated = tableInfo.Schema == null ? "" : $@"""{tableInfo.Schema}"".";
        var fullTableNameFormated = $@"{schemaFormated}""{tableName}""";

        var uniqueIndexName = GetUniqueIndexName(tableInfo);

        var uniqueColumnNames = tableInfo.PrimaryKeysPropertyColumnNameDict.Values.ToList();
        var uniqueColumnNamesFormated = @"""" + string.Join(@""", """, uniqueColumnNames) + @"""";

        var concurrentlyKeyword = concurrently ? " CONCURRENTLY" : string.Empty;
        var q = $@"CREATE UNIQUE INDEX{concurrentlyKeyword} IF NOT EXISTS ""{uniqueIndexName}"" " +
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

        var uniqueConstrainName = GetUniqueIndexName(tableInfo);

        var q = $@"ALTER TABLE {fullTableNameFormated} " +
                $@"ADD CONSTRAINT ""{uniqueConstrainName}"" " +
                $@"UNIQUE USING INDEX ""{uniqueConstrainName}""";
        return q;
    }

    /// <summary>
    /// Generates SQL query to drop a unique index
    /// </summary>
    /// <param name="tableInfo"></param>
    public static string DropUniqueIndex(TableInfo tableInfo)
    {
        var schemaFormated = tableInfo.Schema == null ? "" : $@"""{tableInfo.Schema}"".";
        var uniqueIndexName = GetUniqueIndexName(tableInfo);
        var fullUniqueIndexNameFormated = $@"{schemaFormated}""{uniqueIndexName}""";

        var q = $@"DROP INDEX {fullUniqueIndexNameFormated};";
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

        var uniqueIndexName = GetUniqueIndexName(tableInfo);

        var q = $@"ALTER TABLE {fullTableNameFormated} " +
                $@"DROP CONSTRAINT ""{uniqueIndexName}"";";
        return q;
    }

    /// <summary>
    /// Creates UniqueConstrainName
    /// </summary>
    /// <param name="tableInfo"></param>
    public static string GetUniqueIndexName(TableInfo tableInfo)
    {
        var tableName = tableInfo.TableName;

        var uniqueColumnNames = tableInfo.PrimaryKeysPropertyColumnNameDict.Values.ToList();
        var uniqueColumnNamesDash = string.Join("_", uniqueColumnNames);
        var schemaDash = tableInfo.Schema == null ? "" : $"{tableInfo.Schema}_";
        var uniqueIndexName = $"tempUniqueIndex_{schemaDash}{tableName}_{uniqueColumnNamesDash}";
        uniqueIndexName = uniqueIndexName.Length > 64 ? uniqueIndexName[..64] : uniqueIndexName;

        return uniqueIndexName;
    }

    /// <inheritdoc/>
    public override string RestructureForBatch(string sql, bool isDelete = false)
    {
        sql = sql.Replace("[", @"""").Replace("]", @"""");
        // EF may prepend TagWith comments and aliases can contain several characters.
        // Capture only the command header, preserving comments and all later alias references.
        const RegexOptions options = RegexOptions.IgnoreCase | RegexOptions.CultureInvariant;
        var header = Regex.Match(sql,
            @"\A(?<comments>(?:\s+|--[^\r\n]*(?:\r\n|\r|\n|$)|/\*[\s\S]*?\*/)*)" +
            @"(?:UPDATE|DELETE)\s+(?<alias>""(?:[^""]|"""")*""|[A-Za-z_][A-Za-z0-9_]*)(?=\s|$)",
            options, TimeSpan.FromSeconds(1));
        if (!header.Success)
        {
            throw new NotSupportedException("The GaussDB batch command must start with UPDATE or DELETE and a table alias.");
        }

        var comments = header.Groups["comments"].Value;
        var alias = header.Groups["alias"].Value;
        var body = sql[header.Length..].TrimStart();
        if (isDelete)
        {
            return comments + "DELETE " + body;
        }

        // Match the target declaration, including its complete alias. The table name may
        // be schema qualified; quoted identifiers may contain spaces or escaped quotes.
        const string identifier = @"(?:""(?:[^""]|"""")*""|[A-Za-z_][A-Za-z0-9_]*)";
        var target = Regex.Match(body,
            @"\bFROM\s+(?<table>" + identifier + @"(?:\s*\.\s*" + identifier + @")*\s+AS\s+" +
            Regex.Escape(alias) + @")(?=\s|$)", options, TimeSpan.FromSeconds(1));
        if (!target.Success)
        {
            throw new NotSupportedException("The GaussDB batch command does not contain the target table declaration.");
        }

        var assignments = body[..target.Index].TrimEnd();
        var remainder = body[(target.Index + target.Length)..].TrimStart();
        var result = comments + "UPDATE " + target.Groups["table"].Value + " " + assignments;
        if (!Regex.IsMatch(remainder, @"\bJOIN\b", options, TimeSpan.FromSeconds(1)))
        {
            return remainder.Length == 0 ? result : result + " " + remainder;
        }

        // Move the joined table to FROM and the join predicate to WHERE. An unfiltered
        // batch has no existing WHERE clause, so its join predicate becomes that clause.
        var join = Regex.Match(remainder, @"\bJOIN\s+(?<table>.*?)\s+ON\s+(?<predicate>.*)",
            options | RegexOptions.Singleline, TimeSpan.FromSeconds(1));
        if (!join.Success)
        {
            throw new NotSupportedException("The GaussDB batch join must contain an ON predicate.");
        }
        var predicate = join.Groups["predicate"].Value;
        var where = Regex.Match(predicate, @"\bWHERE\b", options, TimeSpan.FromSeconds(1));
        if (!where.Success)
        {
            return result + " FROM " + join.Groups["table"].Value + " WHERE " + predicate;
        }

        return (result + " FROM " + join.Groups["table"].Value + " " + predicate[where.Index..].TrimStart() +
            " AND " + predicate[..where.Index].TrimEnd()).TrimEnd() + " ";
    }
    #endregion
}
