using Oracle.ManagedDataAccess.Client;
using System.Data;
using System.Data.Common;
using System.Security.Cryptography;
using System.Text;
using static System.Runtime.InteropServices.Marshalling.IIUnknownCacheStrategy;

namespace EFCore.BulkExtensions.SqlAdapters.Oracle;

/// <summary>
///  Contains a list of methods to generate SQL queries required by EFCore
/// </summary>
public class OracleQueryBuilder : SqlQueryBuilder
{
    /// <summary>
    /// Generates SQL query to create table copy
    /// </summary>
    /// <param name="existingTableName"></param>
    /// <param name="newTableName"></param>
    /// <param name="tableInfo"></param>
    /// <param name="useTempDb"></param>
    /// <param name="operationType"></param>
    public static string CreateTableCopy(string existingTableName, string newTableName, TableInfo tableInfo, bool useTempDb, OperationType operationType)
    {
        var selectColummns = "*";
        if(operationType == OperationType.Delete)
        {
            var firstPrimaryKey = tableInfo.EntityPKPropertyColumnNameDict?.FirstOrDefault().Value ?? tableInfo.IdentityColumnName;
            selectColummns = firstPrimaryKey ?? "*";
        }
        string keywordTemp = useTempDb ? "GLOBAL TEMPORARY " : "";
        var query = $@"CREATE {keywordTemp}TABLE {newTableName} AS SELECT {selectColummns} FROM {existingTableName} WHERE 1=0";

        query = query.Replace("[", "").Replace("]", "");

        return query;
    }
    /// <summary>
    /// Generates SQL query to drop table
    /// </summary>
    /// <param name="tableName"></param>
    /// <param name="isTempTable"></param>
    /// <returns></returns>
    public override string DropTable(string tableName, bool isTempTable)
    {
        var query = $@"
BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE {tableName}';
EXCEPTION
    WHEN OTHERS THEN
        IF SQLCODE = -942 THEN
            NULL; -- Table does not exist
        ELSE
            RAISE; -- Re-throw the exception for other errors
        END IF;
END;";

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
        if (operationType == OperationType.InsertOrUpdateOrDelete)
        {
            throw new NotImplementedException("OperationType.InsertOrUpdateOrDelete is not supported for Oracle");
        }
        
        string q = "";
        var firstPrimaryKey = tableInfo.EntityPKPropertyColumnNameDict?.FirstOrDefault().Value ?? tableInfo.IdentityColumnName;


        var columnsList = GetColumnList(tableInfo, operationType);
        var columnsListWithouPrimaryKey = columnsList.Where(x => !Equals(x, firstPrimaryKey)).ToList();

        if (operationType == OperationType.Delete)
        {
            q = $"DELETE FROM {tableInfo.FullTableName} A " +
                $"WHERE A.{firstPrimaryKey} IN (SELECT B.{firstPrimaryKey} FROM {tableInfo.FullTempTableName} B); ";
        }
        else if (operationType == OperationType.Insert)
        {
            var commaSeparatedColumns = SqlQueryBuilder.GetCommaSeparatedColumns(columnsList).Replace("[", "").Replace("]", "");
            q = $"INSERT INTO {tableInfo.FullTableName} ({commaSeparatedColumns}) " +
                $"SELECT {commaSeparatedColumns} FROM {tableInfo.FullTempTableName}; ";
        }
        else if (operationType == OperationType.Update || operationType == OperationType.InsertOrUpdate)
        {
            var commaSeparatedColumns = SqlQueryBuilder.GetCommaSeparatedColumns(columnsList, "B").Replace("[", "").Replace("]", "");
            var commaSeparatedColumnsEq = SqlQueryBuilder.GetCommaSeparatedColumns(columnsListWithouPrimaryKey, "A", "B").Replace("[", "").Replace("]", "");
            
            q = $@"MERGE INTO {tableInfo.FullTableName} A
USING {tableInfo.FullTempTableName} B
ON (A.{firstPrimaryKey} = B.{firstPrimaryKey})
WHEN MATCHED THEN
    UPDATE SET {commaSeparatedColumnsEq}";

            if (operationType == OperationType.InsertOrUpdate)
            {
                q += $@"
WHEN NOT MATCHED THEN
    INSERT ({commaSeparatedColumns.Replace("B.", "")})
    VALUES ({commaSeparatedColumns});";
            }
            else
            {
                q += ";";
            }

            if (tableInfo.CreateOutputTable)
            {
                q += @$"DECLARE
    v_last_insert_id NUMBER;
BEGIN
    IF {tableInfo.CreateOutputTable} THEN
        IF {operationType} IN ('Insert', 'InsertOrUpdate') THEN
            SELECT MAX(id) INTO v_last_insert_id FROM {tableInfo.FullTableName};

            INSERT INTO {tableInfo.FullTempOutputTableName} 
            SELECT * FROM {tableInfo.FullTableName} 
            WHERE {firstPrimaryKey} >= v_last_insert_id;
        ELSIF {operationType} = 'Update' THEN
            INSERT INTO {tableInfo.FullTempOutputTableName} 
            SELECT * FROM {tableInfo.FullTempTableName};
        END IF;
    END IF;
END;";
            }
        }

        q = q.Replace("[", "").Replace("]", "");

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
                var propertySourceFormated = $"{mapping.Value}";
                var propertyFormated = $"{mapping.Value}";
                var sourceProperty = mapping.Key;

                if (qSegment.Contains(propertyFormated))
                {
                    qSegmentUpdated = qSegmentUpdated.Replace(propertyFormated, sourceProperty);
                }

                if (q.Contains(propertySourceFormated))
                {
                    q = q.Replace(propertySourceFormated, $"{sourceProperty}");
                }
            }

            // Pokud se segment změnil, aktualizujte celý dotaz
            if (qSegment != qSegmentUpdated)
            {
                q = q.Replace(qSegment, qSegmentUpdated);
            }
        }
        return q;
    }
    /// <summary>
    /// Generates SQL query to select output from a table
    /// </summary>
    /// <param name="tableInfo"></param>
    /// <returns></returns>
    public override string SelectFromOutputTable(TableInfo tableInfo)
    {
        List<string> columnsNames = tableInfo.OutputPropertyColumnNamesDict.Values.ToList();
        var query = $"SELECT {SqlQueryBuilder.GetCommaSeparatedColumns(columnsNames)} FROM {tableInfo.FullTempOutputTableName} WHERE {tableInfo.PrimaryKeysPropertyColumnNameDict.Select(x => x.Value).FirstOrDefault()} IS NOT NULL";
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
        var schemaFormated = tableInfo.Schema == null ? "" : $@"{tableInfo.Schema}.";
        var fullTableNameFormated = $@"{schemaFormated}{tableName}";

        var uniqueConstrainName = GetUniqueConstrainName(tableInfo);

        var uniqueColumnNames = tableInfo.PrimaryKeysPropertyColumnNameDict.Values.ToList();
        var uniqueColumnNamesComma = string.Join(",", uniqueColumnNames); // TODO When Column is string without defined max length, it should be UNIQUE (`Name`(255)); otherwise exception: BLOB/TEXT column 'Name' used in key specification without a key length'
        uniqueColumnNamesComma += uniqueColumnNamesComma;
        //uniqueColumnNamesComma += uniqueColumnNamesComma.Replace(",", "`, `");
        var uniqueColumnNamesFormated = uniqueColumnNamesComma.TrimEnd(',');
        uniqueColumnNamesFormated = uniqueColumnNamesFormated + "";

        var q = $@"ALTER TABLE {fullTableNameFormated} " +
                $@"ADD CONSTRAINT {uniqueConstrainName} " +
                $@"UNIQUE ({uniqueColumnNamesFormated})";

        q = q.Replace("[", "").Replace("]", "");

        return q;
    }

    /// <summary>
    /// Generates SQL query to drop a unique contstraint
    /// </summary>
    /// <param name="tableInfo"></param>
    public static string DropUniqueConstrain(TableInfo tableInfo)
    {
        var tableName = tableInfo.TableName;
        var schemaFormated = tableInfo.Schema == null ? "" : $@"{tableInfo.Schema}.";
        var fullTableNameFormated = $@"{schemaFormated}{tableName}";

        var uniqueConstrainName = GetUniqueConstrainName(tableInfo);

        var q = $@"DROP INDEX {uniqueConstrainName};";
        
        q = q.Replace("[", "").Replace("]", "");

        return q;
    }

    /// <summary>
    /// Generates SQL query to chaeck if a unique constrain exist
    /// </summary>
    /// <param name="tableInfo"></param>
    public static string HasUniqueConstrain(TableInfo tableInfo)
    {
        _ = tableInfo.TableName;

        var uniqueConstrainName = GetUniqueConstrainName(tableInfo);

        var q = $@"SELECT DISTINCT CONSTRAINT_NAME FROM USER_CONSTRAINTS WHERE " +
                $@"CONSTRAINT_TYPE IN ('U', 'P') AND CONSTRAINT_NAME = '{uniqueConstrainName}';";

        q = q.Replace("[", "").Replace("]", "");

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

        string uniqueConstrainPrefix = "tempUniqueIndex_"; // 16 char length
        string uniqueConstrainNameText = $"{schemaDash}{tableName}_{uniqueColumnNamesDash}";
        if (uniqueConstrainNameText.Length > 64 - (uniqueConstrainPrefix.Length)) // effectively 48 max
        {
            uniqueConstrainNameText = Md5Hash(uniqueConstrainNameText);
        }
        string uniqueConstrainName = uniqueConstrainPrefix + uniqueConstrainNameText;
        return uniqueConstrainName;
    }

    /// <summary>
    /// Restructures a sql query for batch commands
    /// </summary>
    /// <param name="sql"></param>
    /// <param name="isDelete"></param>
    public override string RestructureForBatch(string sql, bool isDelete = false)
    {
        sql = sql.Replace("[", "").Replace("]", "");
        string firstLetterOfTable = sql.Substring(7, 1);

        sql = sql.Replace("`i`", "i");
        if (isDelete)
        {
            sql = sql.Replace($"DELETE {firstLetterOfTable}", "DELETE ");
        }
        else
        {
            string tableAS = sql.Substring(sql.IndexOf("FROM") + 4, sql.IndexOf($" {firstLetterOfTable}") - sql.IndexOf("FROM"));

            sql = sql.Replace($"AS {firstLetterOfTable}", "");
            string fromClause = sql.Substring(sql.IndexOf("FROM"), sql.IndexOf("WHERE") - sql.IndexOf("FROM"));
            sql = sql.Replace(fromClause, "");

            sql = sql.Replace($"UPDATE {firstLetterOfTable}", "UPDATE" + tableAS);
        }
        sql = sql.Replace("[", "").Replace("]", "");

        return sql;
    }

    /// <summary>
    /// Generates SQL query to truncate table
    /// </summary>
    /// <param name="tableName"></param>
    /// <returns></returns>
    public override string TruncateTable(string tableName)
    {
        var q = $"TRUNCATE TABLE {tableName};";
        q = q.Replace("[", "").Replace("]", "");
        return q;
    }

    /// <inheritdoc/>
    public override DbParameter CreateParameter(string parameterName, object? parameterValue = null)
        => new OracleParameter(parameterName, parameterValue);

    /// <inheritdoc/>
    public override DbCommand CreateCommand()
        => new OracleCommand();

    /// <inheritdoc/>
    public override DbType Dbtype()
        => throw new NotSupportedException();

    /// <inheritdoc/>
    public override void SetDbTypeParam(DbParameter parameter, DbType dbType)
        => throw new NotSupportedException();

    private static string Md5Hash(string value)
    {
        var buffer = Encoding.UTF8.GetBytes(value);
        return BitConverter.ToString(MD5.HashData(buffer)).Replace("-", string.Empty);
    }
}
