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
        string keywordTEMP = useTempDb ? "TEMPORARY" : ""; 
        var query = $"CREATE {keywordTEMP} TABLE {newTableName} " +
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
        string query;
        if (isTempTable)
        {
            query = $"DROP TEMPORARY TABLE IF EXISTS {tableName}";
        }
        else
        {
            query = $"DROP TABLE IF EXISTS {tableName}";
        }
        query = query.Replace("[", "").Replace("]", "");
        return query;
    }
}
