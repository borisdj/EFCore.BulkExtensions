using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using Microsoft.EntityFrameworkCore;

namespace EFCore.BulkExtensions.SqlAdapters;


/// <summary>
/// Contains the table alias and SQL query
/// </summary>
public class ExtractedTableAlias
{
#pragma warning disable CS1591 // No XML comments required
    public string TableAlias { get; set; }
    public string TableAliasSuffixAs { get; set; }
    public string Sql { get; set; }
#pragma warning restore CS1591 // No XML comments required
}

/// <summary>
/// Contains a list of methods for query operations
/// </summary>
public interface IQueryBuilderSpecialization
{
    /// <summary>
    /// Reloads the SQL paramaters
    /// </summary>
    /// <param name="context"></param>
    /// <param name="sqlParameters"></param>
    List<object> ReloadSqlParameters(DbContext context, List<object> sqlParameters);

    /// <summary>
    /// Returns the binary expression add operation
    /// </summary>
    /// <param name="binaryExpression"></param>
    string GetBinaryExpressionAddOperation(BinaryExpression binaryExpression);

    /// <summary>
    /// Returns a tuple containing the batch sql reformat table alias
    /// </summary>
    /// <param name="sqlQuery"></param>
    /// <param name="databaseType"></param>
    (string, string) GetBatchSqlReformatTableAliasAndTopStatement(string sqlQuery, DbServer databaseType);

    /// <summary>
    /// Returns the SQL extract table alias data
    /// </summary>
    /// <param name="fullQuery"></param>
    /// <param name="tableAlias"></param>
    /// <param name="tableAliasSuffixAs"></param>
    ExtractedTableAlias GetBatchSqlExtractTableAliasFromQuery(string fullQuery, string tableAlias, string tableAliasSuffixAs);
}
