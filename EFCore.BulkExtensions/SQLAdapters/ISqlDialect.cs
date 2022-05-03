using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using Microsoft.EntityFrameworkCore;

namespace EFCore.BulkExtensions.SqlAdapters;

public class ExtractedTableAlias
{
    public string TableAlias { get; set; }
    public string TableAliasSuffixAs { get; set; }
    public string Sql { get; set; }
}

public interface IQueryBuilderSpecialization
{
    List<object> ReloadSqlParameters(DbContext context, List<object> sqlParameters);

    string GetBinaryExpressionAddOperation(BinaryExpression binaryExpression);

    (string, string) GetBatchSqlReformatTableAliasAndTopStatement(string sqlQuery, DbServer databaseType);

    ExtractedTableAlias GetBatchSqlExtractTableAliasFromQuery(string fullQuery, string tableAlias, string tableAliasSuffixAs);
}
