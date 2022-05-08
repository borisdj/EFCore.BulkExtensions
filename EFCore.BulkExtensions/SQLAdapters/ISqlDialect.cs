using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using Microsoft.EntityFrameworkCore;

namespace EFCore.BulkExtensions.SqlAdapters;

public class ExtractedTableAlias
{
    public string TableAlias { get; set; } = null!;
    public string TableAliasSuffixAs { get; set; } = null!;
    public string Sql { get; set; } = null!;
}

public interface IQueryBuilderSpecialization
{
    List<object> ReloadSqlParameters(DbContext context, List<object> sqlParameters);

    string GetBinaryExpressionAddOperation(BinaryExpression binaryExpression);

    (string, string) GetBatchSqlReformatTableAliasAndTopStatement(string sqlQuery, DbServer databaseType);

    ExtractedTableAlias GetBatchSqlExtractTableAliasFromQuery(string fullQuery, string tableAlias, string tableAliasSuffixAs);
}
