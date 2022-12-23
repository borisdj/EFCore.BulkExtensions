using System;
using System.Linq.Expressions;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using Microsoft.EntityFrameworkCore.Storage;

namespace EFCore.BulkExtensions.Helpers;

internal static class QuerySqlGeneratorHelpers
{
    /// <summary>
    /// Cached delegate.
    /// For more info see: https://codeblog.jonskeet.uk/2008/08/09/making-reflection-fly-and-exploring-delegates/
    /// </summary>
    private static Func<QuerySqlGenerator, SelectExpression, IRelationalCommand>? _getCommandFunc;

    /// <summary>
    /// Retrieves a <see cref="IRelationalCommand"/> using <see cref="QuerySqlGenerator.GetCommand"/>,
    /// But also applies a fix when used with EF7
    /// </summary>
    /// <param name="querySqlGenerator"></param>
    /// <param name="selectExpression"></param>
    /// <returns></returns>
    public static IRelationalCommand GetCommand(QuerySqlGenerator querySqlGenerator, SelectExpression selectExpression)
    {
        if (_getCommandFunc == null)
            return querySqlGenerator.GetCommand(selectExpression);
        return _getCommandFunc(querySqlGenerator, selectExpression);
    }
    
    static QuerySqlGeneratorHelpers()
    {
        InitGetCommand();
    }

    private static void InitGetCommand()
    {
        // EF6 uses QuerySqlGenerator.GetCommand(SelectExpression)
        // EF7 uses QuerySqlGenerator.GetCommand(Expression)
        // See: https://github.com/dotnet/efcore/blob/3f82c2b0af132b7019f6bd8b32c2709469b0ffae/src/EFCore.Relational/Query/QuerySqlGenerator.cs#L67
        var methodInfo = typeof(QuerySqlGenerator)
            .GetMethod(nameof(QuerySqlGenerator.GetCommand), new[] { typeof(Expression) });

        if (methodInfo != null)
        {
            _getCommandFunc =
                (Func<QuerySqlGenerator, SelectExpression, IRelationalCommand>)Delegate.CreateDelegate(
                    typeof(Func<QuerySqlGenerator, SelectExpression, IRelationalCommand>), methodInfo);
        }
    }
}
