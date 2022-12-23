using System;
using System.Linq.Expressions;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using Microsoft.EntityFrameworkCore.Storage;

namespace EFCore.BulkExtensions.Helpers;

internal static class QuerySqlGeneratorHelpers
{

    private static Func<QuerySqlGenerator, SelectExpression, IRelationalCommand>? _getCommandFunc;

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
        // EF6 uses GetCommand(SelectExpression)
        // EF7 uses GetCommand(Expression)
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
