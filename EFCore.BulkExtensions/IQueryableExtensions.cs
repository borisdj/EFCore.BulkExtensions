using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;

namespace EFCore.BulkExtensions
{
    public static class IQueryableExtensions
    {
        public static string ToSql<TEntity>(this IQueryable<TEntity> query) where TEntity : class
        {
            var (sql, parameters) = ToParametrizedSql(query);
            return sql;
        }

        public static (string, IEnumerable<SqlParameter>) ToParametrizedSql<TEntity>(this IQueryable<TEntity> query) where TEntity : class
        {
            var enumerator = query.Provider.Execute<IEnumerable<TEntity>>(query.Expression).GetEnumerator();
            var relationalCommandCache = enumerator.Private("_relationalCommandCache");

            SelectExpression selectExpression;
            IQuerySqlGeneratorFactory factory;
            if (relationalCommandCache != null)
            {
                selectExpression = relationalCommandCache.Private<SelectExpression>("_selectExpression") ?? throw new InvalidOperationException($"Cannot get _selectExpression");
                factory = relationalCommandCache.Private<IQuerySqlGeneratorFactory>("_querySqlGeneratorFactory") ?? throw new InvalidOperationException($"Cannot get _querySqlGeneratorFactory");
            }
            else
            {
                selectExpression = enumerator.Private<SelectExpression>("_selectExpression") ?? throw new InvalidOperationException($"Cannot get _selectExpression");
                factory = enumerator.Private<IQuerySqlGeneratorFactory>("_querySqlGeneratorFactory") ?? throw new InvalidOperationException($"Cannot get _querySqlGeneratorFactory");
            }

            var sqlGenerator = factory.Create();
            var command = sqlGenerator.GetCommand(selectExpression);

            var queryContext = enumerator.Private<RelationalQueryContext>("_relationalQueryContext") ?? throw new InvalidOperationException($"Cannot get RelationalQueryContext");
            SqlParameter[] parameters = queryContext.ParameterValues.Select(a => new SqlParameter("@" + a.Key, a.Value)).ToArray();

            string sql = command.CommandText;
            return (sql, parameters);
        }

        private static object Private(this object obj, string privateField) => obj?.GetType().GetField(privateField, BindingFlags.Instance | BindingFlags.NonPublic)?.GetValue(obj);
        private static T Private<T>(this object obj, string privateField) => (T)obj?.GetType().GetField(privateField, BindingFlags.Instance | BindingFlags.NonPublic)?.GetValue(obj);
    }
}
