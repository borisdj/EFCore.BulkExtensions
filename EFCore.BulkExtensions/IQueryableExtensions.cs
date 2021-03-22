using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.Internal;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using Microsoft.EntityFrameworkCore.Storage;

namespace EFCore.BulkExtensions
{
    public static class IQueryableExtensions
    {
        public static (string, IEnumerable<SqlParameter>) ToParametrizedSql(this IQueryable query)
        {
            string relationalCommandCacheText = "_relationalCommandCache";
            string selectExpressionText = "_selectExpression";
            string querySqlGeneratorFactoryText = "_querySqlGeneratorFactory";
            string relationalQueryContextText = "_relationalQueryContext";

            string cannotGetText = "Cannot get";

            var enumerator = query.Provider.Execute<IEnumerable>(query.Expression).GetEnumerator();
            var relationalCommandCache = enumerator.Private(relationalCommandCacheText) as RelationalCommandCache;
            var queryContext = enumerator.Private<RelationalQueryContext>(relationalQueryContextText) ?? throw new InvalidOperationException($"{cannotGetText} {relationalQueryContextText}");
            var parameterValues = queryContext.ParameterValues;

            string sql;
            IList<SqlParameter> parameters;
            IRelationalCommand command;
            if (relationalCommandCache != null)
            {
#pragma warning disable EF1001 // Internal EF Core API usage.
                command = relationalCommandCache.GetRelationalCommand(parameterValues);
#pragma warning restore EF1001 // Internal EF Core API usage.
            }
            else
            {
                SelectExpression selectExpression = enumerator.Private<SelectExpression>(selectExpressionText) ?? throw new InvalidOperationException($"{cannotGetText} {selectExpressionText}");
                IQuerySqlGeneratorFactory factory = enumerator.Private<IQuerySqlGeneratorFactory>(querySqlGeneratorFactoryText) ?? throw new InvalidOperationException($"{cannotGetText} {querySqlGeneratorFactoryText}");
                command = factory.Create().GetCommand(selectExpression);
            }

            sql = command.CommandText;

            // Use a DbCommand to convert parameter values using ValueConverters to the correct type.
            using (var dbCommand = new SqlCommand())
            {
                foreach (var param in command.Parameters)
                {
                    var values = parameterValues[param.InvariantName];
                    param.AddDbParameter(dbCommand, values);
                }

                parameters = new List<SqlParameter>(dbCommand.Parameters.OfType<SqlParameter>());
                dbCommand.Parameters.Clear();
            }

            return (sql, parameters);
        }

        private static readonly BindingFlags bindingFlags = BindingFlags.Instance | BindingFlags.NonPublic;

        private static object Private(this object obj, string privateField) => obj?.GetType().GetField(privateField, bindingFlags)?.GetValue(obj);

        private static T Private<T>(this object obj, string privateField) => (T)obj?.GetType().GetField(privateField, bindingFlags)?.GetValue(obj);

        private static SqlParameter ToSqlParameter(KeyValuePair<string, object> pv) =>
            pv.Value is object[] parameters && parameters.Length == 1 && parameters[0] is SqlParameter parameter
                ? parameter
                : new SqlParameter("@" + pv.Key, pv.Value);
    }
}
