using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.Internal;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using Microsoft.EntityFrameworkCore.Storage;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

namespace EFCore.BulkExtensions
{
    public static class IQueryableExtensions
    {
        public static (string, IEnumerable<SqlParameter>) ToParametrizedSql(this IQueryable query)
        {
            string relationalQueryContextText = "_relationalQueryContext";
            string relationalCommandCacheText = "_relationalCommandCache";

            string cannotGetText = "Cannot get";

            var enumerator = query.Provider.Execute<IEnumerable>(query.Expression).GetEnumerator();
            var queryContext = enumerator.Private<RelationalQueryContext>(relationalQueryContextText) ?? throw new InvalidOperationException($"{cannotGetText} {relationalQueryContextText}");
            var parameterValues = queryContext.ParameterValues;

#pragma warning disable EF1001 // Internal EF Core API usage.
            var relationalCommandCache = (RelationalCommandCache)enumerator.Private(relationalCommandCacheText);
#pragma warning restore EF1001

            IRelationalCommand command;
            if (relationalCommandCache != null)
            {
#pragma warning disable EF1001 // Internal EF Core API usage.
                command = (IRelationalCommand)relationalCommandCache.GetRelationalCommandTemplate(parameterValues);
#pragma warning restore EF1001
            }
            else
            {
                string selectExpressionText = "_selectExpression";
                string querySqlGeneratorFactoryText = "_querySqlGeneratorFactory";
                SelectExpression selectExpression = enumerator.Private<SelectExpression>(selectExpressionText) ?? throw new InvalidOperationException($"{cannotGetText} {selectExpressionText}");
                IQuerySqlGeneratorFactory factory = enumerator.Private<IQuerySqlGeneratorFactory>(querySqlGeneratorFactoryText) ?? throw new InvalidOperationException($"{cannotGetText} {querySqlGeneratorFactoryText}");
                command = factory.Create().GetCommand(selectExpression);
            }
            string sql = command.CommandText;

            IList<SqlParameter> parameters;
            using (var dbCommand = new SqlCommand()) // Use a DbCommand to convert parameter values using ValueConverters to the correct type.
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
    }
}
