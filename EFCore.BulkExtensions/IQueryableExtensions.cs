using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Internal;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.Internal;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.Extensions.Logging;

namespace EFCore.BulkExtensions
{
    public static class IQueryableExtensions
    {
        private static readonly TypeInfo QueryCompilerTypeInfo = typeof(QueryCompiler).GetTypeInfo();

        private static readonly FieldInfo QueryCompilerField = typeof(EntityQueryProvider).GetTypeInfo().DeclaredFields.First(x => x.Name == "_queryCompiler");

        private static readonly FieldInfo QueryModelGeneratorField = QueryCompilerTypeInfo.DeclaredFields.First(x => x.Name == "_queryModelGenerator");

        private static readonly FieldInfo DataBaseField = QueryCompilerTypeInfo.DeclaredFields.Single(x => x.Name == "_database");

        private static readonly PropertyInfo DatabaseDependenciesField = typeof(Database).GetTypeInfo().DeclaredProperties.Single(x => x.Name == "Dependencies");

        internal static string ToSql<TEntity>(this IQueryable<TEntity> query) where TEntity : class
        {
            var enumerator = query.Provider.Execute<IEnumerable<TEntity>>(query.Expression).GetEnumerator();
            var enumeratorType = enumerator.GetType();

            var bindingFlags = BindingFlags.NonPublic | BindingFlags.Instance;
            var selectExpressionFieldName = "_selectExpression";
            var querySqlGeneratorFactoryFieldName = "_querySqlGeneratorFactory";
            var selectFieldInfo = enumeratorType.GetField(selectExpressionFieldName, bindingFlags);
            var sqlGeneratorFieldInfo = enumeratorType.GetField(querySqlGeneratorFactoryFieldName, bindingFlags);
            if (selectFieldInfo == null || sqlGeneratorFieldInfo == null)
                throw new InvalidOperationException($"Cannot find field {(selectFieldInfo == null ? selectExpressionFieldName : querySqlGeneratorFactoryFieldName) } on type {enumeratorType.Name}");

            var selectExpression = selectFieldInfo.GetValue(enumerator) as SelectExpression;
            var factory = sqlGeneratorFieldInfo.GetValue(enumerator) as IQuerySqlGeneratorFactory;
            if (selectExpression == null || factory == null)
                throw new InvalidOperationException($"Could not get {(selectFieldInfo == null ? nameof(SelectExpression) : nameof(IQuerySqlGeneratorFactory)) }");

            var sqlGenerator = factory.Create();
            var command = sqlGenerator.GetCommand(selectExpression);
            var sql = command.CommandText;
            return sql;

            //DEPRECATED: Used for .NetCore 2 on NetStandard 2.0
            /*
            var queryCompiler = (QueryCompiler)QueryCompilerField.GetValue(query.Provider);
            var modelGenerator = (QueryModelGenerator)QueryModelGeneratorField.GetValue(queryCompiler);
            var queryModel = modelGenerator.ParseQuery(query.Expression);
            var database = (IDatabase)DataBaseField.GetValue(queryCompiler);
            var databaseDependencies = (DatabaseDependencies)DatabaseDependenciesField.GetValue(database);
            var queryCompilationContext = databaseDependencies.QueryCompilationContextFactory.Create(false);
            var modelVisitor = (RelationalQueryModelVisitor)queryCompilationContext.CreateQueryModelVisitor();

            modelVisitor.CreateQueryExecutor<TEntity>(queryModel);
            //modelVisitor.CreateAsyncQueryExecutor<TEntity>(queryModel);
            // CreateAsync not used, throws: Message: System.ArgumentException : Expression of type 'System.Collections.Generic.IEnumerable`1[EFCore.BulkExtensions.Tests.Item]'
            // cannot be used for return type 'System.Collections.Generic.IAsyncEnumerable`1[EFCore.BulkExtensions.Tests.Item]'

            string sql = modelVisitor.Queries.First().ToString();
            return sql;
            */
        }

        // currently not used
        internal static (string, IEnumerable<SqlParameter>) ToParametrizedSql<TEntity>(this IQueryable<TEntity> query) where TEntity : class
        {
            throw new NotSupportedException("ToParametrizedSql does not work on EFCore 3.");
            
            //DEPRECATED: Used for .NetCore 2 on NetStandard 2.0
            /*
            var queryCompiler = (QueryCompiler)QueryCompilerField.GetValue(query.Provider);
            var modelGenerator = (QueryModelGenerator)QueryModelGeneratorField.GetValue(queryCompiler);
            var parameterValues = new SimpleParameterValues();
            var diagnosticsLogger = new DiagnosticsLogger<DbLoggerCategory.Query>(new LoggerFactory(), null, new DiagnosticListener("Temp"));
            var parameterExpression = modelGenerator.ExtractParameters(diagnosticsLogger, query.Expression, parameterValues);
            var queryModel = modelGenerator.ParseQuery(parameterExpression);
            var database = (IDatabase)DataBaseField.GetValue(queryCompiler);
            var databaseDependencies = (DatabaseDependencies)DatabaseDependenciesField.GetValue(database);
            var queryCompilationContext = databaseDependencies.QueryCompilationContextFactory.Create(false);
            var modelVisitor = (RelationalQueryModelVisitor)queryCompilationContext.CreateQueryModelVisitor();

            modelVisitor.CreateQueryExecutor<TEntity>(queryModel);
            //modelVisitor.CreateAsyncQueryExecutor<TEntity>(queryModel);
            // CreateAsync not used, throws: Message: System.ArgumentException : Expression of type 'System.Collections.Generic.IEnumerable`1[EFCore.BulkExtensions.Tests.Item]'
            // cannot be used for return type 'System.Collections.Generic.IAsyncEnumerable`1[EFCore.BulkExtensions.Tests.Item]'

            string sql = modelVisitor.Queries.First().ToString();
            return (sql, parameterValues.ParameterValues.Select(x => new SqlParameter(x.Key, x.Value)));
            */
        }
    }
}
