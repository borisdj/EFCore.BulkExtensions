using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Internal;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.Internal;
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
            // ! IMPORTANT TODO:
            throw new NotSupportedException("Currently not supported for .NET Core 3.0, because in v3.0 there are no classes 'QueryModelGenerator' and 'RelationalQueryModelVisitor'. Will be supported after finding alternative for implementing .ToSql();");
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

        internal static (string, IEnumerable<SqlParameter>) ToParametrizedSql<TEntity>(this IQueryable<TEntity> query) where TEntity : class
        {
            throw new NotSupportedException("Currently not supported for .NET Core 3.0, because in v3.0 there are no classes 'QueryModelGenerator' and 'RelationalQueryModelVisitor'. Will be supported after finding alternative for implementing .ToSql();");
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
