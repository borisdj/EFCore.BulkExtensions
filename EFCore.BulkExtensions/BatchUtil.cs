using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.ChangeTracking.Internal;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.Internal;

namespace EFCore.BulkExtensions
{
    static class BatchUtil
    {
        // In comment are Examples of how SqlQuery is changed for Sql Batch

        // SELECT [a].[Column1], [a].[Column2], .../r/n
        // FROM [Table] AS [a]/r/n
        // WHERE [a].[Column] = FilterValue
        // --
        // DELETE [a]
        // FROM [Table] AS [a]
        // WHERE [a].[Columns] = FilterValues
        public static string GetSqlDelete<T>(IQueryable<T> query) where T : class
        {
            (string sql, string tableAlias) = GetBatchSql(query);
            return $"DELETE [{tableAlias}]{sql}";
        }

        // SELECT [a].[Column1], [a].[Column2], .../r/n
        // FROM [Table] AS [a]/r/n
        // WHERE [a].[Column] = FilterValue
        // --
        // UPDATE [a] SET [UpdateColumns] = N'updateValues'
        // FROM [Table] AS [a]
        // WHERE [a].[Columns] = FilterValues
        public static (string, List<SqlParameter>) GetSqlUpdate<T>(IQueryable<T> query, DbContext context, T updateValues, List<string> updateColumns) where T : class, new()
        {
            (string sql, string tableAlias) = GetBatchSql(query);
            var sqlParameters = new List<SqlParameter>();
            string sqlSET = GetSqlSetSegment(context, updateValues, updateColumns, sqlParameters);
            return ($"UPDATE [{tableAlias}] {sqlSET}{sql}", sqlParameters);
        }

        /// <summary>
        /// get Update Sql
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="query"></param>
        /// <param name="expression"></param>
        /// <returns></returns>
        public static (string, List<SqlParameter>) GetSqlUpdate<T>(IQueryable<T> query, Expression<Func<T, T>> expression) where T : class
        {
            (string sql, string tableAlias) = GetBatchSql(query);
            var sqlColumns = new StringBuilder();
            var sqlParameters = new List<SqlParameter>();
            var columnNameValueDict = TableInfo.CreateInstance(GetDbContext(query), new List<T>(), OperationType.Read, new BulkConfig()).PropertyColumnNamesDict;
            CreateUpdateBody(columnNameValueDict, tableAlias, expression.Body, ref sqlColumns, ref sqlParameters);
            return ($"UPDATE [{tableAlias}] SET {sqlColumns.ToString()} {sql}", sqlParameters);
        }

        public static (string, string) GetBatchSql<T>(IQueryable<T> query) where T : class
        {
            string sqlQuery = query.ToSql();
            string tableAlias = sqlQuery.Substring(8, sqlQuery.IndexOf("]") - 8);
            int indexFROM = sqlQuery.IndexOf(Environment.NewLine);
            string sql = sqlQuery.Substring(indexFROM, sqlQuery.Length - indexFROM);
            sql = sql.Contains("{") ? sql.Replace("{", "{{") : sql; // Curly brackets have to escaped:
            sql = sql.Contains("}") ? sql.Replace("}", "}}") : sql; // https://github.com/aspnet/EntityFrameworkCore/issues/8820
            return (sql, tableAlias);
        }

        public static string GetSqlSetSegment<T>(DbContext context, T updateValues, List<string> updateColumns, List<SqlParameter> parameters) where T : class, new()
        {
            var tableInfo = TableInfo.CreateInstance<T>(context, new List<T>(), OperationType.Read, new BulkConfig());
            string sql = string.Empty;
            Type updateValuesType = typeof(T);
            var defaultValues = new T();
            foreach (var propertyNameColumnName in tableInfo.PropertyColumnNamesDict)
            {
                string propertyName = propertyNameColumnName.Key;
                string columnName = propertyNameColumnName.Value;
                var pArray = propertyName.Split(new char[] { '.' });
                Type lastType = updateValuesType;
                PropertyInfo property = lastType.GetProperty(pArray[0]);
                if (property != null)
                {
                    object propertyUpdateValue = property.GetValue(updateValues);
                    object propertyDefaultValue = property.GetValue(defaultValues);
                    for (int i = 1; i < pArray.Length; i++)
                    {
                        lastType = property.PropertyType;
                        property = lastType.GetProperty(pArray[i]);
                        propertyUpdateValue = propertyUpdateValue != null ? property.GetValue(propertyUpdateValue) : propertyUpdateValue;
                        var lastDefaultValues = lastType.Assembly.CreateInstance(lastType.FullName);
                        propertyDefaultValue = property.GetValue(lastDefaultValues);
                    }

                    bool isDifferentFromDefault = propertyUpdateValue != null && propertyUpdateValue?.ToString() != propertyDefaultValue?.ToString();
                    if (isDifferentFromDefault || (updateColumns != null && updateColumns.Contains(propertyName)))
                    {
                        sql += $"[{columnName}] = @{columnName}, ";
                        propertyUpdateValue = propertyUpdateValue ?? DBNull.Value;
                        parameters.Add(new SqlParameter($"@{columnName}", propertyUpdateValue));
                    }
                }
            }
            if (String.IsNullOrEmpty(sql))
            {
                throw new InvalidOperationException("SET Columns not defined. If one or more columns should be updated to theirs default value use 'updateColumns' argument.");
            }
            sql = sql.Remove(sql.Length - 2, 2); // removes last excess comma and space: ", "
            return $"SET {sql}";
        }

        /// <summary>
        /// Recursive analytic expression 
        /// </summary>
        /// <param name="tableAlias"></param>
        /// <param name="expression"></param>
        /// <param name="sqlColumns"></param>
        /// <param name="sqlParameters"></param>
        /// <summary>
        /// Recursive analytic expression 
        /// </summary>
        /// <param name="tableAlias"></param>
        /// <param name="expression"></param>
        /// <param name="sqlColumns"></param>
        /// <param name="sqlParameters"></param>
        public static void CreateUpdateBody(Dictionary<string, string> columnNameValueDict, string tableAlias, Expression expression, ref StringBuilder sqlColumns, ref List<SqlParameter> sqlParameters)
        {
            if (expression is MemberInitExpression memberInitExpression)
            {
                foreach (var item in memberInitExpression.Bindings)
                {
                    if (item is MemberAssignment assignment)
                    {
                        if (columnNameValueDict.TryGetValue(assignment.Member.Name, out string value))
                            sqlColumns.Append($" [{tableAlias}].[{value}]");
                        else
                            sqlColumns.Append($" [{tableAlias}].[{assignment.Member.Name}]");

                        sqlColumns.Append(" =");

                        CreateUpdateBody(columnNameValueDict, tableAlias, assignment.Expression, ref sqlColumns, ref sqlParameters);

                        if (memberInitExpression.Bindings.IndexOf(item) < (memberInitExpression.Bindings.Count - 1))
                            sqlColumns.Append(" ,");
                    }
                }
            }
            else if (expression is MemberExpression memberExpression && memberExpression.Expression is ParameterExpression)
            {
                if (columnNameValueDict.TryGetValue(memberExpression.Member.Name, out string value))
                    sqlColumns.Append($" [{tableAlias}].[{value}]");
                else
                    sqlColumns.Append($" [{tableAlias}].[{memberExpression.Member.Name}]");
            }
            else if (expression is ConstantExpression constantExpression)
            {
                var parmName = $"param_{sqlParameters.Count}";
                sqlParameters.Add(new SqlParameter(parmName, constantExpression.Value));
                sqlColumns.Append($" @{parmName}");
            }
            else if (expression is UnaryExpression unaryExpression)
            {
                switch (unaryExpression.NodeType)
                {
                    case ExpressionType.Convert:
                        CreateUpdateBody(columnNameValueDict, tableAlias, unaryExpression.Operand, ref sqlColumns, ref sqlParameters);
                        break;
                    case ExpressionType.Not:
                        sqlColumns.Append(" ~");//this way only for SQL Server 
                        CreateUpdateBody(columnNameValueDict, tableAlias, unaryExpression.Operand, ref sqlColumns, ref sqlParameters);
                        break;
                    default: break;
                }
            }
            else if (expression is BinaryExpression binaryExpression)
            {
                CreateUpdateBody(columnNameValueDict, tableAlias, binaryExpression.Left, ref sqlColumns, ref sqlParameters);

                switch (binaryExpression.NodeType)
                {
                    case ExpressionType.Add:
                        sqlColumns.Append(" +");
                        break;
                    case ExpressionType.Divide:
                        sqlColumns.Append(" /");
                        break;
                    case ExpressionType.Multiply:
                        sqlColumns.Append(" *");
                        break;
                    case ExpressionType.Subtract:
                        sqlColumns.Append(" -");
                        break;
                    case ExpressionType.And:
                        sqlColumns.Append(" &");
                        break;
                    case ExpressionType.Or:
                        sqlColumns.Append(" |");
                        break;
                    default: break;
                }

                CreateUpdateBody(columnNameValueDict, tableAlias, binaryExpression.Right, ref sqlColumns, ref sqlParameters);
            }
            else
            {
                var value = Expression.Lambda(expression).Compile().DynamicInvoke();
                var parmName = $"param_{sqlParameters.Count}";
                sqlParameters.Add(new SqlParameter(parmName, value));
                sqlColumns.Append($" @{parmName}");
            }
        }


        public static DbContext GetDbContext(IQueryable query)
        {
            var bindingFlags = BindingFlags.NonPublic | BindingFlags.Instance;
            var queryCompiler = typeof(EntityQueryProvider).GetField("_queryCompiler", bindingFlags).GetValue(query.Provider);
            var queryContextFactory = queryCompiler.GetType().GetField("_queryContextFactory", bindingFlags).GetValue(queryCompiler);

            var dependencies = typeof(RelationalQueryContextFactory).GetProperty("Dependencies", bindingFlags).GetValue(queryContextFactory);
            var queryContextDependencies = typeof(DbContext).Assembly.GetType(typeof(QueryContextDependencies).FullName);
            var stateManagerProperty = queryContextDependencies.GetProperty("StateManager", bindingFlags | BindingFlags.Public).GetValue(dependencies);
            var stateManager = (IStateManager)stateManagerProperty;

            return stateManager.Context;
        }
    }
}
