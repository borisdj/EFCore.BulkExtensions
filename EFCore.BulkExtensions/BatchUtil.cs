using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Reflection;
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
        public static string GetSqlDelete<T>(IQueryable<T> query) where T : class, new()
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
        public static string GetSqlUpdate<T>(IQueryable<T> query, DbContext context, T updateValues, List<string> updateColumns, List<SqlParameter> parameters) where T : class, new()
        {
            (string sql, string tableAlias) = GetBatchSql(query);
            string sqlSET = GetSqlSetSegment(context, updateValues, updateColumns, parameters);
            return $"UPDATE [{tableAlias}] {sqlSET}{sql}";
        }

        public static (string, string) GetBatchSql<T>(IQueryable<T> query) where T : class, new()
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
                    parameters.Add(new SqlParameter($"@{columnName}", propertyUpdateValue));
                }
            }
            if (String.IsNullOrEmpty(sql))
            {
                throw new InvalidOperationException("SET Columns not defined. If one or more columns should be updated to theirs default value use 'updateColumns' argument.");
            }
            sql = sql.Remove(sql.Length - 2, 2); // removes last excess comma and space: ", "
            return $"SET {sql}";
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
