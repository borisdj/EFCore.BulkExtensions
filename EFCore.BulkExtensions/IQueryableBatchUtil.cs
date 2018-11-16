using System;
using System.Collections.Generic;
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
        // In comment are Examples of how QuerySQL is changed for Batch SQL

        // SELECT [a].[Column1], [a].[Column2], ...
        // FROM [Table] AS [a]/r/n
        // WHERE [a].[Column] = FilterValue
        // --
        // DELETE
        // FROM [Table]
        // WHERE [Columns] = FilterValues
        public static string GetSqlDelete<T>(IQueryable<T> query, DbContext context) where T : class
        {
            string sqlQuery = query.ToSql();
            int indexFROM = sqlQuery.IndexOf("FROM");
            string sql = sqlQuery.Substring(indexFROM, sqlQuery.Length - indexFROM);
            sql = ReplaceLetterInBracketsWithA(sql);
            sql = sql.Replace("AS [a]", "").Replace("[a].", "");
            return $"DELETE {sql}";
        }

        // SELECT [a].[Column1], [a].[Column2], ...
        // FROM [Table] AS [a]/r/n
        // WHERE [a].[Column] = FilterValue
        // --
        // UPDATE
        // [Table] SET [UpdateColumns] = 'updateValues'
        // WHERE [Columns] = FilterValues
        public static string GetSqlUpdate<T>(IQueryable<T> query, DbContext context, T updateValues, List<string> updateColumns) where T : class, new()
        {
            string sqlQuery = query.ToSql();
            int indexFROM = sqlQuery.IndexOf("FROM") + 5;
            string sqlSET = GetSqlSetSegment(context, updateValues, updateColumns);
            string sql = sqlQuery.Substring(indexFROM, sqlQuery.Length - indexFROM);
            sql = ReplaceLetterInBracketsWithA(sql);
            sql = sql.Replace("AS [a]", sqlSET).Replace("[a].", "");
            return $"UPDATE {sql}";
        }

        public static string GetSqlSetSegment<T>(DbContext context, T updateValues, List<string> updateColumns) where T : class, new()
        {
            var tableInfo = TableInfo.CreateInstance<T>(context, new List<T>(), OperationType.Read, new BulkConfig());
            string sql = string.Empty;
            Type updateValuesType = typeof(T);
            var defaultValues = new T();
            foreach (var propertyColumn in tableInfo.PropertyColumnNamesDict)
            {
                var property = updateValuesType.GetProperty(propertyColumn.Key);
                var propertyUpdateValue = property.GetValue(updateValues);
                var propertyDefaultValue = property.GetValue(defaultValues);
                bool isDifferentFromDefault = propertyUpdateValue?.ToString() != propertyDefaultValue?.ToString();
                if (isDifferentFromDefault || (updateColumns != null && updateColumns.Contains(propertyColumn.Key)))
                {
                    sql += propertyColumn.Value + " = '" + propertyUpdateValue + "'" + ", ";
                }
            }
            sql = sql.Remove(sql.Length - 2, 2); // removes last excess comma and space: ", "
            return $"SET {sql}";
        }

        public static string ReplaceLetterInBracketsWithA(string sql)
        {
            sql = sql.Replace("[i]", "[a]");
            if (!sql.Contains("[a]"))
            {
                for (char letter = 'b'; letter <= 'z'; letter++)
                {
                    string letterInBracket = $"[{letter}]";
                    if (sql.Contains(letterInBracket))
                    {
                        sql = sql.Replace(letterInBracket, "[a]");
                        break;
                    }
                }
            }
            return sql;
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