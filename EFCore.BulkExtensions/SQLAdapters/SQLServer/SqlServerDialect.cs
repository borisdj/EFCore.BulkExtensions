using EFCore.BulkExtensions.SqlAdapters;
using Microsoft.EntityFrameworkCore;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq.Expressions;

namespace EFCore.BulkExtensions.SQLAdapters.SQLServer
{
    public class SqlServerDialect : IQueryBuilderSpecialization
    {
        private static readonly int SelectStatementLength = "SELECT".Length;

        public virtual List<object> ReloadSqlParameters(DbContext context, List<object> sqlParameters)
        {
            var sqlParametersReloaded = new List<object>();
            foreach (var parameter in sqlParameters)
            {
                var sqlParameter = (IDbDataParameter)parameter;

                try
                {
                    var dt = sqlParameter.DbType;
                    if (sqlParameter.DbType == DbType.DateTime)
                    {
                        sqlParameter.DbType = DbType.DateTime2; // sets most specific parameter DbType possible for so that precision is not lost
                    }
                }
                catch (Exception ex)
                {
                    string noMappingText = "No mapping exists from object type "; // Fixes for Batch ops on PostgreSQL with:
                    if (!ex.Message.StartsWith(noMappingText + "System.Collections.Generic.List") &&             // - Contains
                        !ex.Message.StartsWith(noMappingText + typeof(System.Text.Json.JsonElement).FullName) && // - JsonElement param
                        !ex.Message.StartsWith(noMappingText + typeof(System.Text.Json.JsonDocument).FullName))  // - JsonElement param
                    {
                        throw;
                    }
                }
                sqlParametersReloaded.Add(sqlParameter);
            }
            return sqlParametersReloaded;
        }

        public string GetBinaryExpressionAddOperation(BinaryExpression binaryExpression)
        {
            return "+";
        }

        public (string, string) GetBatchSqlReformatTableAliasAndTopStatement(string sqlQuery, DbServer databaseType)
        {
            var isPostgreSql = databaseType == DbServer.PostgreSQL;
            var escapeSymbolEnd = isPostgreSql ? "." : "]";
            var escapeSymbolStart = isPostgreSql ? " " : "["; // SqlServer : PostrgeSql;
            var tableAliasEnd = sqlQuery.Substring(SelectStatementLength, sqlQuery.IndexOf(escapeSymbolEnd, StringComparison.Ordinal) - SelectStatementLength); // " TOP(10) [table_alias" / " [table_alias" : " table_alias"
            var tableAliasStartIndex = tableAliasEnd.IndexOf(escapeSymbolStart, StringComparison.Ordinal);
            var tableAlias = tableAliasEnd.Substring(tableAliasStartIndex + escapeSymbolStart.Length); // "table_alias"
            var topStatement = tableAliasEnd.Substring(0, tableAliasStartIndex).TrimStart(); // "TOP(10) " / if TOP not present in query this will be a Substring(0,0) == ""
            return (tableAlias, topStatement);
        }

        public ExtractedTableAlias GetBatchSqlExtractTableAliasFromQuery(string fullQuery, string tableAlias, string tableAliasSuffixAs)
        {
            return new ExtractedTableAlias
            {
                TableAlias = tableAlias,
                TableAliasSuffixAs = tableAliasSuffixAs,
                Sql = fullQuery
            };
        }
    }
}
