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
                if (sqlParameter.DbType == DbType.DateTime)
                {
                    sqlParameter.DbType = DbType.DateTime2; // sets most specific parameter DbType possible for so that precision is not lost
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
