using EFCore.BulkExtensions.SqlAdapters;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.ChangeTracking.Internal;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.Internal;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Text.RegularExpressions;

namespace EFCore.BulkExtensions;

/// <summary>
/// Class responsible for all batch utilities related to EFCore.BulkExtensions
/// </summary>
public static class BatchUtil
{
    // In comment are Examples of how SqlQuery is changed for Sql Batch

    // SELECT [a].[Column1], [a].[Column2], .../r/n
    // FROM [Table] AS [a]/r/n
    // WHERE [a].[Column] = FilterValue
    // --
    // DELETE [a]
    // FROM [Table] AS [a]
    // WHERE [a].[Columns] = FilterValues
    /// <summary>
    /// Generates delete sql query
    /// </summary>
    /// <param name="query"></param>
    /// <param name="context"></param>
    /// <returns></returns>
    public static (string, List<DbParameter>) GetSqlDelete(IQueryable query, DbContext context)
    {
        var (sql, tableAlias, _, topStatement, leadingComments, innerParameters) = GetBatchSql(query, context, isUpdate: false);

        innerParameters = ReloadSqlParameters(context, innerParameters.ToList()); // Sqlite requires SqliteParameters
        var databaseType = SqlAdaptersMapping.GetDatabaseType();

        string resultQuery;
        if (databaseType == SqlType.SqlServer)
        {
            tableAlias = $"[{tableAlias}]";
            int outerQueryOrderByIndex = -1;
            var useUpdateableCte = false;
            var lastOrderByIndex = sql.LastIndexOf(Environment.NewLine + $"ORDER BY ", StringComparison.OrdinalIgnoreCase);
            if (lastOrderByIndex > -1)
            {
                var subQueryEnd = sql.LastIndexOf($") AS {tableAlias}" + Environment.NewLine, StringComparison.OrdinalIgnoreCase);
                if (subQueryEnd == -1 || lastOrderByIndex > subQueryEnd)
                {
                    outerQueryOrderByIndex = lastOrderByIndex;

                    if (topStatement.Length > 0)
                    {
                        useUpdateableCte = true;
                    }
                    else
                    {
                        int offSetIndex = sql.LastIndexOf(Environment.NewLine + "OFFSET ", StringComparison.OrdinalIgnoreCase);
                        if (offSetIndex > outerQueryOrderByIndex)
                        {
                            useUpdateableCte = true;
                        }
                    }
                }
            }

            if (useUpdateableCte)
            {
                var cte = string.Concat("cte", Guid.NewGuid().ToString().AsSpan(0, 8)); // 8 chars of Guid as tableNameSuffix to avoid same name collision with other tables
                resultQuery = $"{leadingComments}WITH [{cte}] AS (SELECT {topStatement}* {sql}) DELETE FROM [{cte}]";
            }
            else
            {
                if (outerQueryOrderByIndex > -1)
                {
                    // ORDER BY is not allowed without TOP or OFFSET.
                    sql = sql[..outerQueryOrderByIndex];
                }

                resultQuery = $"{leadingComments}DELETE {topStatement}{tableAlias}{sql}";
            }
        }
        else
        {
            resultQuery = $"{leadingComments}DELETE {topStatement}{tableAlias}{sql}";
        }

        if (databaseType == SqlType.PostgreSql)
        {
            resultQuery = SqlAdaptersMapping.DbServer.QueryBuilder.RestructureForBatch(resultQuery, isDelete: true);

            var npgsqlParameters = new List<DbParameter>();
            foreach (var param in innerParameters)
            {
                npgsqlParameters.Add(SqlAdaptersMapping.DbServer.QueryBuilder.CreateParameter(param.ParameterName, param.Value));
            }
            innerParameters = npgsqlParameters;
        }

        return (resultQuery, new List<DbParameter>(innerParameters));
    }

    // SELECT [a].[Column1], [a].[Column2], .../r/n
    // FROM [Table] AS [a]/r/n
    // WHERE [a].[Column] = FilterValue
    // --
    // UPDATE [a] SET [UpdateColumns] = N'updateValues'
    // FROM [Table] AS [a]
    // WHERE [a].[Columns] = FilterValues
    /// <summary>
    /// Generates sql query to update data
    /// </summary>
    /// <param name="query"></param>
    /// <param name="context"></param>
    /// <param name="type"></param>
    /// <param name="updateValues"></param>
    /// <param name="updateColumns"></param>
    /// <returns></returns>
    public static (string, List<DbParameter>) GetSqlUpdate(IQueryable query, DbContext context, Type type, object? updateValues, List<string>? updateColumns)
    {
        var (sql, tableAlias, tableAliasSufixAs, topStatement, leadingComments, innerParameters) = GetBatchSql(query, context, isUpdate: true);
        var sqlParameters = new List<DbParameter>(innerParameters);

        string sqlSET = GetSqlSetSegment(context, updateValues?.GetType(), updateValues, updateColumns, sqlParameters);

        sqlParameters = ReloadSqlParameters(context, sqlParameters); // Sqlite requires SqliteParameters

        var (resultQuery, sqlParametersResult) = PrepareSqlUpdate(type, sqlParameters, sql, sqlSET, tableAlias, tableAliasSufixAs, topStatement, leadingComments, null);
        return (resultQuery, sqlParametersResult);
    }

    /// <summary>
    /// get Update Sql
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <param name="query"></param>
    /// <param name="expression"></param>
    /// <param name="context"></param>
    /// <param name="type"></param>
    /// <returns></returns>
    public static (string, List<DbParameter>) GetSqlUpdate<T>(IQueryable<T> query, DbContext context, Type type, Expression<Func<T, T>> expression) where T : class
    {
        (string sql, string tableAlias, string tableAliasSufixAs, string topStatement, string leadingComments, IEnumerable<DbParameter> innerParameters) = GetBatchSql(query, context, isUpdate: true);

        var createUpdateBodyData = new BatchUpdateCreateBodyData(sql, context, innerParameters, query, type, tableAlias, expression);

        CreateUpdateBody(createUpdateBodyData, expression.Body);

        var sqlParameters = ReloadSqlParameters(context, createUpdateBodyData.SqlParameters); // Sqlite requires SqliteParameters
        var sqlColumns = (createUpdateBodyData.DatabaseType == SqlType.SqlServer) 
            ? createUpdateBodyData.UpdateColumnsSql
            : createUpdateBodyData.UpdateColumnsSql.Replace($"[{tableAlias}].", "");

        string sqlSET = $"SET {sqlColumns}";

        var (resultQuery, sqlParametersResult) = PrepareSqlUpdate(type, sqlParameters, sql, sqlSET, tableAlias, tableAliasSufixAs, topStatement, leadingComments, sqlColumns);
        return (resultQuery, sqlParametersResult);
    }

    /// <summary>
    /// PrepareSqlUpdate
    /// </summary>
    /// <param name="type"></param>
    /// <param name="sqlParameters"></param>
    /// <param name="sql"></param>
    /// <param name="sqlSET"></param>
    /// <param name="tableAlias"></param>
    /// <param name="tableAliasSufixAs"></param>
    /// <param name="topStatement"></param>
    /// <param name="leadingComments"></param>
    /// <param name="sqlColumns"></param>
    /// <returns></returns>
    public static (string, List<DbParameter>) PrepareSqlUpdate(Type type, List<DbParameter> sqlParameters, string sql, string sqlSET, string tableAlias, string tableAliasSufixAs, string topStatement, string leadingComments, StringBuilder? sqlColumns)
    {
        var resultQuery = $"{leadingComments}UPDATE {topStatement}{tableAlias}{tableAliasSufixAs} {sqlSET} {sql}";

        if (resultQuery.Contains("ORDER") && resultQuery.Contains("TOP"))
        {
            if (sqlColumns != null) // CHECK : remove if so Replace to be always called
            {
                string tableAliasPrefix = "[" + tableAlias + "].";
                sqlSET = sqlSET.Replace(tableAliasPrefix, "");
            }
            resultQuery = $"WITH C AS (SELECT {topStatement}*{sql}) UPDATE C SET {sqlSET}";
        }
        if (resultQuery.Contains("ORDER") && !resultQuery.Contains("TOP")) // When query has ORDER only without TOP(Take) then it is removed since not required and to avoid invalid Sql
        {
            resultQuery = resultQuery.Split("ORDER", StringSplitOptions.None)[0];
        }

        var databaseType = SqlAdaptersMapping.GetDatabaseType();
        if (databaseType == SqlType.PostgreSql)
        {
            resultQuery = SqlAdaptersMapping.DbServer.QueryBuilder.RestructureForBatch(resultQuery);

            var npgsqlParameters = new List<DbParameter>();
            foreach (var param in sqlParameters)
            {
                dynamic npgsqlParam = SqlAdaptersMapping.DbServer.QueryBuilder.CreateParameter(param.ParameterName, param.Value);

                string paramName = npgsqlParam.ParameterName.Replace("@", "");
                var propertyType = type.GetProperties().SingleOrDefault(a => a.Name == paramName)?.PropertyType;
                if (propertyType == typeof(System.Text.Json.JsonElement) || propertyType == typeof(System.Text.Json.JsonElement?)) // for JsonDocument works without fix
                {
                    var dbtypeJsonb = SqlAdaptersMapping.DbServer.QueryBuilder.Dbtype();
                    SqlAdaptersMapping.DbServer.QueryBuilder.SetDbTypeParam(npgsqlParam, dbtypeJsonb);
                }

                npgsqlParameters.Add(npgsqlParam);
            }
            sqlParameters = npgsqlParameters;
        }
        else if (databaseType == SqlType.MySql)
        {
            resultQuery = SqlAdaptersMapping.DbServer.QueryBuilder.RestructureForBatch(resultQuery);

            var mysqlParameters = new List<DbParameter>();
            foreach (var param in sqlParameters)
            {
                dynamic mysqlParam = SqlAdaptersMapping.DbServer.QueryBuilder.CreateParameter(param.ParameterName, param.Value);

                string paramName = mysqlParam.ParameterName.Replace("@", "");
                var propertyType = type.GetProperties().SingleOrDefault(a => a.Name == paramName)?.PropertyType;
                if (propertyType == typeof(System.Text.Json.JsonElement) || propertyType == typeof(System.Text.Json.JsonElement?)) // for JsonDocument works without fix
                {
                    var dbtypeJsonb = SqlAdaptersMapping.DbServer.QueryBuilder.Dbtype();
                    SqlAdaptersMapping.DbServer.QueryBuilder.SetDbTypeParam(mysqlParam, dbtypeJsonb);
                }

                mysqlParameters.Add(mysqlParam);
            }
            sqlParameters = mysqlParameters;
        }
        else if (databaseType == SqlType.Oracle)
        {
            resultQuery = SqlAdaptersMapping.DbServer.QueryBuilder.RestructureForBatch(resultQuery);

            var mysqlParameters = new List<DbParameter>();
            foreach (var param in sqlParameters)
            {
                dynamic mysqlParam = SqlAdaptersMapping.DbServer.QueryBuilder.CreateParameter(param.ParameterName, param.Value);

                string paramName = mysqlParam.ParameterName.Replace(":", "");
                var propertyType = type.GetProperties().SingleOrDefault(a => a.Name == paramName)?.PropertyType;
                if (propertyType == typeof(System.Text.Json.JsonElement) || propertyType == typeof(System.Text.Json.JsonElement?)) // for JsonDocument works without fix
                {
                    var dbtypeJsonb = SqlAdaptersMapping.DbServer.QueryBuilder.Dbtype();
                    SqlAdaptersMapping.DbServer.QueryBuilder.SetDbTypeParam(mysqlParam, dbtypeJsonb);
                }

                mysqlParameters.Add(mysqlParam);
            }
            sqlParameters = mysqlParameters;
        }

        return (resultQuery, sqlParameters);
    }

    /// <summary>
    /// Reloads SQL paramaters
    /// </summary>
    /// <param name="context"></param>
    /// <param name="sqlParameters"></param>
    /// <returns></returns>
    public static List<DbParameter> ReloadSqlParameters(DbContext context, List<DbParameter> sqlParameters)
    {
        return SqlAdaptersMapping.GetAdapterDialect().ReloadSqlParameters(context,sqlParameters);
    }

    /// <summary>
    /// Generates SQL queries for batch operations
    /// </summary>
    /// <param name="query"></param>
    /// <param name="context"></param>
    /// <param name="isUpdate"></param>
    /// <returns></returns>
    public static (string Sql, string TableAlias, string TableAliasSufixAs, string TopStatement, string LeadingComments, IEnumerable<DbParameter> InnerParameters) GetBatchSql(IQueryable query, DbContext context, bool isUpdate)
    {
        SqlAdaptersMapping.ProviderName = context.Database.ProviderName;
        var sqlQueryBuilder = SqlAdaptersMapping.GetAdapterDialect();
        var (fullSqlQuery, innerParameters) = query.ToParametrizedSql();

        var (leadingComments, sqlQuery) = SplitLeadingCommentsAndMainSqlQuery(fullSqlQuery);

        string tableAlias;
        string tableAliasSufixAs = string.Empty;
        string topStatement;

        var databaseType = SqlAdaptersMapping.GetDatabaseType();
        (tableAlias, topStatement) = sqlQueryBuilder.GetBatchSqlReformatTableAliasAndTopStatement(sqlQuery, databaseType);

        int indexFrom = sqlQuery.IndexOf(Environment.NewLine, StringComparison.Ordinal);
        string sql = sqlQuery[indexFrom..];
        sql = sql.Contains('{') ? sql.Replace("{", "{{") : sql; // Curly brackets have to be escaped:
        sql = sql.Contains('}') ? sql.Replace("}", "}}") : sql; // https://github.com/aspnet/EntityFrameworkCore/issues/8820

        if (isUpdate)
        {
            var extracted = sqlQueryBuilder.GetBatchSqlExtractTableAliasFromQuery(
                sql, tableAlias, tableAliasSufixAs
            );
            tableAlias = extracted.TableAlias;
            tableAliasSufixAs = extracted.TableAliasSuffixAs;
            sql = extracted.Sql;
        }

        return (sql, tableAlias, tableAliasSufixAs, topStatement, leadingComments, innerParameters);
    }

    /// <summary>
    /// Returns a sql set seqment query
    /// </summary>
    /// <param name="context"></param>
    /// <param name="updateValuesType"></param>
    /// <param name="updateValues"></param>
    /// <param name="updateColumns"></param>
    /// <param name="parameters"></param>
    /// <returns></returns>
    public static string GetSqlSetSegment(DbContext context, Type? updateValuesType, object? updateValues, List<string>? updateColumns, List<DbParameter> parameters)
    {
        var tableInfo = TableInfo.CreateInstance(context, updateValuesType, new List<object>(), OperationType.Read, new BulkConfig());
        return GetSqlSetSegment(tableInfo, updateValuesType, updateValues, updateValuesType is null ? null : Activator.CreateInstance(updateValuesType), updateColumns, parameters);
    }

    private static string GetSqlSetSegment(TableInfo tableInfo, Type? updateValuesType, object? updateValues, object? defaultValues, List<string>? updateColumns, List<DbParameter> parameters)
    {
        string sql = string.Empty;
        foreach (var propertyNameColumnName in tableInfo.PropertyColumnNamesDict)
        {
            string propertyName = propertyNameColumnName.Key;
            string columnName = propertyNameColumnName.Value;
            var pArray = propertyName.Split(new char[] { '.' });
            Type? lastType = updateValuesType;
            PropertyInfo? property = lastType?.GetProperty(pArray[0]);
            if (property != null)
            {
                object? propertyUpdateValue = property.GetValue(updateValues);
                object? propertyDefaultValue = property.GetValue(defaultValues);

                for (int i = 1; i < pArray.Length; i++)
                {
                    lastType = property?.PropertyType;
                    property = lastType?.GetProperty(pArray[i]);
                    propertyUpdateValue = propertyUpdateValue != null
                        ? property?.GetValue(propertyUpdateValue)
                        : propertyUpdateValue;

                    var lastDefaultValues = lastType!.Assembly.CreateInstance(lastType.FullName!);
                    propertyDefaultValue = property?.GetValue(lastDefaultValues);
                }

                if (tableInfo.ConvertibleColumnConverterDict.ContainsKey(columnName))
                {
                    bool isEnum = tableInfo.ColumnToPropertyDictionary[columnName].ClrType.IsEnum;
                    if (!isEnum) // Omit from ConvertibleColumns because there Enum of byte type gets converter to Number which is then different from default enum value // Test: RunBatchUpdateEnum
                    {
                        propertyUpdateValue = tableInfo.ConvertibleColumnConverterDict[columnName].ConvertToProvider.Invoke(propertyUpdateValue);
                        propertyDefaultValue = tableInfo.ConvertibleColumnConverterDict[columnName].ConvertToProvider.Invoke(propertyDefaultValue);
                    }
                }

                bool isDifferentFromDefault = propertyUpdateValue != null && propertyUpdateValue?.ToString() != propertyDefaultValue?.ToString();
                bool updateColumnExplicit = updateColumns != null && updateColumns.Contains(propertyName);
                if (isDifferentFromDefault || updateColumnExplicit)
                {
                    sql += $"[{columnName}] = @{columnName}, ";
                    var parameterName = $"@{columnName}";
                    DbParameter? param = TryCreateRelationalMappingParameter(
                        tableInfo,
                        columnName,
                        parameterName,
                        propertyUpdateValue);

                    if (param == null)
                    {
                        propertyUpdateValue ??= DBNull.Value;

                        param = SqlAdaptersMapping.DbServer.QueryBuilder.CreateParameter($"@{columnName}", propertyUpdateValue);

                        if (!isDifferentFromDefault && propertyUpdateValue == DBNull.Value
                            && property?.PropertyType == typeof(byte[])) // needed only when having complex type property to be updated to default 'null'
                        {
                            param.DbType = DbType.Binary; // fix for ByteArray since implicit conversion nvarchar to varbinary(max) is not allowed
                        }
                    }

                    parameters.Add(param);
                }
            }
        }
        if (string.IsNullOrEmpty(sql))
        {
            throw new InvalidOperationException("SET Columns not defined. If one or more columns should be updated to theirs default value use 'updateColumns' argument.");
        }
        sql = sql.Remove(sql.Length - 2, 2); // removes last excess comma and space: ", "
        return $"SET {sql}";
    }

    /// <summary>
    /// Recursive analytic expression 
    /// </summary>
    /// <param name="createBodyData"></param>
    /// <param name="expression"></param>
    /// <param name="columnName"></param>
    /// <exception cref="NotSupportedException"></exception>
    public static void CreateUpdateBody(BatchUpdateCreateBodyData createBodyData, Expression expression, string? columnName = null)
    {
        var rootTypeTableInfo = createBodyData.GetTableInfoForType(createBodyData.RootType);
        var columnNameValueDict = rootTypeTableInfo?.PropertyColumnNamesDict;
        var tableAlias = createBodyData.TableAlias;
        var sqlColumns = createBodyData.UpdateColumnsSql;
        var sqlParameters = createBodyData.SqlParameters;

        if (expression is MemberInitExpression memberInitExpression)
        {
            foreach (var item in memberInitExpression.Bindings)
            {
                if (item is MemberAssignment assignment)
                {
                    string? currentColumnName;
                    if (columnNameValueDict?.TryGetValue(assignment.Member.Name, out string? value) ?? false)
                        currentColumnName = value;
                    else
                        currentColumnName = assignment.Member.Name;

                    sqlColumns.Append($" [{tableAlias}].[{currentColumnName}]");
                    sqlColumns.Append(" =");

                    if (!TryCreateUpdateBodyNestedQuery(createBodyData, assignment.Expression, assignment))
                    {
                        CreateUpdateBody(createBodyData, assignment.Expression, currentColumnName);
                    }

                    if (memberInitExpression.Bindings.IndexOf(item) < (memberInitExpression.Bindings.Count - 1))
                        sqlColumns.Append(" ,");
                }
            }

            return;
        }

        if (expression is MemberExpression memberExpression
            && memberExpression.Expression is ParameterExpression parameterExpression
            && parameterExpression.Name == createBodyData.RootInstanceParameterName)
        {
            if (columnNameValueDict?.TryGetValue(memberExpression.Member.Name, out string? value) ?? false)
            {
                sqlColumns.Append($" [{tableAlias}].[{value}]");
            }
            else
            {
                sqlColumns.Append($" [{tableAlias}].[{memberExpression.Member.Name}]");
            }

            return;
        }

        if (expression is ConstantExpression constantExpression)
        {
            // TODO: I believe the EF query builder inserts constant expressions directly into the SQL.
            // This should probably match that behavior for the update body
            AddSqlParameter(sqlColumns, sqlParameters, rootTypeTableInfo, columnName, constantExpression.Value);
            return;
        }

        if (expression is UnaryExpression unaryExpression)
        {
            switch (unaryExpression.NodeType)
            {
                case ExpressionType.Convert:
                    CreateUpdateBody(createBodyData, unaryExpression.Operand, columnName);
                    break;
                case ExpressionType.Not:
                    sqlColumns.Append(" ~");//this way only for SQL Server 
                    CreateUpdateBody(createBodyData, unaryExpression.Operand, columnName);
                    break;
                default: break;
            }

            return;
        }

        if (expression is BinaryExpression binaryExpression)
        {
            switch (binaryExpression.NodeType)
            {
                case ExpressionType.Add:
                    CreateUpdateBody(createBodyData, binaryExpression.Left, columnName);
                    var sqlOperator = SqlAdaptersMapping.GetAdapterDialect()
                        .GetBinaryExpressionAddOperation(binaryExpression);
                    sqlColumns.Append(" " + sqlOperator);
                    CreateUpdateBody(createBodyData, binaryExpression.Right, columnName);
                    break;

                case ExpressionType.Divide:
                    CreateUpdateBody(createBodyData, binaryExpression.Left, columnName);
                    sqlColumns.Append(" /");
                    CreateUpdateBody(createBodyData, binaryExpression.Right, columnName);
                    break;

                case ExpressionType.Multiply:
                    CreateUpdateBody(createBodyData, binaryExpression.Left, columnName);
                    sqlColumns.Append(" *");
                    CreateUpdateBody(createBodyData, binaryExpression.Right, columnName);
                    break;

                case ExpressionType.Subtract:
                    CreateUpdateBody(createBodyData, binaryExpression.Left, columnName);
                    sqlColumns.Append(" -");
                    CreateUpdateBody(createBodyData, binaryExpression.Right, columnName);
                    break;

                case ExpressionType.And:
                    CreateUpdateBody(createBodyData, binaryExpression.Left, columnName);
                    sqlColumns.Append(" &");
                    CreateUpdateBody(createBodyData, binaryExpression.Right, columnName);
                    break;

                case ExpressionType.Or:
                    CreateUpdateBody(createBodyData, binaryExpression.Left, columnName);
                    sqlColumns.Append(" |");
                    CreateUpdateBody(createBodyData, binaryExpression.Right, columnName);
                    break;

                case ExpressionType.ExclusiveOr:
                    CreateUpdateBody(createBodyData, binaryExpression.Left, columnName);
                    sqlColumns.Append(" ^");
                    CreateUpdateBody(createBodyData, binaryExpression.Right, columnName);
                    break;

                case ExpressionType.Coalesce:
                    sqlColumns.Append("COALESCE(");
                    CreateUpdateBody(createBodyData, binaryExpression.Left, columnName);
                    sqlColumns.Append(',');
                    CreateUpdateBody(createBodyData, binaryExpression.Right, columnName);
                    sqlColumns.Append(')');
                    break;

                default:
                    throw new NotSupportedException($"{nameof(BatchUtil)}.{nameof(CreateUpdateBody)}(..) is not supported for a binary exression of type {binaryExpression.NodeType}");
            }

            return;
        }

        // For any other case fallback on compiling and executing the expression
        var compiledExpressionValue = Expression.Lambda(expression).Compile().DynamicInvoke();
        AddSqlParameter(sqlColumns, sqlParameters, rootTypeTableInfo, columnName, compiledExpressionValue);
    }

    /// <summary>
    /// Returns the DbContext
    /// </summary>
    /// <param name="query"></param>
    /// <returns></returns>
    public static DbContext? GetDbContext(IQueryable query)
    {
#pragma warning disable EF1001 // Internal EF Core API usage.
        const BindingFlags bindingFlags = BindingFlags.NonPublic | BindingFlags.Instance;

        var provider = query.Provider;
        if (provider is not EntityQueryProvider) // handling wrapped instances for cases like the DelegateDecompiler
        {
            var providerProp = query.Provider.GetType().GetProperties(bindingFlags).FirstOrDefault(x => x.PropertyType == typeof(IQueryProvider));
            provider = providerProp?.GetValue(query.Provider) as IQueryProvider ?? throw new NotSupportedException($"Could not an EntityQueryProvider either directly or from a wrapped instance");
        }
        var queryCompiler = typeof(EntityQueryProvider).GetField("_queryCompiler", bindingFlags)?.GetValue(provider);

        var queryContextFactory = queryCompiler?.GetType().GetField("_queryContextFactory", bindingFlags)?.GetValue(queryCompiler);

        var dependencies = typeof(RelationalQueryContextFactory).GetProperty("Dependencies", bindingFlags)?.GetValue(queryContextFactory);

        var queryContextDependencies = typeof(DbContext).Assembly.GetType(typeof(QueryContextDependencies).FullName!);
        var stateManagerProperty = queryContextDependencies?.GetProperty("StateManager", bindingFlags | BindingFlags.Public)?.GetValue(dependencies);
        var stateManager = (IStateManager?)stateManagerProperty;

        return stateManager?.Context;
#pragma warning restore EF1001
    }

    /// <summary>
    /// Splits the leading comments from the main sql query
    /// </summary>
    /// <param name="sqlQuery"></param>
    /// <returns></returns>
    public static (string, string) SplitLeadingCommentsAndMainSqlQuery(string sqlQuery)
    {
        var leadingCommentsBuilder = new StringBuilder();
        var mainSqlQuery = sqlQuery;
        while (!string.IsNullOrWhiteSpace(mainSqlQuery) 
            && !mainSqlQuery.StartsWith("SELECT", StringComparison.OrdinalIgnoreCase))
        {
            if (mainSqlQuery.StartsWith("--"))
            {
                // pull off line comment
                var indexOfNextNewLine = mainSqlQuery.IndexOf(Environment.NewLine, StringComparison.Ordinal);
                if (indexOfNextNewLine > -1)
                {
                    leadingCommentsBuilder.Append(mainSqlQuery[..(indexOfNextNewLine + Environment.NewLine.Length)]);
                    mainSqlQuery = mainSqlQuery[(indexOfNextNewLine + Environment.NewLine.Length)..];
                    continue;
                }
            }

            if (mainSqlQuery.StartsWith("/*"))
            {
                var nextBlockCommentEndIndex = mainSqlQuery.IndexOf("*/", StringComparison.Ordinal);
                if (nextBlockCommentEndIndex > -1)
                {
                    leadingCommentsBuilder.Append(mainSqlQuery.AsSpan(0, nextBlockCommentEndIndex + 2));
                    mainSqlQuery = mainSqlQuery[(nextBlockCommentEndIndex + 2)..];
                    continue;
                }
            }

            var nextNonWhitespaceIndex = Array.FindIndex(mainSqlQuery.ToCharArray(), a => !char.IsWhiteSpace(a));

            if (nextNonWhitespaceIndex > 0)
            {
                leadingCommentsBuilder.Append(mainSqlQuery[..nextNonWhitespaceIndex]);
                mainSqlQuery = mainSqlQuery[nextNonWhitespaceIndex..];
                continue;
            }

            // Fallback... just find the first index of SELECT
            var selectIndex = mainSqlQuery.IndexOf("SELECT", StringComparison.OrdinalIgnoreCase);
            if (selectIndex > 0)
            {
                leadingCommentsBuilder.Append(mainSqlQuery[..selectIndex]);
                mainSqlQuery = mainSqlQuery[selectIndex..];
            }

            break;
        }

        return (leadingCommentsBuilder.ToString(), mainSqlQuery);
    }

    private static void AddSqlParameter(StringBuilder sqlColumns, List<DbParameter> sqlParameters, TableInfo? tableInfo, string? columnName, object? value)
    {
        var paramName = $"@param_{sqlParameters.Count}";

        var valueOrig = value;
        if (columnName != null && (tableInfo?.ConvertibleColumnConverterDict.TryGetValue(columnName, out var valueConverter) ?? false))
        {
            value = valueConverter.ConvertToProvider.Invoke(value);
        }

        // will rely on SqlClientHelper.CorrectParameterType to fix the type before executing
        var sqlParameter = TryCreateRelationalMappingParameter(tableInfo, columnName, paramName, value, valueOrig);
        if (sqlParameter == null)
        {
            sqlParameter = SqlAdaptersMapping.DbServer.QueryBuilder.CreateParameter(paramName, value ?? DBNull.Value);
            var columnType = columnName is null ? null : tableInfo?.ColumnNamesTypesDict[columnName];
            if (value == null
                && (columnType?.Contains(DbType.Binary.ToString(), StringComparison.OrdinalIgnoreCase) ?? false)) //"varbinary(max)".Contains("binary")
            {
                sqlParameter.DbType = DbType.Binary; // fix for ByteArray since implicit conversion nvarchar to varbinary(max) is not allowed
            }
        }

        sqlParameters.Add(sqlParameter);
        sqlColumns.Append($" {paramName}");
    }

    private static readonly MethodInfo? DbContextSetMethodInfo =
        typeof(DbContext).GetMethod(nameof(DbContext.Set), BindingFlags.Public | BindingFlags.Instance, null, Array.Empty<Type>(), null);

    /// <summary>
    /// Regex pattern to get table alias
    /// </summary>
    public static readonly Regex TableAliasPattern = new(@"(?:FROM|JOIN)\s+(\[\S+\]) AS (\[\S+\])", RegexOptions.IgnoreCase | RegexOptions.Compiled);

    /// <summary>
    /// Attempt to create a DbParameter using the 'RelationalTypeMapping.CreateParameter(DbCommand, string, object, bool?)'
    /// call for the specified column name.
    /// </summary>
    public static DbParameter? TryCreateRelationalMappingParameter(TableInfo? tableInfo, string? columnName, string parameterName, object? value, object? valueOrig = null)
    {
        if (columnName == null)
            return null;

        IProperty? propertyInfo = null;
        if (!tableInfo?.ColumnToPropertyDictionary.TryGetValue(columnName, out propertyInfo) ?? false)
            return null;

        var relationalTypeMapping = propertyInfo?.GetRelationalTypeMapping();
        using var dbCommand = SqlAdaptersMapping.DbServer.QueryBuilder.CreateCommand();
        try
        {
            return relationalTypeMapping?.CreateParameter(dbCommand, parameterName, value, propertyInfo?.IsNullable);
        }
        catch (Exception)
        {
            try
            {
                return relationalTypeMapping?.CreateParameter(dbCommand, parameterName, valueOrig, propertyInfo?.IsNullable); // fix for EnumToStringValue or NodaTime.Instant
            }
            catch (Exception) { }
        }

        return null;
    }

    /// <summary>
    /// Tries to create nested body query
    /// </summary>
    /// <param name="createBodyData"></param>
    /// <param name="expression"></param>
    /// <param name="memberAssignment"></param>
    /// <returns></returns>
    public static bool TryCreateUpdateBodyNestedQuery(BatchUpdateCreateBodyData createBodyData, Expression expression, MemberAssignment memberAssignment)
    {
        if (expression is MemberExpression rootMemberExpression && rootMemberExpression.Expression is ParameterExpression)
        {
            // This is a basic assignment expression so don't try checking for a nested query
            return false;
        }

        var rootTypeTableInfo = createBodyData.GetTableInfoForType(createBodyData.RootType);

        var expressionStack = new Stack<ExpressionNode>();
        var visited = new HashSet<Expression>();

        var rootParameterExpressionNodes = new List<ExpressionNode>();
        expressionStack.Push(new ExpressionNode(expression, null));

        // Perform a depth first traversal of the expression tree and see if there is a
        // leaf node in the format rootLambdaParameter.NavigationProperty indicating a nested
        // query is needed
        while (expressionStack.Count > 0)
        {
            var currentExpressionNode = expressionStack.Pop();
            var currentExpression = currentExpressionNode.Expression;
            if (visited.Contains(currentExpression))
            {
                continue;
            }

            visited.Add(currentExpression);
            switch (currentExpression)
            {
                case MemberExpression currentMemberExpression:
                    if (currentMemberExpression.Expression is ParameterExpression finalExpression
                        && finalExpression.Name == createBodyData.RootInstanceParameterName)
                    {
                        if (rootTypeTableInfo?.AllNavigationsDictionary.TryGetValue(currentMemberExpression.Member.Name, out _) ?? false)
                        {
                            rootParameterExpressionNodes.Add(new ExpressionNode(finalExpression, currentExpressionNode));
                            break;
                        }
                    }

                    expressionStack.Push(new ExpressionNode(currentMemberExpression.Expression!, currentExpressionNode));
                    break;

                case MethodCallExpression currentMethodCallExpresion:
                    if (currentMethodCallExpresion.Object != null)
                    {
                        expressionStack.Push(new ExpressionNode(currentMethodCallExpresion.Object, currentExpressionNode));
                    }

                    if (currentMethodCallExpresion.Arguments?.Count > 0)
                    {
                        foreach (var argumentExpression in currentMethodCallExpresion.Arguments)
                        {
                            expressionStack.Push(new ExpressionNode(argumentExpression, currentExpressionNode));
                        }
                    }
                    break;

                case LambdaExpression currentLambdaExpression:
                    expressionStack.Push(new ExpressionNode(currentLambdaExpression.Body, currentExpressionNode));
                    break;

                case UnaryExpression currentUnaryExpression:
                    expressionStack.Push(new ExpressionNode(currentUnaryExpression.Operand, currentExpressionNode));
                    break;

                case BinaryExpression currentBinaryExpression:
                    expressionStack.Push(new ExpressionNode(currentBinaryExpression.Left, currentExpressionNode));
                    expressionStack.Push(new ExpressionNode(currentBinaryExpression.Right, currentExpressionNode));
                    break;

                case ConditionalExpression currentConditionalExpression:
                    expressionStack.Push(new ExpressionNode(currentConditionalExpression.Test, currentExpressionNode));
                    expressionStack.Push(new ExpressionNode(currentConditionalExpression.IfTrue, currentExpressionNode));
                    expressionStack.Push(new ExpressionNode(currentConditionalExpression.IfFalse, currentExpressionNode));
                    break;

                default:
                    break;
            }
        }

        if (rootParameterExpressionNodes.Count < 1)
        {
            return false;
        }

        if (memberAssignment.Member is not PropertyInfo memberPropertyInfo)
        {
            return false;
        }

        var originalParameterNode = rootParameterExpressionNodes.FirstOrDefault();
        var firstNavigationNode = originalParameterNode?.Parent;
        var firstMemberExpression = (MemberExpression?)firstNavigationNode?.Expression;
        var firstNavigation = firstMemberExpression?.Member.Name is null ? null : rootTypeTableInfo?.AllNavigationsDictionary[firstMemberExpression?.Member.Name!];
        var isFirstNavigationACollectionType = firstNavigation?.IsCollection;

        var firstNavigationTargetType = firstNavigation?.TargetEntityType;
        var firstNavigationType = firstNavigationTargetType?.ClrType;
        var firstNavigationTableName = firstNavigationTargetType?.GetTableName();

        IQueryable? innerQueryable;
        if (isFirstNavigationACollectionType == true)
        {
            var dbSetGenericMethod = DbContextSetMethodInfo?.MakeGenericMethod(createBodyData.RootType);
            var dbSetQueryable = (IQueryable?)dbSetGenericMethod?.Invoke(createBodyData.DbContext, null);

            var rootParameter = originalParameterNode?.Expression as ParameterExpression;
            innerQueryable = dbSetQueryable?.Provider.CreateQuery(Expression.Call(
                null,
                QueryableMethods.Select.MakeGenericMethod(createBodyData.RootType, memberPropertyInfo.PropertyType),
                dbSetQueryable.Expression,
                Expression.Lambda(expression, rootParameter!)
            ));
        }
        else
        {
            var dbSetGenericMethod = firstNavigationType is null ? null : DbContextSetMethodInfo?.MakeGenericMethod(firstNavigationType);
            var dbSetQueryable = (IQueryable?)dbSetGenericMethod?.Invoke(createBodyData.DbContext, null);

            var rootParamterName = $"x{firstMemberExpression?.Member.Name}";
            var rootParameter = firstNavigationType is null
                ? throw new ArgumentException("Unable to create root paramater if Navigation type is null")
                : Expression.Parameter(firstNavigationType, rootParamterName);

            Expression lambdaBody = rootParameter;
            var previousNode = firstNavigationNode;
            var currentNode = previousNode?.Parent;

            while (currentNode != null)
            {
                var wasNodeHandled = false;
                switch (currentNode.Expression)
                {
                    case MemberExpression currentMemberExpression:
                        lambdaBody = Expression.MakeMemberAccess(lambdaBody, currentMemberExpression.Member);
                        wasNodeHandled = true;
                        break;

                    case MethodCallExpression currentMethodCallExpression:
                        if (currentMethodCallExpression.Object == previousNode?.Expression)
                        {
                            lambdaBody = Expression.Call(lambdaBody, currentMethodCallExpression.Method, currentMethodCallExpression.Arguments);
                            wasNodeHandled = true;
                        }
                        else if (currentMethodCallExpression.Arguments != null)
                        {
                            var didFindArgumentToSwap = false;
                            var newArguments = new List<Expression>();
                            foreach (var nextArgument in currentMethodCallExpression.Arguments)
                            {
                                if (nextArgument == previousNode?.Expression)
                                {
                                    newArguments.Add(lambdaBody);
                                    didFindArgumentToSwap = true;
                                    continue;
                                }

                                newArguments.Add(nextArgument);
                            }

                            if (didFindArgumentToSwap)
                            {
                                lambdaBody = Expression.Call(currentMethodCallExpression.Object, currentMethodCallExpression.Method, newArguments);
                                wasNodeHandled = true;
                            }
                        }
                        break;

                    case UnaryExpression currentUnaryExpression:
                        if (currentUnaryExpression.Operand == previousNode?.Expression)
                        {
                            lambdaBody = Expression.MakeUnary(currentUnaryExpression.NodeType, lambdaBody, currentUnaryExpression.Type);
                            wasNodeHandled = true;
                        }
                        break;

                    case BinaryExpression currentBinaryExpression:
                        if (currentBinaryExpression.Left == previousNode?.Expression)
                        {
                            lambdaBody = Expression.MakeBinary(currentBinaryExpression.NodeType, lambdaBody, currentBinaryExpression.Right);
                            wasNodeHandled = true;
                        }
                        else if (currentBinaryExpression.Right == previousNode?.Expression)
                        {
                            lambdaBody = Expression.MakeBinary(currentBinaryExpression.NodeType, currentBinaryExpression.Left, lambdaBody);
                            wasNodeHandled = true;
                        }
                        break;

                    case LambdaExpression currentLambdaExpression:
                        if (currentLambdaExpression.Body == previousNode?.Expression)
                        {
                            lambdaBody = Expression.Lambda(lambdaBody, currentLambdaExpression.Parameters);
                            wasNodeHandled = true;
                        }
                        break;

                    case ConditionalExpression currentConditionalExpression:
                        if (currentConditionalExpression.Test == previousNode?.Expression)
                        {
                            lambdaBody = Expression.Condition(lambdaBody, currentConditionalExpression.IfTrue, currentConditionalExpression.IfFalse, currentConditionalExpression.Type);
                            wasNodeHandled = true;
                        }
                        else if (currentConditionalExpression.IfTrue == previousNode?.Expression)
                        {
                            lambdaBody = Expression.Condition(currentConditionalExpression.Test, lambdaBody, currentConditionalExpression.IfFalse, currentConditionalExpression.Type);
                            wasNodeHandled = true;
                        }
                        else if (currentConditionalExpression.IfFalse == previousNode?.Expression)
                        {
                            lambdaBody = Expression.Condition(currentConditionalExpression.Test, currentConditionalExpression.IfTrue, lambdaBody, currentConditionalExpression.Type);
                            wasNodeHandled = true;
                        }
                        break;

                    default:
                        break;
                }

                if (!wasNodeHandled)
                {
                    return false;
                }

                previousNode = currentNode;
                currentNode = currentNode.Parent;
            }

            innerQueryable = dbSetQueryable?.Provider.CreateQuery(Expression.Call(
                null,
                QueryableMethods.Select.MakeGenericMethod(firstNavigationType, memberPropertyInfo.PropertyType),
                dbSetQueryable.Expression,
                Expression.Lambda(lambdaBody, rootParameter)
            ));
        }

        if (innerQueryable is null)
        {
            throw new ArgumentException("InnerQuerable is null");
        }

        var (innerSql, innerSqlParameters) = innerQueryable.ToParametrizedSql();

        innerSql = innerSql.Trim();

        string? firstNavigationAlias = null;
        var rootTableNameWithBrackets = $"[{rootTypeTableInfo?.TableName}]";
        var rootTableAliasWithBrackets = $"[{createBodyData.TableAlias}]";
        var firstNavigationTableNameWithBrackets = $"[{firstNavigationTableName}]";
        foreach (Match match in TableAliasPattern.Matches(innerSql))
        {
            var tableName = match.Groups[1].Value;
            var originalAlias = match.Groups[2].Value;

            if ((isFirstNavigationACollectionType ?? false)
                && tableName.Equals(rootTableNameWithBrackets, StringComparison.OrdinalIgnoreCase)
                && originalAlias.Equals(rootTableAliasWithBrackets, StringComparison.OrdinalIgnoreCase))
            {
                // Don't rename this alias, and cut off the unnecessary FROM clause
                innerSql = innerSql[..match.Index];
                continue;
            }

            if (!createBodyData.TableAliasesInUse.Contains(originalAlias))
            {
                if (tableName.Equals(firstNavigationTableNameWithBrackets, StringComparison.OrdinalIgnoreCase))
                {
                    firstNavigationAlias = originalAlias;
                }

                createBodyData.TableAliasesInUse.Add(originalAlias);
                continue;
            }

            var aliasIndex = -1;
            var aliasPrefix = originalAlias[0..^1];
            string newAlias;
            do
            {
                ++aliasIndex;
                newAlias = $"{aliasPrefix}{aliasIndex}]";
            }
            while (createBodyData.TableAliasesInUse.Contains(newAlias));

            createBodyData.TableAliasesInUse.Add(newAlias);
            innerSql = innerSql.Replace(originalAlias, newAlias);

            if (tableName.Equals(firstNavigationTableNameWithBrackets, StringComparison.OrdinalIgnoreCase))
            {
                firstNavigationAlias = newAlias;
            }
        }

        if (isFirstNavigationACollectionType ?? false)
        {
            innerSql = innerSql[6..].Trim();
            if (innerSql.StartsWith("("))
            {
                createBodyData.UpdateColumnsSql.Append(' ').Append(innerSql);
            }
            else
            {
                createBodyData.UpdateColumnsSql.Append(" (").Append(innerSql).Append(')');
            }

            createBodyData.SqlParameters.AddRange(innerSqlParameters);
            return true;
        }

        var whereClauseCondition = new StringBuilder("WHERE ");
        var dependencyKeyProperties = firstNavigation?.ForeignKey.Properties;
        var principalKeyProperties = firstNavigation?.ForeignKey.PrincipalKey.Properties;
        var navigationColumnFastLookup = createBodyData?.GetTableInfoForType(firstNavigationType!)?.PropertyColumnNamesDict;
        var columnNameValueDict = rootTypeTableInfo?.PropertyColumnNamesDict;
        var rootTableAlias = createBodyData?.TableAlias;
        if (firstNavigation?.IsOnDependent ?? false)
        {
            for (int keyIndex = 0; keyIndex < dependencyKeyProperties?.Count; ++keyIndex)
            {
                if (keyIndex > 0)
                {
                    whereClauseCondition.Append(" AND ");
                }

                var dependencyColumnName = navigationColumnFastLookup![dependencyKeyProperties[keyIndex].Name];
                var principalColumnName = columnNameValueDict![principalKeyProperties![keyIndex].Name];
                whereClauseCondition.Append(firstNavigationAlias).Append(".[").Append(principalColumnName).Append("] = [")
                    .Append(rootTableAlias).Append("].[").Append(dependencyColumnName).Append(']');
            }
        }
        else
        {
            for (int keyIndex = 0; keyIndex < dependencyKeyProperties?.Count; ++keyIndex)
            {
                if (keyIndex > 0)
                {
                    whereClauseCondition.Append(" AND ");
                }

                var dependencyColumnName = navigationColumnFastLookup![dependencyKeyProperties[keyIndex].Name];
                var principalColumnName = columnNameValueDict![principalKeyProperties![keyIndex].Name];
                whereClauseCondition.Append(firstNavigationAlias).Append(".[").Append(dependencyColumnName).Append("] = [")
                    .Append(rootTableAlias).Append("].[").Append(principalColumnName).Append(']');
            }
        }

        var whereClauseIndex = innerSql.LastIndexOf("WHERE ", StringComparison.OrdinalIgnoreCase);
        if (whereClauseIndex > -1)
        {
            innerSql = innerSql[..whereClauseIndex] + whereClauseCondition.ToString() + "AND " + innerSql[(whereClauseIndex + 5)..];
        }
        else
        {
            var orderByIndex = innerSql.LastIndexOf("ORDER BY ", StringComparison.OrdinalIgnoreCase);
            if (orderByIndex > -1)
            {
                innerSql = innerSql[..orderByIndex] + '\n' + whereClauseCondition.ToString() + '\n' + innerSql[orderByIndex..];
            }
            else
            {
                innerSql = innerSql + '\n' + whereClauseCondition.ToString();
            }

        }

        createBodyData?.UpdateColumnsSql.Append(" (\n    ").Append(innerSql.Replace("\n", "\n    ")).Append(')');
        createBodyData?.SqlParameters.AddRange(innerSqlParameters);

        return true;
    }

    /// <summary>
    /// ExpressionNode
    /// </summary>
    public class ExpressionNode
    {
#pragma warning disable CS1591 // No XML comment required here. Used internally only
        public ExpressionNode (Expression expression, ExpressionNode? parent)
        {
            Expression = expression;
            Parent = parent;
        }

        public Expression Expression { get; }
        public ExpressionNode? Parent { get; }
#pragma warning restore CS1591 // No XML comment required here. Used internally only
    }
}
