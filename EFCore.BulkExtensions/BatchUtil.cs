using EFCore.BulkExtensions.SqlAdapters;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.ChangeTracking.Internal;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.Internal;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Text.RegularExpressions;

namespace EFCore.BulkExtensions
{
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
        public static (string, List<object>) GetSqlDelete(IQueryable query, DbContext context)
        {
            var (sql, tableAlias, _, topStatement, leadingComments, innerParameters) = GetBatchSql(query, context, isUpdate: false);

            innerParameters = ReloadSqlParameters(context, innerParameters.ToList()); // Sqlite requires SqliteParameters
            tableAlias = SqlAdaptersMapping.GetDatabaseType(context) == DbServer.SqlServer ? $"[{tableAlias}]" : tableAlias;

            var resultQuery = $"{leadingComments}DELETE {topStatement}{tableAlias}{sql}";
            return (resultQuery, new List<object>(innerParameters));
        }

        // SELECT [a].[Column1], [a].[Column2], .../r/n
        // FROM [Table] AS [a]/r/n
        // WHERE [a].[Column] = FilterValue
        // --
        // UPDATE [a] SET [UpdateColumns] = N'updateValues'
        // FROM [Table] AS [a]
        // WHERE [a].[Columns] = FilterValues
        public static (string, List<object>) GetSqlUpdate(IQueryable query, DbContext context, object updateValues, List<string> updateColumns)
        {
            var (sql, tableAlias, tableAliasSufixAs, topStatement, leadingComments, innerParameters) = GetBatchSql(query, context, isUpdate: true);
            var sqlParameters = new List<object>(innerParameters);

            string sqlSET = GetSqlSetSegment(context, updateValues.GetType(), updateValues, updateColumns, sqlParameters);

            sqlParameters = ReloadSqlParameters(context, sqlParameters); // Sqlite requires SqliteParameters

            var resultQuery = $"{leadingComments}UPDATE {topStatement}{tableAlias}{tableAliasSufixAs} {sqlSET}{sql}";
            return (resultQuery, sqlParameters);
        }

        /// <summary>
        /// get Update Sql
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="query"></param>
        /// <param name="expression"></param>
        /// <returns></returns>
        public static (string, List<object>) GetSqlUpdate<T>(IQueryable<T> query, DbContext context, Expression<Func<T, T>> expression) where T : class
        {
            return GetSqlUpdate<T>(query, context, typeof(T), expression);
        }
        public static (string, List<object>) GetSqlUpdate(IQueryable query, DbContext context, Type type, Expression<Func<object, object>> expression)
        {
            return GetSqlUpdate<object>(query, context, type, expression);
        }

        private static (string, List<object>) GetSqlUpdate<T>(IQueryable query, DbContext context, Type type, Expression<Func<T, T>> expression) where T : class
        {
            (string sql, string tableAlias, string tableAliasSufixAs, string topStatement, string leadingComments, IEnumerable<object> innerParameters) = GetBatchSql(query, context, isUpdate: true);

            var createUpdateBodyData = new BatchUpdateCreateBodyData(sql, context, innerParameters, query, type, tableAlias, expression);

            CreateUpdateBody(createUpdateBodyData, expression.Body);

            var sqlParameters = ReloadSqlParameters(context, createUpdateBodyData.SqlParameters); // Sqlite requires SqliteParameters
            var sqlColumns = (createUpdateBodyData.DatabaseType == DbServer.SqlServer) 
                ? createUpdateBodyData.UpdateColumnsSql
                : createUpdateBodyData.UpdateColumnsSql.Replace($"[{tableAlias}].", "");

            var resultQuery = $"{leadingComments}UPDATE {topStatement}{tableAlias}{tableAliasSufixAs} SET {sqlColumns} {sql}";
            return (resultQuery, sqlParameters);
        }

        public static List<object> ReloadSqlParameters(DbContext context, List<object> sqlParameters)
        {
            return SqlAdaptersMapping.GetAdapterDialect(context).ReloadSqlParameters(context,sqlParameters);
        }

        public static (string, string, string, string, string, IEnumerable<object>) GetBatchSql(IQueryable query, DbContext context, bool isUpdate)
        {
            var sqlQueryBuilder = SqlAdaptersMapping.GetAdapterDialect(context);
            var (fullSqlQuery, innerParameters) = query.ToParametrizedSql();

            var (leadingComments, sqlQuery) = SplitLeadingCommentsAndMainSqlQuery(fullSqlQuery);

            string tableAlias;
            string tableAliasSufixAs = string.Empty;
            string topStatement;

            (tableAlias, topStatement) = sqlQueryBuilder.GetBatchSqlReformatTableAliasAndTopStatement(sqlQuery);

            int indexFrom = sqlQuery.IndexOf(Environment.NewLine, StringComparison.Ordinal);
            string sql = sqlQuery.Substring(indexFrom, sqlQuery.Length - indexFrom);
            sql = sql.Contains("{") ? sql.Replace("{", "{{") : sql; // Curly brackets have to be escaped:
            sql = sql.Contains("}") ? sql.Replace("}", "}}") : sql; // https://github.com/aspnet/EntityFrameworkCore/issues/8820

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

        public static string GetSqlSetSegment<T>(DbContext context, T updateValues, List<string> updateColumns, List<object> parameters) where T : class, new()
        {
            var tableInfo = TableInfo.CreateInstance<T>(context, new List<T>(), OperationType.Read, new BulkConfig());
            return GetSqlSetSegment(context, tableInfo, typeof(T), updateValues, new T(), updateColumns, parameters);
        }

        public static string GetSqlSetSegment(DbContext context, Type updateValuesType, object updateValues, List<string> updateColumns, List<object> parameters)
        {
            var tableInfo = TableInfo.CreateInstance(context, updateValuesType, new List<object>(), OperationType.Read, new BulkConfig());
            return GetSqlSetSegment(context, tableInfo, updateValuesType, updateValues, Activator.CreateInstance(updateValuesType), updateColumns, parameters);
        }

        private static string GetSqlSetSegment(DbContext context, TableInfo tableInfo, Type updateValuesType, object updateValues, object defaultValues, List<string> updateColumns, List<object> parameters)
        {
            string sql = string.Empty;
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

                    if (tableInfo.ConvertibleProperties.ContainsKey(columnName))
                    {
                        propertyUpdateValue = tableInfo.ConvertibleProperties[columnName].ConvertToProvider.Invoke(propertyUpdateValue);
                    }

                    bool isDifferentFromDefault = propertyUpdateValue != null && propertyUpdateValue?.ToString() != propertyDefaultValue?.ToString();
                    if (isDifferentFromDefault || (updateColumns != null && updateColumns.Contains(propertyName)))
                    {
                        sql += $"[{columnName}] = @{columnName}, ";
                        propertyUpdateValue = propertyUpdateValue ?? DBNull.Value;
                        var p = SqlClientHelper.CreateParameter(context.Database.GetDbConnection());
                        p.ParameterName = $"@{columnName}";
                        p.Value = propertyUpdateValue;
                        parameters.Add(p);
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
        /// <param name="tableAlias"></param>
        /// <param name="expression"></param>
        /// <param name="sqlColumns"></param>
        /// <param name="sqlParameters"></param>
        public static void CreateUpdateBody(BatchUpdateCreateBodyData createBodyData, Expression expression)
        {
            var rootTypeTableInfo = createBodyData.GetTableInfoForType(createBodyData.RootType);
            var columnNameValueDict = rootTypeTableInfo.PropertyColumnNamesDict;
            var tableAlias = createBodyData.TableAlias;
            var sqlColumns = createBodyData.UpdateColumnsSql;
            var sqlParameters = createBodyData.SqlParameters;

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

                        if (!TryCreateUpdateBodyNestedQuery(createBodyData, assignment.Expression, assignment))
                        {
                            CreateUpdateBody(createBodyData, assignment.Expression);
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
                if (columnNameValueDict.TryGetValue(memberExpression.Member.Name, out string value))
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
                var constantParamName = $"param_{sqlParameters.Count}";
                // will rely on SqlClientHelper.CorrectParameterType to fix the type before executing
                sqlParameters.Add(new Microsoft.Data.SqlClient.SqlParameter(constantParamName, constantExpression.Value ?? DBNull.Value));
                sqlColumns.Append($" @{constantParamName}");
                return;
            }

            if (expression is UnaryExpression unaryExpression)
            {
                switch (unaryExpression.NodeType)
                {
                    case ExpressionType.Convert:
                        CreateUpdateBody(createBodyData, unaryExpression.Operand);
                        break;
                    case ExpressionType.Not:
                        sqlColumns.Append(" ~");//this way only for SQL Server 
                        CreateUpdateBody(createBodyData, unaryExpression.Operand);
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
                        CreateUpdateBody(createBodyData, binaryExpression.Left);
                        var sqlOperator = SqlAdaptersMapping.GetAdapterDialect(createBodyData.DatabaseType)
                            .GetBinaryExpressionAddOperation(binaryExpression);
                        sqlColumns.Append(" " + sqlOperator);
                        CreateUpdateBody(createBodyData, binaryExpression.Right);
                        break;

                    case ExpressionType.Divide:
                        CreateUpdateBody(createBodyData, binaryExpression.Left);
                        sqlColumns.Append(" /");
                        CreateUpdateBody(createBodyData, binaryExpression.Right);
                        break;

                    case ExpressionType.Multiply:
                        CreateUpdateBody(createBodyData, binaryExpression.Left);
                        sqlColumns.Append(" *");
                        CreateUpdateBody(createBodyData, binaryExpression.Right);
                        break;

                    case ExpressionType.Subtract:
                        CreateUpdateBody(createBodyData, binaryExpression.Left);
                        sqlColumns.Append(" -");
                        CreateUpdateBody(createBodyData, binaryExpression.Right);
                        break;

                    case ExpressionType.And:
                        CreateUpdateBody(createBodyData, binaryExpression.Left);
                        sqlColumns.Append(" &");
                        CreateUpdateBody(createBodyData, binaryExpression.Right);
                        break;

                    case ExpressionType.Or:
                        CreateUpdateBody(createBodyData, binaryExpression.Left);
                        sqlColumns.Append(" |");
                        CreateUpdateBody(createBodyData, binaryExpression.Right);
                        break;

                    case ExpressionType.ExclusiveOr:
                        CreateUpdateBody(createBodyData, binaryExpression.Left);
                        sqlColumns.Append(" ^");
                        CreateUpdateBody(createBodyData, binaryExpression.Right);
                        break;

                    case ExpressionType.Coalesce:
                        sqlColumns.Append("COALESCE(");
                        CreateUpdateBody(createBodyData, binaryExpression.Left);
                        sqlColumns.Append(", ");
                        CreateUpdateBody(createBodyData, binaryExpression.Right);
                        break;

                    default: 
                        throw new NotSupportedException($"{nameof(BatchUtil)}.{nameof(CreateUpdateBody)}(..) is not supported for a binary exression of type {binaryExpression.NodeType}");
                }

                return;
            }

            // For any other case fallback on compiling and executing the expression
            var compiledExpressionValue = Expression.Lambda(expression).Compile().DynamicInvoke();
            var parmName = $"param_{sqlParameters.Count}";
            // will rely on SqlClientHelper.CorrectParameterType to fix the type before executing
            sqlParameters.Add(new Microsoft.Data.SqlClient.SqlParameter(parmName, compiledExpressionValue ?? DBNull.Value));
            sqlColumns.Append($" @{parmName}");
        }

        public static DbContext GetDbContext(IQueryable query)
        {
            const BindingFlags bindingFlags = BindingFlags.NonPublic | BindingFlags.Instance;
            var queryCompiler = typeof(EntityQueryProvider).GetField("_queryCompiler", bindingFlags).GetValue(query.Provider);
            var queryContextFactory = queryCompiler.GetType().GetField("_queryContextFactory", bindingFlags).GetValue(queryCompiler);

            var dependencies = typeof(RelationalQueryContextFactory).GetField("_dependencies", bindingFlags).GetValue(queryContextFactory);
            var queryContextDependencies = typeof(DbContext).Assembly.GetType(typeof(QueryContextDependencies).FullName);
            var stateManagerProperty = queryContextDependencies.GetProperty("StateManager", bindingFlags | BindingFlags.Public).GetValue(dependencies);
            var stateManager = (IStateManager)stateManagerProperty;

#pragma warning disable EF1001 // Internal EF Core API usage.
            return stateManager.Context;
#pragma warning restore EF1001 // Internal EF Core API usage.
        }

      

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
                        leadingCommentsBuilder.Append(mainSqlQuery.Substring(0, indexOfNextNewLine + Environment.NewLine.Length));
                        mainSqlQuery = mainSqlQuery.Substring(indexOfNextNewLine + Environment.NewLine.Length);
                        continue;
                    }
                }

                if (mainSqlQuery.StartsWith("/*"))
                {
                    var nextBlockCommentEndIndex = mainSqlQuery.IndexOf("*/", StringComparison.Ordinal);
                    if (nextBlockCommentEndIndex > -1)
                    {
                        leadingCommentsBuilder.Append(mainSqlQuery.Substring(0, nextBlockCommentEndIndex + 2));
                        mainSqlQuery = mainSqlQuery.Substring(nextBlockCommentEndIndex + 2);
                        continue;
                    }
                }

                var nextNonWhitespaceIndex = Array.FindIndex(mainSqlQuery.ToCharArray(), x => !char.IsWhiteSpace(x));

                if (nextNonWhitespaceIndex > 0)
                {
                    leadingCommentsBuilder.Append(mainSqlQuery.Substring(0, nextNonWhitespaceIndex));
                    mainSqlQuery = mainSqlQuery.Substring(nextNonWhitespaceIndex);
                    continue;
                }

                // Fallback... just find the first index of SELECT
                var selectIndex = mainSqlQuery.IndexOf("SELECT", StringComparison.OrdinalIgnoreCase);
                if (selectIndex > 0)
                {
                    leadingCommentsBuilder.Append(mainSqlQuery.Substring(0, selectIndex));
                    mainSqlQuery = mainSqlQuery.Substring(selectIndex);
                }

                break;
            }

            return (leadingCommentsBuilder.ToString(), mainSqlQuery);
        }

        private static readonly MethodInfo DbContextSetMethodInfo = typeof(DbContext)
            .GetMethod(nameof(DbContext.Set), BindingFlags.Public | BindingFlags.Instance, null, Array.Empty<Type>(), null);

        public static readonly Regex TableAliasPattern = new Regex(@"(?:FROM|JOIN)\s+(\[\S+\]) AS (\[\S+\])", RegexOptions.IgnoreCase | RegexOptions.Compiled);

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

            // Perform a depth first traversal of the the expression tree and see if there is a
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
                            if (rootTypeTableInfo.AllNavigationsDictionary.TryGetValue(currentMemberExpression.Member.Name, out _))
                            {
                                rootParameterExpressionNodes.Add(new ExpressionNode(finalExpression, currentExpressionNode));
                                break;
                            }
                        }

                        expressionStack.Push(new ExpressionNode(currentMemberExpression.Expression, currentExpressionNode));
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

            if (!(memberAssignment.Member is PropertyInfo memberPropertyInfo))
            {
                return false;
            }

            var originalParameterNode = rootParameterExpressionNodes.FirstOrDefault();
            var firstNavigationNode = originalParameterNode.Parent;
            var firstMemberExpression = (MemberExpression)firstNavigationNode.Expression;
            var firstNavigation = rootTypeTableInfo.AllNavigationsDictionary[firstMemberExpression.Member.Name];
            var isFirstNavigationACollectionType = firstNavigation.IsCollection();

            var firstNavigationTargetType = firstNavigation.GetTargetType();
            var firstNavigationType = firstNavigationTargetType.ClrType;
            var firstNavigationTableName = firstNavigationTargetType.GetTableName();

            IQueryable innerQueryable;
            if (isFirstNavigationACollectionType)
            {
                var dbSetGenericMethod = DbContextSetMethodInfo.MakeGenericMethod(createBodyData.RootType);
                var dbSetQueryable = (IQueryable)dbSetGenericMethod.Invoke(createBodyData.DbContext, null);

                var rootParameter = originalParameterNode.Expression as ParameterExpression;
                innerQueryable = dbSetQueryable.Provider.CreateQuery(Expression.Call(
                    null,
                    QueryableMethods.Select.MakeGenericMethod(createBodyData.RootType, memberPropertyInfo.PropertyType),
                    dbSetQueryable.Expression,
                    Expression.Lambda(expression, rootParameter)
                ));
            }
            else
            {
                var dbSetGenericMethod = DbContextSetMethodInfo.MakeGenericMethod(firstNavigationType);
                var dbSetQueryable = (IQueryable)dbSetGenericMethod.Invoke(createBodyData.DbContext, null);

                var rootParamterName = $"x{firstMemberExpression.Member.Name}";
                var rootParameter = Expression.Parameter(firstNavigationType, rootParamterName);

                Expression lambdaBody = rootParameter;
                var previousNode = firstNavigationNode;
                var currentNode = previousNode.Parent;
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
                            if (currentMethodCallExpression.Object == previousNode.Expression)
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
                                    if (nextArgument == previousNode.Expression)
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
                            if (currentUnaryExpression.Operand == previousNode.Expression)
                            {
                                lambdaBody = Expression.MakeUnary(currentUnaryExpression.NodeType, lambdaBody, currentUnaryExpression.Type);
                                wasNodeHandled = true;
                            }
                            break;

                        case BinaryExpression currentBinaryExpression:
                            if (currentBinaryExpression.Left == previousNode.Expression)
                            {
                                lambdaBody = Expression.MakeBinary(currentBinaryExpression.NodeType, lambdaBody, currentBinaryExpression.Right);
                                wasNodeHandled = true;
                            }
                            else if (currentBinaryExpression.Right == previousNode.Expression)
                            {
                                lambdaBody = Expression.MakeBinary(currentBinaryExpression.NodeType, currentBinaryExpression.Left, lambdaBody);
                                wasNodeHandled = true;
                            }
                            break;

                        case LambdaExpression currentLambdaExpression:
                            if (currentLambdaExpression.Body == previousNode.Expression)
                            {
                                lambdaBody = Expression.Lambda(lambdaBody, currentLambdaExpression.Parameters);
                                wasNodeHandled = true;
                            }
                            break;

                        case ConditionalExpression currentConditionalExpression:
                            if (currentConditionalExpression.Test == previousNode.Expression)
                            {
                                lambdaBody = Expression.Condition(lambdaBody, currentConditionalExpression.IfTrue, currentConditionalExpression.IfFalse, currentConditionalExpression.Type);
                                wasNodeHandled = true;
                            }
                            else if (currentConditionalExpression.IfTrue == previousNode.Expression)
                            {
                                lambdaBody = Expression.Condition(currentConditionalExpression.Test, lambdaBody, currentConditionalExpression.IfFalse, currentConditionalExpression.Type);
                                wasNodeHandled = true;
                            }
                            else if (currentConditionalExpression.IfFalse == previousNode.Expression)
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

                innerQueryable = dbSetQueryable.Provider.CreateQuery(Expression.Call(
                    null,
                    QueryableMethods.Select.MakeGenericMethod(firstNavigationType, memberPropertyInfo.PropertyType),
                    dbSetQueryable.Expression,
                    Expression.Lambda(lambdaBody, rootParameter)
                ));
            }

            var (innerSql, innerSqlParameters) = innerQueryable.ToParametrizedSql();
            innerSql = innerSql.Trim();

            string firstNavigationAlias = null;
            var rootTableNameWithBrackets = $"[{rootTypeTableInfo.TableName}]";
            var rootTableAliasWithBrackets = $"[{createBodyData.TableAlias}]";
            var firstNavigationTableNameWithBrackets = $"[{firstNavigationTableName}]";
            foreach (Match match in TableAliasPattern.Matches(innerSql))
            {
                var tableName = match.Groups[1].Value;
                var originalAlias = match.Groups[2].Value;

                if (isFirstNavigationACollectionType
                    && tableName.Equals(rootTableNameWithBrackets, StringComparison.OrdinalIgnoreCase)
                    && originalAlias.Equals(rootTableAliasWithBrackets, StringComparison.OrdinalIgnoreCase))
                {
                    // Don't rename this alias, and cut off the unnecessary FROM clause
                    innerSql = innerSql.Substring(0, match.Index);
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
                var aliasPrefix = originalAlias.Substring(0, originalAlias.Length - 1);
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

            if (isFirstNavigationACollectionType)
            {
                innerSql = innerSql.Substring(6).Trim();
                
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
            var dependencyKeyProperties = firstNavigation.ForeignKey.Properties;
            var principalKeyProperties = firstNavigation.ForeignKey.PrincipalKey.Properties;
            var navigationColumnFastLookup = createBodyData.GetTableInfoForType(firstNavigationType).PropertyColumnNamesDict;
            var columnNameValueDict = rootTypeTableInfo.PropertyColumnNamesDict;
            var rootTableAlias = createBodyData.TableAlias;
            if (firstNavigation.IsDependentToPrincipal())
            {
                for (int keyIndex = 0; keyIndex < dependencyKeyProperties.Count; ++keyIndex)
                {
                    if (keyIndex > 0)
                    {
                        whereClauseCondition.Append(" AND ");
                    }

                    var dependencyColumnName = navigationColumnFastLookup[dependencyKeyProperties[keyIndex].Name];
                    var principalColumnName = columnNameValueDict[principalKeyProperties[keyIndex].Name];
                    whereClauseCondition.Append(firstNavigationAlias).Append(".[").Append(principalColumnName).Append("] = [")
                        .Append(rootTableAlias).Append("].[").Append(dependencyColumnName).Append(']');
                }
            }
            else
            {
                for (int keyIndex = 0; keyIndex < dependencyKeyProperties.Count; ++keyIndex)
                {
                    if (keyIndex > 0)
                    {
                        whereClauseCondition.Append(" AND ");
                    }

                    var dependencyColumnName = navigationColumnFastLookup[dependencyKeyProperties[keyIndex].Name];
                    var principalColumnName = columnNameValueDict[principalKeyProperties[keyIndex].Name];
                    whereClauseCondition.Append(firstNavigationAlias).Append(".[").Append(dependencyColumnName).Append("] = [")
                        .Append(rootTableAlias).Append("].[").Append(principalColumnName).Append(']');
                }
            }

            var whereClauseIndex = innerSql.LastIndexOf("WHERE ", StringComparison.OrdinalIgnoreCase);
            if (whereClauseIndex > -1)
            {
                innerSql = innerSql.Substring(0, whereClauseIndex) + whereClauseCondition.ToString() + "AND " + innerSql.Substring(whereClauseIndex + 5);
            }
            else
            {
                var orderByIndex = innerSql.LastIndexOf("ORDER BY ", StringComparison.OrdinalIgnoreCase);
                if (orderByIndex > -1)
                {
                    innerSql = innerSql.Substring(0, orderByIndex) + '\n' + whereClauseCondition.ToString() + '\n' + innerSql.Substring(orderByIndex);
                }
                else
                {
                    innerSql = innerSql + '\n' + whereClauseCondition.ToString();
                }

            }

            createBodyData.UpdateColumnsSql.Append(" (\n    ").Append(innerSql.Replace("\n", "\n    ")).Append(')');
            createBodyData.SqlParameters.AddRange(innerSqlParameters);

            return true;
        }

        public class ExpressionNode
        {
            public ExpressionNode (Expression expression, ExpressionNode parent)
            {
                Expression = expression;
                Parent = parent;
            }

            public Expression Expression { get; }
            public ExpressionNode Parent { get; }
        }
    }
}
