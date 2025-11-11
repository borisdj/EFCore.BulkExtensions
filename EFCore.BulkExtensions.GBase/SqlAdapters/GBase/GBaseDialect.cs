using GBS.Data.GBasedbt;
using Microsoft.EntityFrameworkCore;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text.RegularExpressions;

namespace EFCore.BulkExtensions.SqlAdapters.GBase;

/// <inheritdoc/>
public class GBaseDialect : IQueryBuilderSpecialization
{
    /// <inheritdoc/>
    public List<DbParameter> ReloadSqlParameters(DbContext context, List<DbParameter> sqlParameters)
    {
        var parameterNameFromEFCore = "? ";
        var sqlParametersReloaded = new List<DbParameter>();

        // some parameters in sqlParameters are retrieved from GenerateData.EntityFramework.GBase,
        // their parameter name is changed to "? ".
        // these parameters maps to the ? in where condition.
        // since GBase does not support named parameters.
        // they should not be added to the Parameters of command first.
        // so here we change the order of the parameters.
        foreach (var parameter in sqlParameters.Where(o => o.ParameterName != parameterNameFromEFCore))
        {
            sqlParametersReloaded.Add(new GbsParameter(parameter.ParameterName, parameter.Value));
        }

        // here we add the parameters which are retrieved from GenerateData.EntityFramework.GBase.
        // we make the parameter name unique.
        // otherwise "Wrong number of parameters" exception will be thrown out.
        // GBase ado.net provider does not care about the parameter name is unique or not.
        // but the framework cares, it ignores the duplicated parameters and then pass to GBase ado.net.
        int i = 0;
        foreach (var parameter in sqlParameters.Where(o => o.ParameterName == parameterNameFromEFCore))
        {
            parameter.ParameterName += i++;
            sqlParametersReloaded.Add(new GbsParameter(parameter.ParameterName, parameter.Value));
        }
        return sqlParametersReloaded;
    }

    /// <inheritdoc/>
    public string GetBinaryExpressionAddOperation(BinaryExpression binaryExpression)
    {
        return IsStringConcat(binaryExpression) ? "||" : "+";
    }

    /// <inheritdoc/>
    public (string, string) GetBatchSqlReformatTableAliasAndTopStatement(string sqlQuery, SqlType databaseType)
    {
        return (string.Empty, string.Empty);
    }

    /// <inheritdoc/>
    public ExtractedTableAlias GetBatchSqlExtractTableAliasFromQuery(string fullQuery, string tableAlias,
        string tableAliasSuffixAs)
    {
        var result = new ExtractedTableAlias();

        // var sql = "FROM gbasedbt.customer AS c WHERE c.customer_num = 1";
        // match the "gbasedbt.customer" and "c" from a sql statement.
        var match = Regex.Match(fullQuery, @"FROM\s+(?<table>\w+(?:\.\w+)*)(?:\s+AS\s+(?<alias>\w+))?");
        result.TableAlias = match.Groups["table"].Value;

        // Add a space to avoid sql syntax error.
        result.TableAliasSuffixAs = " " + match.Groups["alias"].Value;
        result.Sql = fullQuery[(match.Index + match.Length)..];

        return result;
    }

    internal static bool IsStringConcat(BinaryExpression binaryExpression)
    {
        var methodProperty = binaryExpression.GetType().GetProperty("Method");
        if (methodProperty == null)
        {
            return false;
        }

        var method = methodProperty.GetValue(binaryExpression) as MethodInfo;
        if (method == null)
        {
            return false;
        }

        return method.DeclaringType == typeof(string) && method.Name == nameof(string.Concat);
    }
}
