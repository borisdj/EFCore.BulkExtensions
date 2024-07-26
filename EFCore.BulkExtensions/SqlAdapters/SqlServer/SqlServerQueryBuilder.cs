using System;
using System.Data;
using System.Data.Common;
using Microsoft.Data.SqlClient;

namespace EFCore.BulkExtensions.SqlAdapters.SqlServer;

/// <summary>
/// Contains a compilation of SQL queries used in EFCore.
/// </summary>
public class SqlServerQueryBuilder : SqlQueryBuilder
{
    /// <inheritdoc/>
    public override DbParameter CreateParameter(string parameterName, object? parameterValue = null)
    {
        return new SqlParameter(parameterName, parameterValue);
    }

    /// <inheritdoc/>
    public override DbCommand CreateCommand()
    {
        return new SqlCommand();
    }

    /// <inheritdoc/>
    public override DbType Dbtype()
    {
        throw new NotSupportedException();
    }

    /// <inheritdoc/>
    public override string RestructureForBatch(string sql, bool isDelete = false)
    {
        throw new NotSupportedException();
    }

    /// <inheritdoc/>
    public override void SetDbTypeParam(DbParameter parameter, DbType dbType)
    {
        throw new NotSupportedException();
    }
}
