using Microsoft.Data.SqlClient;
using System;

namespace EFCore.BulkExtensions.SqlAdapters.SqlServer;

/// <summary>
/// Contains a compilation of SQL queries used in EFCore.
/// </summary>
public class SqlServerQueryBuilder : SqlQueryBuilder
{
    /// <inheritdoc/>
    public override object CreateParameter(SqlParameter sqlParameter)
    {
        throw new NotImplementedException();
    }

    /// <inheritdoc/>
    public override object Dbtype()
    {
        throw new NotImplementedException();
    }

    /// <inheritdoc/>
    public override string RestructureForBatch(string sql, bool isDelete = false)
    {
        throw new NotImplementedException();
    }

    /// <inheritdoc/>
    public override void SetDbTypeParam(object npgsqlParameter, object dbType)
    {
        throw new NotImplementedException();
    }
}
