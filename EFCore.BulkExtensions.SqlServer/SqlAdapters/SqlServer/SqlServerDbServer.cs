﻿using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;

namespace EFCore.BulkExtensions.SqlAdapters.SqlServer;

/// <inheritdoc/>
public class SqlServerDbServer : IDbServer
{
    SqlType IDbServer.Type => SqlType.SqlServer;

    SqlServerAdapter _adapter = new ();
    ISqlOperationsAdapter IDbServer.Adapter => _adapter;

    SqlServerDialect _dialect = new();
    IQueryBuilderSpecialization IDbServer.Dialect => _dialect;

    SqlAdapters.SqlQueryBuilder _queryBuilder = new SqlServerQueryBuilder();
    /// <inheritdoc/>
    public SqlQueryBuilder QueryBuilder => _queryBuilder;

    string IDbServer.ValueGenerationStrategy => nameof(SqlServerValueGenerationStrategy);

    bool IDbServer.PropertyHasIdentity(IAnnotation annotation) => (SqlServerValueGenerationStrategy?)annotation.Value == SqlServerValueGenerationStrategy.IdentityColumn;
}
