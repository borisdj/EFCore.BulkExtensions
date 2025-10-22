﻿using Microsoft.EntityFrameworkCore.Infrastructure;
using Npgsql.EntityFrameworkCore.PostgreSQL.Metadata.Internal;

namespace EFCore.BulkExtensions.SqlAdapters.PostgreSql;

/// <inheritdoc/>
public class PostgreSqlDbServer : IDbServer
{
    SqlType IDbServer.Type => SqlType.PostgreSql;

    PostgreSqlAdapter _adapter = new ();
    ISqlOperationsAdapter IDbServer.Adapter => _adapter;

    PostgreSqlDialect _dialect = new();
    IQueryBuilderSpecialization IDbServer.Dialect => _dialect;

    SqlAdapters.SqlQueryBuilder _queryBuilder = new PostgreSqlQueryBuilder();
    /// <inheritdoc/>
    public SqlQueryBuilder QueryBuilder => _queryBuilder;

#pragma warning disable EF1001
    string IDbServer.ValueGenerationStrategy => NpgsqlAnnotationNames.ValueGenerationStrategy;
#pragma warning restore EF1001

    bool IDbServer.PropertyHasIdentity(IAnnotation annotation) => (Npgsql.EntityFrameworkCore.PostgreSQL.Metadata.NpgsqlValueGenerationStrategy?)annotation.Value == Npgsql.EntityFrameworkCore.PostgreSQL.Metadata.NpgsqlValueGenerationStrategy.IdentityByDefaultColumn;
}
