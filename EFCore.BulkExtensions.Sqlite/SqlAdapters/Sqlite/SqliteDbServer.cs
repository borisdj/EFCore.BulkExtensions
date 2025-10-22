using Microsoft.EntityFrameworkCore.Infrastructure;
using System;

namespace EFCore.BulkExtensions.SqlAdapters.Sqlite;

/// <inheritdoc/>
public class SqliteDbServer : IDbServer
{
    SqlType IDbServer.Type => SqlType.Sqlite;

    SqliteAdapter _adapter = new ();
    ISqlOperationsAdapter IDbServer.Adapter => _adapter;

    SqliteDialect _dialect = new();
    IQueryBuilderSpecialization IDbServer.Dialect => _dialect;

    SqlAdapters.SqlQueryBuilder _queryBuilder = new SqliteQueryBuilder();
    /// <inheritdoc/>
    public SqlQueryBuilder QueryBuilder => _queryBuilder;

    string IDbServer.ValueGenerationStrategy => String.Empty;

    bool IDbServer.PropertyHasIdentity(IAnnotation annotation) => false;
}
