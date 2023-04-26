using Microsoft.EntityFrameworkCore.Infrastructure;
using System;
using System.Data.Common;

namespace EFCore.BulkExtensions.SqlAdapters.Sqlite;

/// <inheritdoc/>
public class SqliteDbServer : IDbServer
{
    SqlType IDbServer.Type => SqlType.Sqlite;

    SqliteAdapter _adapter = new ();
    ISqlOperationsAdapter IDbServer.Adapter => _adapter;

    SqliteDialect _dialect = new();
    IQueryBuilderSpecialization IDbServer.Dialect => _dialect;

    /// <inheritdoc/>
    public DbConnection? DbConnection { get; set; }

    /// <inheritdoc/>
    public DbTransaction? DbTransaction { get; set; }

    SqlAdapters.QueryBuilderExtensions _queryBuilder = new SqliteQueryBuilder();
    /// <inheritdoc/>
    public QueryBuilderExtensions QueryBuilder => _queryBuilder;

    string IDbServer.ValueGenerationStrategy => String.Empty;

    bool IDbServer.PropertyHasIdentity(IAnnotation annotation) => false;
}
