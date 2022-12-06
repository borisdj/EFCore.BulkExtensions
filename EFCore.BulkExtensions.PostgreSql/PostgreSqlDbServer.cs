using EFCore.BulkExtensions.SqlAdapters;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Npgsql.EntityFrameworkCore.PostgreSQL.Metadata.Internal;
using System.Data.Common;

namespace EFCore.BulkExtensions.SqlAdapters.PostgreSql;

/// <inheritdoc/>
public class PostgreSqlDbServer : IDbServer
{
    DbServer IDbServer.Type => DbServer.PostgreSQL;

    PostgreSqlAdapter _adapter = new ();
    ISqlOperationsAdapter IDbServer.Adapter => _adapter;

    PostgreSqlDialect _dialect = new();
    IQueryBuilderSpecialization IDbServer.Dialect => _dialect;

    /// <inheritdoc/>
    public DbConnection? DbConnection { get; set; }

    /// <inheritdoc/>
    public DbTransaction? DbTransaction { get; set; }

    SqlAdapters.QueryBuilderExtensions _queryBuilder = new SqlQueryBuilderPostgreSql();
    /// <inheritdoc/>
    public QueryBuilderExtensions QueryBuilder => _queryBuilder;

#pragma warning disable EF1001
    string IDbServer.ValueGenerationStrategy => NpgsqlAnnotationNames.ValueGenerationStrategy;
#pragma warning restore EF1001

    bool IDbServer.PropertyHasIdentity(IAnnotation annotation) => (Npgsql.EntityFrameworkCore.PostgreSQL.Metadata.NpgsqlValueGenerationStrategy?)annotation.Value == Npgsql.EntityFrameworkCore.PostgreSQL.Metadata.NpgsqlValueGenerationStrategy.IdentityByDefaultColumn;
}
