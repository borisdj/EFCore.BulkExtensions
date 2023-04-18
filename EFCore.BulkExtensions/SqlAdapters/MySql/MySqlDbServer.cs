using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;
using System.Data.Common;

namespace EFCore.BulkExtensions.SqlAdapters.MySql;

/// <inheritdoc/>
public class MySqlDbServer : IDbServer
{
    DatabaseType IDbServer.Type => DatabaseType.MySql;

    MySqlAdapter _adapter = new ();
    ISqlOperationsAdapter IDbServer.Adapter => _adapter;

    MySqlDialect _dialect = new();
    IQueryBuilderSpecialization IDbServer.Dialect => _dialect;

    SqlAdapters.QueryBuilderExtensions _queryBuilder = new MySqlQueryBuilder();
    /// <inheritdoc/>
    public QueryBuilderExtensions QueryBuilder => _queryBuilder;

    string IDbServer.ValueGenerationStrategy => nameof(MySqlValueGenerationStrategy);

    /// <inheritdoc/>
    public DbConnection? DbConnection { get; set; }

    /// <inheritdoc/>
    public DbTransaction? DbTransaction { get; set; }

    bool IDbServer.PropertyHasIdentity(IAnnotation annotation) => (MySqlValueGenerationStrategy?)annotation.Value == MySqlValueGenerationStrategy.IdentityColumn;
}
