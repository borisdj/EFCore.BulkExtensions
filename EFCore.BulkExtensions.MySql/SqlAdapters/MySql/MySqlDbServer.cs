using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;

namespace EFCore.BulkExtensions.SqlAdapters.MySql;

/// <inheritdoc/>
public class MySqlDbServer : IDbServer
{
    SqlType IDbServer.Type => SqlType.MySql;

    MySqlAdapter _adapter = new ();
    ISqlOperationsAdapter IDbServer.Adapter => _adapter;

    MySqlDialect _dialect = new();
    IQueryBuilderSpecialization IDbServer.Dialect => _dialect;

    SqlAdapters.SqlQueryBuilder _queryBuilder = new MySqlQueryBuilder();
    /// <inheritdoc/>
    public SqlQueryBuilder QueryBuilder => _queryBuilder;

    string IDbServer.ValueGenerationStrategy => nameof(MySqlValueGenerationStrategy);

    bool IDbServer.PropertyHasIdentity(IAnnotation annotation) => (MySqlValueGenerationStrategy?)annotation.Value == MySqlValueGenerationStrategy.IdentityColumn;
}
