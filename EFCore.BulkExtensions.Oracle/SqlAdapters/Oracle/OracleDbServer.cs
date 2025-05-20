using Microsoft.EntityFrameworkCore.Infrastructure;
using Oracle.EntityFrameworkCore.Metadata;

namespace EFCore.BulkExtensions.SqlAdapters.Oracle;

/// <inheritdoc/>
public class OracleDbServer : IDbServer
{
    SqlType IDbServer.Type => SqlType.Oracle;

    private readonly OracleAdapter _adapter = new();
    ISqlOperationsAdapter IDbServer.Adapter => _adapter;

    private readonly OracleDialect _dialect = new();
    IQueryBuilderSpecialization IDbServer.Dialect => _dialect;

    private readonly SqlAdapters.SqlQueryBuilder _queryBuilder = new OracleQueryBuilder();
    /// <inheritdoc/>
    public SqlQueryBuilder QueryBuilder => _queryBuilder;

    string IDbServer.ValueGenerationStrategy => nameof(OracleValueGenerationStrategy);

    bool IDbServer.PropertyHasIdentity(IAnnotation annotation) => (OracleValueGenerationStrategy?)annotation.Value == OracleValueGenerationStrategy.IdentityColumn;
}
