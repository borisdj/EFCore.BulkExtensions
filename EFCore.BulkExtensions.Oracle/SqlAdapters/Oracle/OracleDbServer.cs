using Microsoft.EntityFrameworkCore.Infrastructure;
using Oracle.EntityFrameworkCore.Metadata;
using System.Data.Common;

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

    /// <inheritdoc/>
    public DbConnection? DbConnection { get; set; }

    /// <inheritdoc/>
    public DbTransaction? DbTransaction { get; set; }

    bool IDbServer.PropertyHasIdentity(IAnnotation annotation) => (OracleValueGenerationStrategy?)annotation.Value == OracleValueGenerationStrategy.IdentityColumn;
}
