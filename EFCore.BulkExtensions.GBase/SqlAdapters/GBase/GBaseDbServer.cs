using GBase.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Infrastructure;
using System.Data.Common;

namespace EFCore.BulkExtensions.SqlAdapters.GBase;

/// <inheritdoc/>
public class GBaseDbServer : IDbServer
{
    SqlType IDbServer.Type => SqlType.GBase;

    GBaseAdapter _adapter = new ();
    ISqlOperationsAdapter IDbServer.Adapter => _adapter;

    GBaseDialect _dialect = new();
    IQueryBuilderSpecialization IDbServer.Dialect => _dialect;

    /// <inheritdoc/>
    public DbConnection? DbConnection { get; set; }

    /// <inheritdoc/>
    public DbTransaction? DbTransaction { get; set; }

    SqlAdapters.SqlQueryBuilder _queryBuilder = new GBaseQueryBuilder();
    /// <inheritdoc/>
    public SqlQueryBuilder QueryBuilder => _queryBuilder;

    string IDbServer.ValueGenerationStrategy => nameof(GBaseValueGenerationStrategy);

    bool IDbServer.PropertyHasIdentity(IAnnotation annotation) => (GBaseValueGenerationStrategy?)annotation.Value == GBaseValueGenerationStrategy.IdentityColumn;
}
