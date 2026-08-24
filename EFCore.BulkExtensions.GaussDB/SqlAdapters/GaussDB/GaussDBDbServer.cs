using GaussDB.EntityFrameworkCore.PostgreSQL.Metadata;
using GaussDB.EntityFrameworkCore.PostgreSQL.Metadata.Internal;
using Microsoft.EntityFrameworkCore.Infrastructure;

namespace EFCore.BulkExtensions.SqlAdapters.GaussDB;

/// <inheritdoc/>
public class GaussDBDbServer : IDbServer
{
    /// <inheritdoc/>
    public SqlType Type => SqlType.GaussDB;

    private GaussDBAdapter _adapter = new();

    /// <inheritdoc/>
    public ISqlOperationsAdapter Adapter => _adapter;


    private GaussDBDialect _dialect = new();

    /// <inheritdoc/>
    public IQueryBuilderSpecialization Dialect => _dialect;

    private SqlQueryBuilder _queryBuilder = new GaussDBQueryBuilder();

    /// <inheritdoc/>
    public SqlQueryBuilder QueryBuilder => _queryBuilder;

#pragma warning disable EF1001
    /// <inheritdoc/>
    public string ValueGenerationStrategy => GaussDBAnnotationNames.ValueGenerationStrategy;

    /// <inheritdoc/>
    public bool PropertyHasIdentity(IAnnotation annotation)
    {
        return (GaussDBValueGenerationStrategy?)annotation.Value == GaussDBValueGenerationStrategy.IdentityByDefaultColumn;
    }
#pragma warning restore EF1001
}
