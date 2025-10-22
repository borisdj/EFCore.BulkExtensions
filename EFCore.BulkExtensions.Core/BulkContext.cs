using EFCore.BulkExtensions.SqlAdapters;
using Microsoft.EntityFrameworkCore;
using System.Data.Common;

namespace EFCore.BulkExtensions;

/// <summary>
/// The bulk context, which contains every related to a bulk operation.
/// </summary>
/// <param name="dbContext">The database context.</param>
/// <param name="server">The server.</param>
public sealed class BulkContext(DbContext dbContext, IDbServer server)
{
    /// <summary>
    /// The actual database context.
    /// </summary>
    public DbContext DbContext { get; } = dbContext;

    /// <summary>
    /// The server.
    /// </summary>
    public IDbServer Server { get; } = server;

    /// <summary>
    /// The adapter.
    /// </summary>
    public ISqlOperationsAdapter Adapter { get; } = server.Adapter;

    /// <summary>
    /// Contains a list of methods for query operations
    /// </summary>
    public IQueryBuilderSpecialization Dialect { get; } = server.Dialect;

    /// <summary>
    /// Contains a compilation of SQL queries used in EFCore.
    /// </summary>
    public SqlQueryBuilder QueryBuilder { get; } = server.QueryBuilder;

    /// <summary>
    /// Gets or Sets a DbConnection for the provider
    /// </summary>
    public DbConnection? DbConnection { get; set; }

    /// <summary>
    /// Gets or Sets a DbTransaction for the provider
    /// </summary>
    public DbTransaction? DbTransaction { get; set; }

    internal static BulkContext Create(DbContext dbContext)
    {
        return new BulkContext(dbContext, SqlAdaptersMapping.DbServer(dbContext));
    }
}
