using Microsoft.EntityFrameworkCore.Infrastructure;

namespace EFCore.BulkExtensions.SqlAdapters;

/// <summary>
/// Contains a list of methods to generate Adpaters and helpers instances
/// </summary>
public interface IDbServer
{
    /// <summary>
    /// Returns the Database type
    /// </summary>
    SqlType Type { get; }

    /// <summary>
    /// Returns a Operation Server Adapter for DbServer
    /// </summary>
    ISqlOperationsAdapter Adapter { get; }

    /// <summary>
    /// Contains a list of methods for query operations
    /// </summary>
    IQueryBuilderSpecialization Dialect { get; }

    /// <summary>
    /// Contains a compilation of SQL queries used in EFCore.
    /// </summary>
    SqlQueryBuilder QueryBuilder { get; }

    /// <summary>
    /// Returns the current Provider's Value Generating Strategy
    /// </summary>
    string ValueGenerationStrategy { get; }

    /// <summary>
    /// Returns if <paramref name="annotation"/> has Identity Generation Strategy Annotation
    /// </summary>
    /// <param name="annotation"></param>
    /// <returns></returns>
    bool PropertyHasIdentity(IAnnotation annotation);
}
