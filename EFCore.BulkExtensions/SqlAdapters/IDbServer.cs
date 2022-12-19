using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq.Expressions;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;

namespace EFCore.BulkExtensions.SqlAdapters;

/// <summary>
/// Contains a list of methods to generate Adpaters and helpers instances
/// </summary>
public interface IDbServer
{
    /// <summary>
    /// Returns the Database type
    /// </summary>
    DbServerType Type { get; }

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
    SqlAdapters.QueryBuilderExtensions QueryBuilder { get; }

    /// <summary>
    /// Gets or Sets a DbConnection for the provider
    /// </summary>
    DbConnection? DbConnection { get; set; }

    /// <summary>
    /// Gets or Sets a DbTransaction for the provider
    /// </summary>
    DbTransaction? DbTransaction { get; set; }

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
