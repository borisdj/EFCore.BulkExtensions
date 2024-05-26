using Microsoft.EntityFrameworkCore.Infrastructure;
using System.Data.Common;

namespace EFCore.BulkExtensions.SqlAdapters;

/// <summary>
/// Contains a list of methods to generate Adpaters and helpers instances
/// </summary>
public interface IDbServer
{
    /// <summary>
    /// Returns the Database type
    /// 返回数据库类型
    /// </summary>
    SqlType Type { get; }

    /// <summary>
    /// Returns a Operation Server Adapter for DbServer
    /// 返回DbServer的操作服务器适配器
    /// </summary>
    ISqlOperationsAdapter Adapter { get; }

    /// <summary>
    /// Contains a list of methods for query operations
    /// 包含查询操作的方法列表
    /// </summary>
    IQueryBuilderSpecialization Dialect { get; }

    /// <summary>
    /// Contains a compilation of SQL queries used in EFCore.
    /// 包含EFCore中使用的SQL查询的编译。
    /// </summary>
    SqlAdapters.SqlQueryBuilder QueryBuilder { get; }

    /// <summary>
    /// Gets or Sets a DbConnection for the provider
    /// 获取或设置提供程序的DbConnection
    /// </summary>
    DbConnection? DbConnection { get; set; }

    /// <summary>
    /// Gets or Sets a DbTransaction for the provider
    /// 获取或设置提供程序的DbTransaction
    /// </summary>
    DbTransaction? DbTransaction { get; set; }

    /// <summary>
    /// Returns the current Provider's Value Generating Strategy
    /// 返回当前提供者的价值生成策略
    /// </summary>
    string ValueGenerationStrategy { get; }

    /// <summary>
    /// Returns if <paramref name="annotation"/> has Identity Generation Strategy Annotation
    /// 返回＜paramref name=“annotation”/＞是否具有标识生成策略注释
    /// </summary>
    /// <param name="annotation"></param>
    /// <returns></returns>
    bool PropertyHasIdentity(IAnnotation annotation);
}
