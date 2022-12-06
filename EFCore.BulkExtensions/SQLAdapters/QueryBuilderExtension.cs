using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;

namespace EFCore.BulkExtensions.SqlAdapters;


/// <summary>
/// Contains a list of methods to generate Adpaters and helpers instances
/// </summary>
public abstract class QueryBuilderExtensions
{
    /// <summary>
    /// Generates SQL query to select output from a table
    /// </summary>
    /// <param name="tableInfo"></param>
    /// <returns></returns>
    public abstract string SelectFromOutputTable(TableInfo tableInfo);

    /// <summary>
    /// Restructures a sql query for batch commands
    /// </summary>
    /// <param name="sql"></param>
    /// <param name="isDelete"></param>
    public abstract string RestructureForBatch(string sql, bool isDelete = false);

    /// <summary>
    /// Returns a DbParameters intanced per provider
    /// </summary>
    /// <param name="sqlParameter"></param>
    /// <returns></returns>
    public abstract object CreateParameter(SqlParameter sqlParameter);

    /// <summary>
    /// Returns NpgsqlDbType for PostgreSql parameters. Throws <see cref="NotImplementedException"/> for anothers providers
    /// </summary>
    /// <returns></returns>
    public abstract object Dbtype();
}
