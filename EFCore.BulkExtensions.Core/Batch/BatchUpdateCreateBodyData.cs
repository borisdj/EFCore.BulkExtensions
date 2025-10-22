﻿using EFCore.BulkExtensions.SqlAdapters;
using Microsoft.EntityFrameworkCore;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Text.RegularExpressions;

namespace EFCore.BulkExtensions;

/// <summary>
/// Provides config for batch update/create
/// </summary>
public class BatchUpdateCreateBodyData
{
    private readonly BulkConfig _tableInfoBulkConfig;
    private readonly Dictionary<Type, TableInfo> _tableInfoLookup;

    /// <summary>
    /// Creates an instance of BatchUpdateCreateBodyData used to provide a config for batch updates and creations
    /// </summary>
    /// <param name="baseSql"></param>
    /// <param name="context"></param>
    /// <param name="innerParameters"></param>
    /// <param name="query"></param>
    /// <param name="rootType"></param>
    /// <param name="tableAlias"></param>
    /// <param name="updateExpression"></param>
    public BatchUpdateCreateBodyData(
        string baseSql,
        BulkContext context,
        IEnumerable<DbParameter> innerParameters,
        IQueryable query,
        Type rootType,
        string tableAlias,
        LambdaExpression updateExpression)
    {
        BaseSql = baseSql;
        Context = context;
        Query = query;
        RootInstanceParameterName = updateExpression.Parameters?.FirstOrDefault()?.Name;
        RootType = rootType;
        TableAlias = tableAlias;
        TableAliasesInUse = new List<string>();
        UpdateColumnsSql = new StringBuilder();
        UpdateExpression = updateExpression;

        _tableInfoBulkConfig = new BulkConfig();
        _tableInfoLookup = new Dictionary<Type, TableInfo>();

        var tableInfo = TableInfo.CreateInstance(context, rootType, Array.Empty<object>(), OperationType.Read, _tableInfoBulkConfig);
        _tableInfoLookup.Add(rootType, tableInfo);

        SqlParameters = new List<DbParameter>(innerParameters);

        foreach (Match match in BatchUtil.TableAliasPattern.Matches(baseSql))
        {
            TableAliasesInUse.Add(match.Groups[2].Value);
        }
    }

#pragma warning disable CS1591 // No need for XML comments here.
    public string BaseSql { get; }
    public SqlType DatabaseType => Context.Server.Type;
    public BulkContext Context { get; }
    public DbContext DbContext => Context.DbContext;
    public IQueryable Query { get; }
    public string? RootInstanceParameterName { get; }
    public Type RootType { get; }
    public List<DbParameter> SqlParameters { get; }
    public string TableAlias { get; }
    public List<string> TableAliasesInUse { get; }
    public StringBuilder UpdateColumnsSql { get; }
    public LambdaExpression UpdateExpression { get; }

    public TableInfo? GetTableInfoForType(Type typeToLookup)
    {
        if (_tableInfoLookup.TryGetValue(typeToLookup, out var tableInfo))
        {
            return tableInfo;
        }

        tableInfo = TableInfo.CreateInstance(Context, typeToLookup, Array.Empty<object>(), OperationType.Read, _tableInfoBulkConfig);
        if (tableInfo != null)
        {
            _tableInfoLookup.Add(typeToLookup, tableInfo);
        }

        return tableInfo;
    }
#pragma warning restore CS1591 // No need for XML comments here.
}
