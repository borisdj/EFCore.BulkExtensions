using EFCore.BulkExtensions.SqlAdapters;
using System;
using System.Collections.Generic;

namespace EFCore.BulkExtensions.Tests.BulkInsertOrUpdate;

public abstract class BulkDbTestsFixture : IDisposable
{
    private readonly string _dbName;

    private readonly HashSet<SqlType> _initialized = new();

    public BulkDbTestsFixture(string dbName)
    {
        _dbName = dbName;
    }

    public SimpleBulkTestsContext GetDb(SqlType sqlType)
    {
        var shouldInitialize = _initialized.Add(sqlType);

        if (shouldInitialize)
        {
            using var db = CreateContextInternal(sqlType);

            db.Database.EnsureDeleted();
            db.Database.EnsureCreated();
        }

        return CreateContextInternal(sqlType);
    }

    private SimpleBulkTestsContext CreateContextInternal(SqlType sqlType)
    {
        var options = ContextUtil.GetOptions<SimpleBulkTestsContext>(sqlType, databaseName: _dbName);

        return new SimpleBulkTestsContext(options);
    }

    public void Dispose()
    {
        // Drop all dbs which were initialized:
        foreach (var sqlType in _initialized)
        {
            using var db = GetDb(sqlType);
            db.Database.EnsureDeleted();
        }
    }
}
