using System;
using System.Collections.Generic;
using System.Data;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.EntityFrameworkCore;
using MySqlConnector;

namespace EFCore.BulkExtensions.SqlAdapters.MySql;

/// <inheritdoc/>
public class MySqLAdapter : ISqlOperationsAdapter
{
    /// <inheritdoc/>
    #region Methods
    // Insert
    public void Insert<T>(DbContext context, Type type, IList<T> entities, TableInfo tableInfo, Action<decimal>? progress)
    {
        InsertAsync(context, entities, tableInfo, progress, isAsync: false, CancellationToken.None).GetAwaiter().GetResult();
    }

    /// <inheritdoc/>
    public Task InsertAsync<T>(DbContext context, Type type, IList<T> entities, TableInfo tableInfo, Action<decimal>? progress,
        CancellationToken cancellationToken)
    {
        throw new NotImplementedException();
    }

    /// <inheritdoc/>
    protected static async Task InsertAsync<T>(DbContext context, IList<T> entities, TableInfo tableInfo,
        Action<decimal>? progress, bool isAsync, CancellationToken cancellationToken)
    {
        MySqlConnection? connection = tableInfo.MySqlConnection;
        bool closeConnectionInternally = false;
        if (connection == null)
        {
            (connection, closeConnectionInternally) =
                isAsync? 
        }
    }

    /// <inheritdoc/>
    public void Merge<T>(DbContext context, Type type, IList<T> entities, TableInfo tableInfo, OperationType operationType,
        Action<decimal>? progress) where T : class
    {
        throw new NotImplementedException();
    }

    /// <inheritdoc/>
    public Task MergeAsync<T>(DbContext context, Type type, IList<T> entities, TableInfo tableInfo, OperationType operationType,
        Action<decimal>? progress, CancellationToken cancellationToken) where T : class
    {
        throw new NotImplementedException();
    }

    /// <inheritdoc/>
    public void Read<T>(DbContext context, Type type, IList<T> entities, TableInfo tableInfo, Action<decimal>? progress) where T : class
    {
        throw new NotImplementedException();
    }

    /// <inheritdoc/>
    public Task ReadAsync<T>(DbContext context, Type type, IList<T> entities, TableInfo tableInfo, Action<decimal>? progress,
        CancellationToken cancellationToken) where T : class
    {
        throw new NotImplementedException();
    }

    /// <inheritdoc/>
    public void Truncate(DbContext context, TableInfo tableInfo)
    {
        throw new NotImplementedException();
    }

    /// <inheritdoc/>
    public Task TruncateAsync(DbContext context, TableInfo tableInfo, CancellationToken cancellationToken)
    {
        throw new NotImplementedException();
    }
    #endregion
    #region Connection

    internal static async Task<(MySqlConnection, bool)> OpenAndGetMySqlConnectionAsync(DbContext context, CancellationToken cancellationToken)
    {
        bool closeConnectionInternally = false;
        var mySqlConnection = (MySqlConnection)context.Database.GetDbConnection();
        if (mySqlConnection.State != ConnectionState.Open)
        {
            await mySqlConnection.OpenAsync(cancellationToken).ConfigureAwait(false);
            closeConnectionInternally = true;
        }
        return (mySqlConnection, closeConnectionInternally);
    }
    #endregion
}
