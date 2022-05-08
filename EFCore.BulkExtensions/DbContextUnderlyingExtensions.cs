using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;
using System.Data.Common;

namespace EFCore.BulkExtensions;

#pragma warning disable CS1591 // No XML comments required here
public static class DbContextUnderlyingExtensions
{
    public static DbConnection GetUnderlyingConnection(this DbContext context, BulkConfig config)
    {
        var connection = context.Database.GetDbConnection();
        if (config?.UnderlyingConnection != null)
        {
            connection = config.UnderlyingConnection(connection);
        }
        return connection;
    }

    public static DbTransaction GetUnderlyingTransaction(this IDbContextTransaction ctxTransaction, BulkConfig config)
    {
        var dbTransaction = ctxTransaction.GetDbTransaction();
        if (config?.UnderlyingTransaction != null)
        {
            dbTransaction = config.UnderlyingTransaction(dbTransaction);
        }
        return dbTransaction;
    }
}
#pragma warning restore CS1591 // No XML comments required here

