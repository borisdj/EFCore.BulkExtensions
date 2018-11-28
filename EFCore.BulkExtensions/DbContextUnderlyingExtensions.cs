using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Text;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;

namespace EFCore.BulkExtensions
{
    public static class DbContextUnderlyingExtensions
    {
        public static DbConnection GetUnderlyingConnection(this DbContext context, BulkConfig config)
        {
            var connection = context.Database.GetDbConnection();
            if (config?.GetUnderlyingConnection != null) connection = config.GetUnderlyingConnection(connection);
            return connection;
        }

        public static DbTransaction GetUnderlyingTransaction(this IDbContextTransaction ctxTransaction, BulkConfig config)
        {
            var dbTransaction = ctxTransaction.GetDbTransaction();
            if (config?.GetUnderlyingTransaction!= null) dbTransaction = config.GetUnderlyingTransaction(dbTransaction);
            return dbTransaction;
        }

        public static DbTransaction GetUnderlyingTransaction(this DbContext context, BulkConfig config)
        {
            var dbTransaction = context.Database.CurrentTransaction?.GetDbTransaction();
            if (dbTransaction == null) return null;
            if (config?.GetUnderlyingTransaction!= null) dbTransaction = config.GetUnderlyingTransaction(dbTransaction);
            return dbTransaction;
        }
    }
}
