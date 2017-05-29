using System.Collections.Generic;
using Microsoft.EntityFrameworkCore;

namespace EFCore.BulkExtensions
{
    public static class DbContextBulkExtensions
    {
        public static void BulkInsert<T>(this DbContext context, IList<T> entities, BulkConfig bulkConfig = null) where T : class
        {
            DbContextBulkTransaction.Execute<T>(context, entities, OperationType.Insert, bulkConfig);
        }

        public static void BulkInsertOrUpdate<T>(this DbContext context, IList<T> entities, BulkConfig bulkConfig = null) where T : class
        {
            DbContextBulkTransaction.Execute<T>(context, entities, OperationType.InsertOrUpdate, bulkConfig);
        }

        public static void BulkUpdate<T>(this DbContext context, IList<T> entities, BulkConfig bulkConfig = null) where T : class
        {
            DbContextBulkTransaction.Execute<T>(context, entities, OperationType.Update, bulkConfig);
        }

        public static void BulkDelete<T>(this DbContext context, IList<T> entities, BulkConfig bulkConfig = null) where T : class
        {
            DbContextBulkTransaction.Execute<T>(context, entities, OperationType.Delete, bulkConfig);
        }
    }
}
