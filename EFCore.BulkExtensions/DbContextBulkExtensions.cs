using System.Collections.Generic;
using Microsoft.EntityFrameworkCore;

namespace EFCore.BulkExtensions
{
    public static class DbContextBulkExtensions
    {
        public static void BulkInsert<T>(this DbContext context, IList<T> entities)
        {
            DbContextBulkTransaction.Execute<T>(context, entities, OperationType.Insert);
        }

        public static void BulkInsertOrUpdate<T>(this DbContext context, IList<T> entities)
        {
            DbContextBulkTransaction.Execute<T>(context, entities, OperationType.Insert);
        }

        public static void BulkUpdate<T>(this DbContext context, IList<T> entities)
        {
            DbContextBulkTransaction.Execute<T>(context, entities, OperationType.Update);
        }

        public static void BulkDelete<T>(this DbContext context, IList<T> entities)
        {
            DbContextBulkTransaction.Execute<T>(context, entities, OperationType.Delete);
        }
    }
}
