using System.Collections.Generic;
using System.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;

namespace EFCore.BulkExtensions
{
    internal static class DbContextBulkTransaction
    {
        public static void Execute<T>(DbContext context, IList<T> entities, OperationType operationType)
        {
            var tableInfo = new TableInfo();
            var isDeleteOperation = operationType == OperationType.Delete;
            tableInfo.LoadData<T>(context, isDeleteOperation);
            
            using (SqlTransaction transaction = (SqlTransaction)context.Database.BeginTransaction().GetDbTransaction())
            {
                try {
                    if (operationType == OperationType.Insert)
                    {
                        SqlBulkOperation.Insert<T>(context, transaction, entities, tableInfo, false);
                    }
                    else
                    {
                        SqlBulkOperation.Merge<T>(context, transaction, entities, tableInfo, operationType);
                    }
                    transaction.Commit();
                }
                catch {
                    transaction.Rollback();
                }
            }
        }
    }
}
