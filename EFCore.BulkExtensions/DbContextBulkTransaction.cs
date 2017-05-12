using System.Collections.Generic;
using Microsoft.EntityFrameworkCore;

namespace EFCore.BulkExtensions
{
    internal static class DbContextBulkTransaction
    {
        public static void Execute<T>(DbContext context, IList<T> entities, OperationType operationType)
        {
            var tableInfo = new TableInfo();
            var isDeleteOperation = operationType == OperationType.Delete;
            tableInfo.LoadData<T>(context, isDeleteOperation);
            
            if (operationType == OperationType.Insert)
            {
                SqlBulkOperation.Insert<T>(context, entities, tableInfo, false);
            }
            else
            {
                SqlBulkOperation.Merge<T>(context, entities, tableInfo, operationType);
            }
        }
    }
}
