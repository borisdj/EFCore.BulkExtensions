using System.Collections.Generic;
using Microsoft.EntityFrameworkCore;

namespace EFCore.BulkExtensions
{
    internal static class DbContextBulkTransaction
    {
        public static void Execute<T>(DbContext context, IList<T> entities, OperationType operationType, bool setOutputIdentity = false) where T : class
        {
            var tableInfo = new TableInfo();
            var isDeleteOperation = operationType == OperationType.Delete;
            tableInfo.LoadData<T>(context, isDeleteOperation);
            tableInfo.SetOutputIdentity = setOutputIdentity;

            if (operationType == OperationType.Insert && !setOutputIdentity)
            {
                SqlBulkOperation.Insert<T>(context, entities, tableInfo);
            }
            else
            {
                SqlBulkOperation.Merge<T>(context, entities, tableInfo, operationType);
            }
        }
    }
}
