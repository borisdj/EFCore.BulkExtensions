using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace EFCore.BulkExtensions.Tests
{
    public static class DbDataReaderExtensions
    {
        public static T Field<T>(this DbDataReader reader, string columnName)
        {
            var columnIndex = reader.GetOrdinal(columnName);
            return reader.GetFieldValue<T>(columnIndex);
        }

        public static async Task<T> FieldAsync<T>(this DbDataReader reader, string columnName, CancellationToken cancellationToken = default)
        {
            var columnIndex = reader.GetOrdinal(columnName);
            return await reader.GetFieldValueAsync<T>(columnIndex, cancellationToken);
        }
    }
}
