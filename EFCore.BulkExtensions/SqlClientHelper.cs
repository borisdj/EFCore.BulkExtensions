using System;
using System.Data;

namespace EFCore.BulkExtensions
{
    /// <summary>
    /// Provides helper functionality to support both System.Data.SqlClient
    /// and Microsoft.Data.SqlClient
    /// </summary>
    public static class SqlClientHelper
    {
        /// <summary>
        /// Creates a parameter with the right type for the connection
        /// </summary>
        /// <param name="connection"></param>
        /// <returns></returns>
        public static IDbDataParameter CreateParameter(IDbConnection connection)
        {
            var parameterType = typeof(Microsoft.Data.SqlClient.SqlParameter);
            return (IDbDataParameter)Activator.CreateInstance(parameterType);
        }
    }
}
