using System;
using System.Collections.Generic;
using System.Data;
using System.Text;

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
            var parameterType = GetParameterType(connection);
            return (IDbDataParameter)Activator.CreateInstance(parameterType);
        }

        /// <summary>
        /// Gets the type of parameter supported by the connection
        /// </summary>
        /// <param name="connection"></param>
        /// <returns></returns>
        public static Type GetParameterType(IDbConnection connection)
        {
            if (IsSystemConnection(connection))
            {
                return typeof(System.Data.SqlClient.SqlParameter);
            }
            else
            {
                return typeof(Microsoft.Data.SqlClient.SqlParameter);
            }
        }

        /// <summary>
        /// As long as <paramref name="parameter"/> is the correct type for
        /// <paramref name="connection"/>, the original <paramref name="parameter"/>
        /// will be returned; otherwise, a new parameter will be returned with the 
        /// name and value copied from <paramref name="parameter"/>.  Note, only name
        /// and value are copied, so if you have set other properties, this method
        /// will ignore those.
        /// </summary>
        /// <param name="connection"></param>
        /// <param name="parameter"></param>
        /// <returns></returns>
        public static IDbDataParameter CorrectParameterType(IDbConnection connection, IDbDataParameter parameter)
        {
            var correctParameterType = GetParameterType(connection);
            if (parameter.GetType() == correctParameterType)
            {
                // since type already matches, return original
                return parameter;
            }

            // create a new parameter of the correct type
            var newParameter = CreateParameter(connection);

            // copy properties from original parameter to copy
            newParameter.ParameterName = parameter.ParameterName;
            newParameter.Value = parameter.Value;

            return newParameter;
        }

        /// <summary>
        /// Use to determine what type of structures support the connection
        /// </summary>
        /// <param name="connection"></param>
        /// <returns>true if the connection is System.Data.SqlClient.SqlConnection; otherwise, 
        /// returns false, indicating that it is Microsoft.Data.SqlClient.SqlConnection.</returns>
        internal static bool IsSystemConnection(IDbConnection connection)
        {
            if (connection.GetType() == typeof(System.Data.SqlClient.SqlConnection))
            {
                return true;
            }

            return false;
        }
    }
}
