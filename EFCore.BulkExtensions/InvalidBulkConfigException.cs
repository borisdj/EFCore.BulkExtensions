using System;
using System.Runtime.Serialization;

namespace EFCore.BulkExtensions
{
    [Serializable]
    public class InvalidBulkConfigException : Exception
    {
        public InvalidBulkConfigException()
        {
        }

        public InvalidBulkConfigException(string message) : base(message)
        {
        }

        public InvalidBulkConfigException(string message, Exception innerException) : base(message, innerException)
        {
        }

        protected InvalidBulkConfigException(SerializationInfo info, StreamingContext context) : base(info, context)
        {
        }
    }
}
