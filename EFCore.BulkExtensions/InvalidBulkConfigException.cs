using System;
using System.Runtime.Serialization;

namespace EFCore.BulkExtensions
{
    [Serializable]
    public class InvalidBulkConfigException : Exception
    {
        public InvalidBulkConfigException() { }

        public InvalidBulkConfigException(string message) : base(message) { }

        public InvalidBulkConfigException(string message, Exception innerException) : base(message, innerException) { }

        protected InvalidBulkConfigException(SerializationInfo info, StreamingContext context) : base(info, context) { }
    }

    [Serializable]
    class MultiplePropertyListSetException : InvalidBulkConfigException
    {
        public MultiplePropertyListSetException() { }
        public MultiplePropertyListSetException(string propertyList1Name, string PropertyList2Name)
            : base(string.Format("Only one group of properties, either {0} or {1} can be specified, specifying both not allowed.", propertyList1Name, PropertyList2Name)) { }
    }
}
