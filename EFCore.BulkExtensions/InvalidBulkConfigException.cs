using System;
using System.Runtime.Serialization;

namespace EFCore.BulkExtensions;

public static class BulkExceptionMessage
{
    public static string ColumnMappingNotMatch => "The given ColumnMapping does not match up with any column in the source or destination";
    public static string SpecifiedDoubleConfigLists => "Only one group of properties, either {0} or {1} can be specified, specifying both not allowed.";
}

[Serializable]
public class InvalidBulkConfigException : Exception
{
    public InvalidBulkConfigException() { }

    public InvalidBulkConfigException(string message) : base(message) { }

    public InvalidBulkConfigException(string message, Exception innerException) : base(message, innerException) { }

    protected InvalidBulkConfigException(SerializationInfo info, StreamingContext context) : base(info, context) { }
}


[Serializable]
class ColumnMappingExceptionMessage : InvalidBulkConfigException
{
    public ColumnMappingExceptionMessage() : base(BulkExceptionMessage.ColumnMappingNotMatch) { }
}

[Serializable]
class MultiplePropertyListSetException : InvalidBulkConfigException
{
    public MultiplePropertyListSetException() { }
    public MultiplePropertyListSetException(string propertyList1Name, string PropertyList2Name)
        : base(string.Format(BulkExceptionMessage.SpecifiedDoubleConfigLists, propertyList1Name, PropertyList2Name)) { }
}
