﻿using System;
using System.Runtime.Serialization;

namespace EFCore.BulkExtensions;

/// <summary>
/// Provides a list of static exception messages
/// </summary>
public static class BulkExceptionMessage
{
    /// <summary>
    /// Exception message to define column mapping does not match
    /// </summary>
    public static string ColumnMappingNotMatch => "The given ColumnMapping does not match up with any column in the source or destination";
    /// <summary>
    /// Exception message to define specified double config list is not valid
    /// </summary> 
    public static string SpecifiedDoubleConfigLists => "Only one group of properties, either {0} or {1} can be specified, specifying both not allowed.";
}

/// <summary>
/// Custom exception class
/// </summary>
//[Serializable]
public class InvalidBulkConfigException : Exception
{
    /// <summary>
    /// Custom exception class to indicate a custom exception was triggered for BulkConfig
    /// </summary>
    public InvalidBulkConfigException() { }

    /// <summary>
    /// Custom exception class to indicate a custom exception was triggered for BulkConfig
    /// </summary>
    public InvalidBulkConfigException(string message) : base(message) { }

    /// <summary>
    /// Custom exception class to indicate a custom exception was triggered for BulkConfig
    /// </summary>
    /// <param name="message"></param>
    /// <param name="innerException"></param>
    public InvalidBulkConfigException(string message, Exception innerException) : base(message, innerException) { }
    /*
    /// <summary>
    /// Custom exception class to indicate a custom exception was triggered for BulkConfig
    /// </summary>
    /// <param name="info"></param>
    /// <param name="context"></param>
    protected InvalidBulkConfigException(SerializationInfo info, StreamingContext context) : base(info, context) { }
    */
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
