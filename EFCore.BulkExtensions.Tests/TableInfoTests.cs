﻿using System.Collections.Generic;
using EFCore.BulkExtensions.SqlAdapters;
using Xunit;

namespace EFCore.BulkExtensions.Tests;

public class TableInfoTests
{
    [Theory]
    [InlineData(SqlType.SqlServer)]
    private void CreateTableInfo_For_Hidden_PrimaryKey_Does_Not_Throw(SqlType sqlType)
    {
        using var dbContext = new TestContext(sqlType);
        var context = BulkContext.Create(dbContext);
        var entities = new List<TrayType> { new TrayType() };
        var info = TableInfo.CreateInstance(context, null, entities, OperationType.Insert, new BulkConfig
        {
            SetOutputIdentity = true
        });

        Assert.NotNull(info);
    }
}
