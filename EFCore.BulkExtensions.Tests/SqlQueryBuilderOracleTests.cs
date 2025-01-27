using DelegateDecompiler.EntityFrameworkCore;
using EFCore.BulkExtensions.SqlAdapters;
using EFCore.BulkExtensions.SqlAdapters.Oracle;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

namespace EFCore.BulkExtensions.Tests;

public class SqlQueryBuilderUnitOracleTests
{
    private TableInfo GetTestTableInfo(Func<string, string, string>? onConflictUpdateWhereSql = null)
    {
        var tableInfo = new TableInfo()
        {
            Schema = "dbo",
            TempSchema = "dbo",
            TableName = nameof(Item),
            TempTableName = nameof(Item) + "Temp1234",
            TempTableSufix = "Temp1234",
            PrimaryKeysPropertyColumnNameDict = new Dictionary<string, string> { { nameof(Item.ItemId), nameof(Item.ItemId) } },
            BulkConfig = new BulkConfig()
            {
                OnConflictUpdateWhereSql = onConflictUpdateWhereSql
            }
        };
        const string nameText = nameof(Item.Name);

        tableInfo.PropertyColumnNamesDict.Add(tableInfo.PrimaryKeysPropertyColumnNameDict.Keys.First(), tableInfo.PrimaryKeysPropertyColumnNameDict.Values.First());
        tableInfo.PropertyColumnNamesDict.Add(nameText, nameText);
        //compare on all columns (default)
        tableInfo.PropertyColumnNamesCompareDict = tableInfo.PropertyColumnNamesDict;
        //update all columns (default)
        tableInfo.PropertyColumnNamesUpdateDict = tableInfo.PropertyColumnNamesDict;
        return tableInfo;
    }

    [Fact]
    public void MergeTableInsertTest()
    {
        TableInfo tableInfo = GetTestTableInfo();
        tableInfo.IdentityColumnName = "ItemId";
        string result = OracleQueryBuilder.MergeTable<Item>(tableInfo, OperationType.Insert);

        string expected = "INSERT INTO dbo.Item (Name) SELECT Name FROM dbo.ItemTemp1234; ";

        Assert.Equal(result, expected);
    }
    [Fact]
    public void MergeTableUpdateTest()
    {
        TableInfo tableInfo = GetTestTableInfo();
        tableInfo.IdentityColumnName = "ItemId";
        string result = OracleQueryBuilder.MergeTable<Item>(tableInfo, OperationType.Update);

        string expected = @"MERGE INTO dbo.Item AS A
USING dbo.ItemTemp1234 AS B
ON (A.ItemId = B.ItemId)
WHEN MATCHED THEN
    UPDATE SET A.Name = B.Name;";

        Assert.Equal(result, expected);
    }
    [Fact]
    public void MergeTableDeleteTest()
    {
        TableInfo tableInfo = GetTestTableInfo();
        tableInfo.IdentityColumnName = "ItemId";
        string result = OracleQueryBuilder.MergeTable<Item>(tableInfo, OperationType.Delete);

        string expected = "DELETE FROM FROM dbo.Item A WHERE A.ItemId IN (SELECT B.ItemId FROM dbo.ItemTemp1234 B); ";

        Assert.Equal(result, expected);
    }

    [Fact]
    public void MergeTableInsertOrUpdateTest()
    {
        TableInfo tableInfo = GetTestTableInfo();
        tableInfo.IdentityColumnName = "ItemId";
        string result = OracleQueryBuilder.MergeTable<Item>(tableInfo, OperationType.InsertOrUpdate);

        string expected = @"MERGE INTO dbo.Item AS A
USING dbo.ItemTemp1234 AS B
ON (A.ItemId = B.ItemId)
WHEN MATCHED THEN
    UPDATE SET A.Name = B.Name
WHEN NOT MATCHED THEN
    INSERT (ItemId, Name)
    VALUES (B.ItemId, B.Name);";

        Assert.Equal(result, expected);
    }
}
