using DelegateDecompiler.EntityFrameworkCore;
using EFCore.BulkExtensions.SqlAdapters;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

namespace EFCore.BulkExtensions.Tests;

public class SqlQueryBuilderUnitTests
{
    [Fact]
    public void MergeTableInsertTest()
    {
        TableInfo tableInfo = GetTestTableInfo();
        tableInfo.IdentityColumnName = "ItemId";
        string result = SqlQueryBuilder.MergeTable<Item>(null, tableInfo, OperationType.Insert).sql;

        string expected = "MERGE [dbo].[Item] WITH (HOLDLOCK) AS T " + 
                          "USING (SELECT TOP 0 * FROM [dbo].[ItemTemp1234] ORDER BY [ItemId]) AS S " +
                          "ON T.[ItemId] = S.[ItemId] " +
                          "WHEN NOT MATCHED BY TARGET " +
                          "THEN INSERT ([Name]) VALUES (S.[Name]);";

        Assert.Equal(result, expected);
    }

    [Fact]
    public void MergeTableInsertOrUpdateTest()
    {
        TableInfo tableInfo = GetTestTableInfo();
        tableInfo.IdentityColumnName = "ItemId";
        string result = SqlQueryBuilder.MergeTable<Item>(null, tableInfo, OperationType.InsertOrUpdate).sql;

        string expected = "MERGE [dbo].[Item] WITH (HOLDLOCK) AS T " +
                          "USING (SELECT TOP 0 * FROM [dbo].[ItemTemp1234] ORDER BY [ItemId]) AS S " +
                          "ON T.[ItemId] = S.[ItemId] " +
                          "WHEN NOT MATCHED BY TARGET " +
                          "THEN INSERT ([Name]) VALUES (S.[Name]) " +
                          "WHEN MATCHED AND " +
                          "EXISTS (SELECT S.[Name]" +
                          " EXCEPT SELECT T.[Name]) " +
                          "THEN UPDATE SET T.[Name] = S.[Name];";

        Assert.Equal(result, expected);
    }
    
    [Fact]
    public void MergeTableInsertOrUpdateWithOnConflictUpdateWhereSqlTest()
    {
        TableInfo tableInfo = GetTestTableInfo((existing, inserted) => $"{inserted}.ItemTimestamp > {existing}.ItemTimestamp");
        tableInfo.IdentityColumnName = "ItemId";
        string actual = SqlQueryBuilder.MergeTable<Item>(null, tableInfo, OperationType.InsertOrUpdate).sql;

        string expected = "MERGE [dbo].[Item] WITH (HOLDLOCK) AS T " +
                          "USING (SELECT TOP 0 * FROM [dbo].[ItemTemp1234] ORDER BY [ItemId]) AS S " +
                          "ON T.[ItemId] = S.[ItemId] " +
                          "WHEN NOT MATCHED BY TARGET " +
                          "THEN INSERT ([Name]) VALUES (S.[Name]) " +
                          "WHEN MATCHED AND " +
                          "EXISTS (SELECT S.[Name]" +
                          " EXCEPT SELECT T.[Name]) " +
                          "AND S.ItemTimestamp > T.ItemTimestamp " +
                          "THEN UPDATE SET T.[Name] = S.[Name];";

        Assert.Equal(expected, actual);
    }

    [Fact]
    public void MergeTableInsertOrUpdateWithCompareTest()
    {
        TableInfo tableInfo = GetTestTableWithCompareInfo();
        tableInfo.IdentityColumnName = "ItemId";
        string result = SqlQueryBuilder.MergeTable<Item>(null, tableInfo, OperationType.InsertOrUpdate).sql;

        string expected = "MERGE [dbo].[Item] WITH (HOLDLOCK) AS T " +
                          "USING (SELECT TOP 0 * FROM [dbo].[ItemTemp1234] ORDER BY [ItemId]) AS S " +
                          "ON T.[ItemId] = S.[ItemId] " +
                          "WHEN NOT MATCHED BY TARGET " +
                          "THEN INSERT ([Name], [TimeUpdated]) VALUES (S.[Name], S.[TimeUpdated]) " +
                          "WHEN MATCHED AND " +
                          "EXISTS (SELECT S.[Name]" +
                          " EXCEPT SELECT T.[Name]) " +
                          "THEN UPDATE SET T.[Name] = S.[Name], T.[TimeUpdated] = S.[TimeUpdated];";

        Assert.Equal(result, expected);
    }
    
    [Fact]
    public void MergeTableInsertOrUpdateWithCompareAndOnConflictUpdateWhereSqlTest()
    {
        TableInfo tableInfo = GetTestTableWithCompareInfo((existing, inserted) => $"{inserted}.ItemTimestamp > {existing}.ItemTimestamp");
        tableInfo.IdentityColumnName = "ItemId";
        string actual = SqlQueryBuilder.MergeTable<Item>(null, tableInfo, OperationType.InsertOrUpdate).sql;

        string expected = "MERGE [dbo].[Item] WITH (HOLDLOCK) AS T " +
                          "USING (SELECT TOP 0 * FROM [dbo].[ItemTemp1234] ORDER BY [ItemId]) AS S " +
                          "ON T.[ItemId] = S.[ItemId] " +
                          "WHEN NOT MATCHED BY TARGET THEN INSERT ([Name], [TimeUpdated]) VALUES (S.[Name], S.[TimeUpdated]) " +
                          "WHEN MATCHED AND " +
                          "EXISTS (SELECT S.[Name]" +
                          " EXCEPT SELECT T.[Name]) " +
                          "AND S.ItemTimestamp > T.ItemTimestamp " +
                          "THEN UPDATE SET T.[Name] = S.[Name], T.[TimeUpdated] = S.[TimeUpdated];";

        Assert.Equal(expected, actual);
    }
    
    [Fact]
    public void MergeTableInsertOrUpdateNoUpdateTest()
    {
        TableInfo tableInfo = GetTestTableWithNoUpdateInfo();
        tableInfo.IdentityColumnName = "ItemId";
        string result = SqlQueryBuilder.MergeTable<Item>(null, tableInfo, OperationType.InsertOrUpdate).sql;

        string expected = "MERGE [dbo].[Item] WITH (HOLDLOCK) AS T " +
                          "USING (SELECT TOP 0 * FROM [dbo].[ItemTemp1234] ORDER BY [ItemId]) AS S " +
                          "ON T.[ItemId] = S.[ItemId] " +
                          "WHEN NOT MATCHED BY TARGET " +
                          "THEN INSERT ([Name], [TimeUpdated]) VALUES (S.[Name], S.[TimeUpdated]) " +
                          "WHEN MATCHED AND " +
                          "EXISTS (SELECT S.[Name], S.[TimeUpdated]" +
                          " EXCEPT SELECT T.[Name], T.[TimeUpdated]) " +
                          "THEN UPDATE SET T.[Name] = S.[Name];";

        Assert.Equal(result, expected);
    }
    
    [Fact]
    public void MergeTableInsertOrUpdateNoUpdateWithOnConflictUpdateWhereSqlTest()
    {
        TableInfo tableInfo = GetTestTableWithNoUpdateInfo((existing, inserted) => $"{inserted}.ItemTimestamp > {existing}.ItemTimestamp");
        tableInfo.IdentityColumnName = "ItemId";
        string actual = SqlQueryBuilder.MergeTable<Item>(null, tableInfo, OperationType.InsertOrUpdate).sql;

        string expected = "MERGE [dbo].[Item] WITH (HOLDLOCK) AS T " +
                          "USING (SELECT TOP 0 * FROM [dbo].[ItemTemp1234] ORDER BY [ItemId]) AS S " +
                          "ON T.[ItemId] = S.[ItemId] " +
                          "WHEN NOT MATCHED BY TARGET THEN INSERT ([Name], [TimeUpdated]) VALUES (S.[Name], S.[TimeUpdated]) " +
                          "WHEN MATCHED AND " +
                          "EXISTS (SELECT S.[Name], S.[TimeUpdated]" +
                          " EXCEPT SELECT T.[Name], T.[TimeUpdated]) " +
                          "AND S.ItemTimestamp > T.ItemTimestamp " +
                          "THEN UPDATE SET T.[Name] = S.[Name];";

        Assert.Equal(expected, actual);
    }

    [Fact]
    public void MergeTableUpdateTest()
    {
        TableInfo tableInfo = GetTestTableInfo();
        tableInfo.IdentityColumnName = "ItemId";
        string result = SqlQueryBuilder.MergeTable<Item>(null, tableInfo, OperationType.Update).sql;

        string expected = "MERGE [dbo].[Item] WITH (HOLDLOCK) AS T USING " +
                          "(SELECT TOP 0 * FROM [dbo].[ItemTemp1234] ORDER BY [ItemId]) AS S " +
                          "ON T.[ItemId] = S.[ItemId] " +
                          "WHEN MATCHED AND " +
                          "EXISTS (SELECT S.[Name]" +
                          " EXCEPT SELECT T.[Name]) " +
                          "THEN UPDATE SET T.[Name] = S.[Name];";

        Assert.Equal(result, expected);
    }
    
    [Fact]
    public void MergeTableUpdateWithOnConflictUpdateWhereSqlTest()
    {
        TableInfo tableInfo = GetTestTableInfo((existing, inserted) => $"{inserted}.ItemTimestamp > {existing}.ItemTimestamp");
        tableInfo.IdentityColumnName = "ItemId";
        string actual = SqlQueryBuilder.MergeTable<Item>(null, tableInfo, OperationType.Update).sql;

        string expected = "MERGE [dbo].[Item] WITH (HOLDLOCK) AS T USING " +
                          "(SELECT TOP 0 * FROM [dbo].[ItemTemp1234] ORDER BY [ItemId]) AS S " +
                          "ON T.[ItemId] = S.[ItemId] " +
                          "WHEN MATCHED AND " +
                          "EXISTS (SELECT S.[Name]" +
                          " EXCEPT SELECT T.[Name]) " +
                          "AND S.ItemTimestamp > T.ItemTimestamp " +
                          "THEN UPDATE SET T.[Name] = S.[Name];";

        Assert.Equal(expected, actual);
    }

    [Fact]
    public void SelectJoinTableReadTest()
    {
        TableInfo tableInfo = GetTestTableInfo();
        tableInfo.BulkConfig.UpdateByProperties = new List<string> {nameof(Item.Name)};
        string result = SqlQueryBuilder.SelectJoinTable(tableInfo);

        string expected = "SELECT S.[ItemId], S.[Name] FROM [dbo].[Item] AS S " +
                          "JOIN [dbo].[ItemTemp1234] AS J " +
                          "ON S.[ItemId] = J.[ItemId]";

        Assert.Equal(result, expected);
    }

    [Fact]
    public void MergeTableDeleteDeleteTest()
    {
        var tableInfo = GetTestTableInfo();
        string result = SqlQueryBuilder.MergeTable<Item>(null, tableInfo, OperationType.Delete).sql;

        string expected = "MERGE [dbo].[Item] WITH (HOLDLOCK) AS T USING " +
                          "(SELECT TOP 0 * FROM [dbo].[ItemTemp1234] ORDER BY [ItemId]) AS S " +
                          "ON T.[ItemId] = S.[ItemId] " +
                          "WHEN MATCHED THEN DELETE;";

        Assert.Equal(result, expected);
    }

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

    private TableInfo GetTestTableWithCompareInfo(Func<string, string, string>? onConflictUpdateWhereSql = null)
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
        const string timeUpdatedText = nameof(Item.TimeUpdated);

        tableInfo.PropertyColumnNamesDict.Add(tableInfo.PrimaryKeysPropertyColumnNameDict.Keys.First(), tableInfo.PrimaryKeysPropertyColumnNameDict.Values.First());
        tableInfo.PropertyColumnNamesDict.Add(nameText, nameText);
        tableInfo.PropertyColumnNamesDict.Add(timeUpdatedText, timeUpdatedText);

        //do not update if only the TimeUpdated changed
        tableInfo.PropertyColumnNamesCompareDict =
            tableInfo.PropertyColumnNamesDict.Where(p => p.Key != timeUpdatedText).ToDictionary(p => p.Key, p => p.Value);

        //if an update id called, update all columns
        tableInfo.PropertyColumnNamesUpdateDict = tableInfo.PropertyColumnNamesDict;
        return tableInfo;
    }
    private TableInfo GetTestTableWithNoUpdateInfo(Func<string, string, string>? onConflictUpdateWhereSql = null)
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
        const string timeUpdatedText = nameof(Item.TimeUpdated);

        tableInfo.PropertyColumnNamesDict.Add(tableInfo.PrimaryKeysPropertyColumnNameDict.Keys.First(), tableInfo.PrimaryKeysPropertyColumnNameDict.Values.First());
        tableInfo.PropertyColumnNamesDict.Add(nameText, nameText);
        tableInfo.PropertyColumnNamesDict.Add(timeUpdatedText, timeUpdatedText);

        //update a row if any of the values are updated
        tableInfo.PropertyColumnNamesCompareDict = tableInfo.PropertyColumnNamesDict;

        //the TimeUpdated can be inserted but not updated.
        tableInfo.PropertyColumnNamesUpdateDict =
            tableInfo.PropertyColumnNamesDict.Where(p => p.Key != timeUpdatedText).ToDictionary(p => p.Key, p => p.Value);

        return tableInfo;
    }

    [Fact]
    public async Task DelegateDecompiler_DecompileAsync_WorksAsync()
    {
        using var context = new TestContext(ContextUtil.GetOptions());
#pragma warning disable
        await context.Items
            .Where(x => x.ItemId < 0)
            .DecompileAsync()
            .BatchDeleteAsync()
        ;
    }
}
