using EFCore.BulkExtensions.SqlAdapters.Sqlite;
using System;
using System.Collections.Generic;
using System.Linq;
using Xunit;

namespace EFCore.BulkExtensions.Tests;

public class SqlQueryBuilderSqliteTests
{
    [Fact]
    public void MergeTableInsertOrUpdateWithoutOnConflictUpdateWhereSqlTest()
    {
        TableInfo tableInfo = GetTestTableInfo();
        tableInfo.IdentityColumnName = "ItemId";
        string actual = SqliteQueryBuilder.InsertIntoTable(tableInfo, OperationType.InsertOrUpdate);

        string expected = @"INSERT INTO [Item] ([ItemId], [Name]) " +
                          @"VALUES (@ItemId, @Name) " +
                          @"ON CONFLICT([ItemId]) DO UPDATE SET [ItemId] = @ItemId, [Name] = @Name " +
                          @"WHERE [ItemId] = @ItemId;";

        Assert.Equal(expected, actual);
    }
    
    [Fact]
    public void MergeTableInsertOrUpdateWithOnConflictUpdateWhereSqlTest()
    {
        TableInfo tableInfo = GetTestTableInfo((existing, inserted) => $"{inserted}.ItemTimestamp > {existing}.ItemTimestamp");
        tableInfo.IdentityColumnName = "ItemId";
        string actual = SqliteQueryBuilder.InsertIntoTable(tableInfo, OperationType.InsertOrUpdate);

        string expected = @"INSERT INTO [Item] ([ItemId], [Name]) " +
                          @"VALUES (@ItemId, @Name) " +
                          @"ON CONFLICT([ItemId]) DO UPDATE SET [ItemId] = @ItemId, [Name] = @Name " +
                          @"WHERE [ItemId] = @ItemId AND excluded.ItemTimestamp > [Item].ItemTimestamp;";

        Assert.Equal(expected, actual);
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
}
