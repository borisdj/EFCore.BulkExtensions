using EFCore.BulkExtensions.SqlAdapters;
using Microsoft.EntityFrameworkCore;
using System;
using System.Collections.Generic;
using System.Linq;
using Xunit;

namespace EFCore.BulkExtensions.Tests;

public class EFCoreBulkTestOracle
{
    protected static int EntitiesNumber => 100000;

    private static readonly Func<TestContext, int> ItemsCountQuery = EF.CompileQuery<TestContext, int>(ctx => ctx.Items.Count());
    private static readonly Func<TestContext, Item?> LastItemQuery = EF.CompileQuery<TestContext, Item?>(ctx => ctx.Items.LastOrDefault());
    private static readonly Func<TestContext, IEnumerable<Item>> AllItemsQuery = EF.CompileQuery<TestContext, IEnumerable<Item>>(ctx => ctx.Items.AsNoTracking());

    [Theory]
    [InlineData(SqlType.Oracle)]
    public void BulkInsert(SqlType sqlType)
    {
        ContextUtil.DatabaseType = sqlType;

        using var context = new OracleTestContext(ContextUtil.GetOptions<OracleTestContext>());
        _ = context.Database.ExecuteSqlRaw($@"DELETE FROM {nameof(Item)}");

        var items = new List<Item>();
        for (int i = 1; i <= EntitiesNumber; i++)
        {
            items.Add(new Item
            {
                ItemId = i,
                Name = "BulkInsert " + i,
                Description = "BulkInsert Description " + i,
                Price = 0.1m * i,
                Quantity = i,
                TimeUpdated = DateTime.UtcNow,
                Category = new ItemCategory { Id = i, Name = "Some " + i }
            });
        }
        context.BulkInsert(items);

        var result = context.Items.AsNoTracking().First(x => x.ItemId == items[0].ItemId);

        Assert.True(result.Name == items[0].Name);
    }
    [Theory]
    [InlineData(SqlType.Oracle)]
    public void BulkUpdate(SqlType sqlType)
    {
        ContextUtil.DatabaseType = sqlType;

        using var context = new OracleTestContext(ContextUtil.GetOptions<OracleTestContext>());

        var items = new List<Item>();
        for (int i = 1; i <= 100; i++)
        {
            items.Add(new Item
            {
                ItemId = i,
                Name = "BulkUpdate " + i,
                Description = "BulkUpdate Description " + i,
                Price = 0.1m * i,
                Quantity = i,
                TimeUpdated = DateTime.UtcNow,
                Category = new ItemCategory { Id = i, Name = "Some " + i }
            });
        }
        context.BulkUpdate(items);

        var result = context.Items.AsNoTracking().First(x => x.ItemId == items[0].ItemId);

        Assert.True(result.Name == items[0].Name);
    }
    [Theory]
    [InlineData(SqlType.Oracle)]
    public void BulkInsertOrUpdate(SqlType sqlType)
    {
        ContextUtil.DatabaseType = sqlType;

        using var context = new OracleTestContext(ContextUtil.GetOptions<OracleTestContext>());

        var items = new List<Item>();
        for (int i = 1; i <= 10; i++)
        {
            items.Add(new Item
            {
                ItemId = i,
                Name = "BulkInsertOrUpdate" + i,
                Description = "BulkInsertOrUpdate Description " + i,
                Price = 0.1m * i,
                Quantity = i,
                TimeUpdated = DateTime.UtcNow,
                Category = new ItemCategory { Id = i, Name = "Some " + i }
            });
        }
        for (int i = EntitiesNumber; i <= (EntitiesNumber + 10); i++)
        {
            items.Add(new Item
            {
                ItemId = i,
                Name = "BulkInsertOrUpdate " + i,
                Description = "BulkInsertOrUpdate Description " + i,
                Price = 0.1m * i,
                Quantity = i,
                TimeUpdated = DateTime.UtcNow,
                Category = new ItemCategory { Id = i, Name = "Some " + i }
            });
        }
        context.BulkInsertOrUpdate(items);

        var result = context.Items.AsNoTracking().First(x => x.ItemId == items[0].ItemId);

        Assert.True(result.Name == items[0].Name);
    }
    [Theory]
    [InlineData(SqlType.Oracle)]
    public void BulkDelete(SqlType sqlType)
    {
        ContextUtil.DatabaseType = sqlType;

        using var context = new OracleTestContext(ContextUtil.GetOptions<OracleTestContext>());

        var items = new List<Item>();
        for (int i = 21; i <= 30; i++)
        {
            items.Add(new Item
            {
                ItemId = i,
                Name = "BulkDelete " + i,
                Description = "BulkDelete Description " + i,
                Price = 0.1m * i,
                Quantity = i,
                TimeUpdated = DateTime.UtcNow,
                Category = new ItemCategory { Id = i, Name = "Some " + i }
            });
        }
        context.BulkDelete(items);

        var result = context.Items.AsNoTracking().FirstOrDefault(x => x.ItemId == items[0].ItemId);

        Assert.True(result == null);
    }
}
