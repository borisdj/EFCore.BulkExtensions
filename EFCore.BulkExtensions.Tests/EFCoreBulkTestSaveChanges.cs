using EFCore.BulkExtensions.SqlAdapters;
using Microsoft.EntityFrameworkCore;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

namespace EFCore.BulkExtensions.Tests;

public class EFCoreBulkTestSaveChanges
{
    [Theory]
    [InlineData(DatabaseType.SqlServer)]
    [InlineData(DatabaseType.PostgreSql)]
    [InlineData(DatabaseType.Sqlite)]
    public void SaveChangesTest(DatabaseType dbServer)
    {
        ContextUtil.DatabaseType = dbServer;

        new EFCoreBatchTest().RunDeleteAll(dbServer);
        using (var context = new TestContext(ContextUtil.GetOptions()))
        {
            context.ItemHistories.BatchDelete();
        }

        RunSaveChangesOnInsert(dbServer);
        RunSaveChangesOnInsertAndUpdate(dbServer);
    }

    [Theory]
    [InlineData(DatabaseType.SqlServer)]
    [InlineData(DatabaseType.PostgreSql)]
    [InlineData(DatabaseType.Sqlite)]
    public async Task SaveChangesTestAsync(DatabaseType dbServer)
    {
        ContextUtil.DatabaseType = dbServer;

        //await new EFCoreBatchTestAsync().RunDeleteAllAsync(dbServer);
        using (var context = new TestContext(ContextUtil.GetOptions()))
        {
            await context.ItemHistories.BatchDeleteAsync();

            context.Database.ExecuteSqlRaw($@"DELETE FROM ""{nameof(Item)}""");
            context.Database.ExecuteSqlRaw($@"ALTER SEQUENCE ""{nameof(Item)}_{nameof(Item.ItemId)}_seq"" RESTART WITH 1");
        }

        await RunSaveChangesOnInsertAsync(dbServer);
        await RunSaveChangesOnInsertAndUpdateAsync(dbServer);
    }

    private static List<Item> GetNewEntities(DatabaseType dbServer, int count, string NameSufix)
    {
        var newEntities = new List<Item>();
        var dateTimeNow = dbServer == DatabaseType.PostgreSql ? DateTime.UtcNow : DateTime.Now;

        for (int i = 1; i <= count; i += 1) // Insert 4000 new ones
        {
            newEntities.Add(new Item
            {
                //ItemId = i,
                Name = "Name " + NameSufix + i,
                Description = "info",
                Quantity = i + 100,
                Price = i / (i % 5 + 1),
                TimeUpdated = dateTimeNow,
                ItemHistories = new List<ItemHistory>()
                    {
                        new ItemHistory
                        {
                            //ItemId = i,
                            ItemHistoryId = SeqGuid.Create(),
                            Remark = $"some more info {i}.1"
                        },
                        new ItemHistory
                        {
                            //ItemId = i,
                            ItemHistoryId = SeqGuid.Create(),
                            Remark = $"some more info {i}.2"
                        }
                    }
            });
        }
        return newEntities;
    }

    private static void RunSaveChangesOnInsert(DatabaseType dbServer)
    {
        using var context = new TestContext(ContextUtil.GetOptions());

        var newEntities = GetNewEntities(dbServer, 5000, "");

        using var transaction = context.Database.BeginTransaction();

        context.Items.AddRange(newEntities);
        context.BulkSaveChanges();

        transaction.Commit();

        // Validate Test
        int entitiesCount = context.Items.Count();
        Item? firstEntity = context.Items.SingleOrDefault(a => a.ItemId == 1);

        Assert.Equal(5000, entitiesCount);
        Assert.Equal("Name 1", firstEntity?.Name);
    }

    private static async Task RunSaveChangesOnInsertAsync(DatabaseType dbServer)
    {
        using var context = new TestContext(ContextUtil.GetOptions());

        var newEntities = GetNewEntities(dbServer, 5000, "");

        await context.Items.AddRangeAsync(newEntities);
        await context.BulkSaveChangesAsync();

        // Validate Test
        int entitiesCount = await context.Items.CountAsync();
        Item? firstEntity = await context.Items.SingleOrDefaultAsync(a => a.ItemId == 1);

        Assert.Equal(5000, entitiesCount);
        Assert.Equal("Name 1", firstEntity?.Name);
    }

    private static void RunSaveChangesOnInsertAndUpdate(DatabaseType dbServer)
    {
        using var context = new TestContext(ContextUtil.GetOptions());

        var loadedEntites = context.Items.Include(a => a.ItemHistories).Where(a => a.ItemId <= 3000).ToList(); // load first 3000 entities
        var existingEntites = loadedEntites.Where(a => a.ItemId <= 2000).ToList(); // take first 2000 of loaded entities and update them
        foreach (var existingEntity in existingEntites)
        {
            existingEntity.Description += " UPDATED";
            existingEntity.ItemHistories.First().Remark += " UPD";
        }

        var newEntities = GetNewEntities(dbServer, 4000, "NEW ");

        context.Items.AddRange(newEntities);
        context.BulkSaveChanges();

        // Validate Test
        int entitiesCount = context.Items.Count();
        Item? firstEntity = context.Items.SingleOrDefault(a => a.ItemId == 1);

        Assert.Equal(9000, entitiesCount);
        Assert.EndsWith(" UPDATED", firstEntity?.Description);
    }

    private static async Task RunSaveChangesOnInsertAndUpdateAsync(DatabaseType dbServer)
    {
        using var context = new TestContext(ContextUtil.GetOptions());

        var loadedEntites = await context.Items.Include(a => a.ItemHistories).Where(a => a.ItemId <= 3000).ToListAsync(); // load first 3000 entities
        var existingEntites = loadedEntites.Where(a => a.ItemId <= 2000).ToList(); // take first 2000 of loaded entities and update them
        foreach (var existingEntity in existingEntites)
        {
            existingEntity.Description += " UPDATED";
            existingEntity.ItemHistories.First().Remark += " UPD";
        }

        var newEntities = GetNewEntities(dbServer, 4000, "NEW ");

        await context.Items.AddRangeAsync(newEntities);
        await context.BulkSaveChangesAsync();

        // Validate Test
        int entitiesCount = await context.Items.CountAsync();
        Item? firstEntity = await context.Items.SingleOrDefaultAsync(a => a.ItemId == 1);

        Assert.Equal(9000, entitiesCount);
        Assert.EndsWith(" UPDATED", firstEntity?.Description);
    }
}
