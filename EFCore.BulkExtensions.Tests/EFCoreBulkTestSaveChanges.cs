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
    [InlineData(DbServer.SQLServer)]
    [InlineData(DbServer.SQLite)]
    public void SaveChangesTest(DbServer dbServer)
    {
        ContextUtil.DbServer = dbServer;

        new EFCoreBatchTest().RunDeleteAll(dbServer);
        using (var context = new TestContext(ContextUtil.GetOptions()))
        {
            context.ItemHistories.BatchDelete();
        }

        RunSaveChangesOnInsert();
        RunSaveChangesOnInsertAndUpdate();
    }

    [Theory]
    [InlineData(DbServer.SQLServer)]
    [InlineData(DbServer.SQLite)]
    public async Task SaveChangesTestAsync(DbServer dbServer)
    {
        ContextUtil.DbServer = dbServer;

        await new EFCoreBatchTestAsync().RunDeleteAllAsync(dbServer);
        using (var context = new TestContext(ContextUtil.GetOptions()))
        {
            await context.ItemHistories.BatchDeleteAsync();
        }

        await RunSaveChangesOnInsertAsync();
        await RunSaveChangesOnInsertAndUpdateAsync();
    }

    private static List<Item> GetNewEntities(int count, string NameSufix)
    {
        var newEntities = new List<Item>();
        var dateTimeNow = DateTime.Now;
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

    private void RunSaveChangesOnInsert()
    {
        using var context = new TestContext(ContextUtil.GetOptions());

        var newEntities = GetNewEntities(5000, "");

        context.Items.AddRange(newEntities);
        context.BulkSaveChanges();

        // Validate Test
        int entitiesCount = context.Items.Count();
        Item firstEntity = context.Items.SingleOrDefault(a => a.ItemId == 1);

        Assert.Equal(5000, entitiesCount);
        Assert.Equal("Name 1", firstEntity.Name);
    }

    private async Task RunSaveChangesOnInsertAsync()
    {
        using var context = new TestContext(ContextUtil.GetOptions());

        var newEntities = GetNewEntities(5000, "");

        await context.Items.AddRangeAsync(newEntities);
        await context.BulkSaveChangesAsync();

        // Validate Test
        int entitiesCount = await context.Items.CountAsync();
        Item firstEntity = await context.Items.SingleOrDefaultAsync(a => a.ItemId == 1);

        Assert.Equal(5000, entitiesCount);
        Assert.Equal("Name 1", firstEntity.Name);
    }

    private void RunSaveChangesOnInsertAndUpdate()
    {
        using var context = new TestContext(ContextUtil.GetOptions());

        var loadedEntites = context.Items.Include(a => a.ItemHistories).Where(a => a.ItemId <= 3000).ToList(); // load first 3000 entities
        var existingEntites = loadedEntites.Where(a => a.ItemId <= 2000).ToList(); // take first 2000 of loaded entities and update them
        foreach (var existingEntity in existingEntites)
        {
            existingEntity.Description += " UPDATED";
            existingEntity.ItemHistories.FirstOrDefault().Remark += " UPD";
        }

        var newEntities = GetNewEntities(4000, "NEW ");

        context.Items.AddRange(newEntities);
        context.BulkSaveChanges();

        // Validate Test
        int entitiesCount = context.Items.Count();
        Item firstEntity = context.Items.SingleOrDefault(a => a.ItemId == 1);

        Assert.Equal(9000, entitiesCount);
        Assert.EndsWith(" UPDATED", firstEntity.Description);
    }

    private async Task RunSaveChangesOnInsertAndUpdateAsync()
    {
        using var context = new TestContext(ContextUtil.GetOptions());

        var loadedEntites = await context.Items.Include(a => a.ItemHistories).Where(a => a.ItemId <= 3000).ToListAsync(); // load first 3000 entities
        var existingEntites = loadedEntites.Where(a => a.ItemId <= 2000).ToList(); // take first 2000 of loaded entities and update them
        foreach (var existingEntity in existingEntites)
        {
            existingEntity.Description += " UPDATED";
            existingEntity.ItemHistories.FirstOrDefault().Remark += " UPD";
        }

        var newEntities = GetNewEntities(4000, "NEW ");

        await context.Items.AddRangeAsync(newEntities);
        await context.BulkSaveChangesAsync();

        // Validate Test
        int entitiesCount = await context.Items.CountAsync();
        Item firstEntity = await context.Items.SingleOrDefaultAsync(a => a.ItemId == 1);

        Assert.Equal(9000, entitiesCount);
        Assert.EndsWith(" UPDATED", firstEntity.Description);
    }
}
