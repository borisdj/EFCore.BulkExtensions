using EFCore.BulkExtensions.SqlAdapters;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Storage;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

namespace EFCore.BulkExtensions.Tests;

public class EFCoreBulkTestAsync
{
    protected static int EntitiesNumber => 10000;

    private static readonly Func<TestContext, int> ItemsCountQuery = EF.CompileQuery<TestContext, int>(ctx => ctx.Items.Count());
    private static readonly Func<TestContext, Item?> LastItemQuery = EF.CompileQuery<TestContext, Item?>(ctx => ctx.Items.LastOrDefault());
    private static readonly Func<TestContext, IEnumerable<Item>> AllItemsQuery = EF.CompileQuery<TestContext, IEnumerable<Item>>(ctx => ctx.Items.AsNoTracking());

    [Theory]
    [InlineData(SqlType.SqlServer, true)]
    [InlineData(SqlType.Sqlite, true)]
    //[InlineData(DatabaseType.SqlServer, false)] // for speed comparison with Regular EF CUD operations
    public async Task OperationsTestAsync(SqlType sqlType, bool isBulk)
    {
        ContextUtil.DatabaseType = sqlType;

        //await DeletePreviousDatabaseAsync().ConfigureAwait(false);
        await new EFCoreBatchTestAsync().RunDeleteAllAsync(sqlType);

        // Test can be run individually by commenting others and running each separately in order one after another
        await RunInsertAsync(isBulk);
        await RunInsertOrUpdateAsync(isBulk, sqlType);
        await RunUpdateAsync(isBulk, sqlType);

        await RunReadAsync();

        if (sqlType == SqlType.SqlServer)
        {
            await RunInsertOrUpdateOrDeleteAsync(isBulk); // Not supported for Sqlite (has only UPSERT), instead use BulkRead, then split list into sublists and call separately Bulk methods for Insert, Update, Delete.
        }
        //await RunDeleteAsync(isBulk, sqlType);
    }

    [Theory]
    [InlineData(SqlType.SqlServer)]
    //[InlineData(DbServer.Sqlite)] // has to be run separately as single test, otherwise throws (SQLite Error 1: 'table "#MyTempTable1" already exists'.)
    public async Task SideEffectsTestAsync(SqlType sqlType)
    {
        await BulkOperationShouldNotCloseOpenConnectionAsync(sqlType, context => context.BulkInsertAsync(new[] { new Item() }), "1");
        await BulkOperationShouldNotCloseOpenConnectionAsync(sqlType, context => context.BulkUpdateAsync(new[] { new Item() }), "2");
    }

    private static async Task DeletePreviousDatabaseAsync()
    {
        using var context = new TestContext(ContextUtil.GetOptions());
        await context.Database.EnsureDeletedAsync().ConfigureAwait(false);
    }

    private static void WriteProgress(decimal percentage)
    {
        Debug.WriteLine(percentage);
    }

    private static async Task BulkOperationShouldNotCloseOpenConnectionAsync(SqlType sqlType, Func<TestContext, Task> bulkOperation, string tableSufix)
    {
        ContextUtil.DatabaseType = sqlType;
        using var context = new TestContext(ContextUtil.GetOptions());

        var sqlHelper = context.GetService<ISqlGenerationHelper>();
        await context.Database.OpenConnectionAsync();

        try
        {
            // we use a temp table to verify whether the connection has been closed (and re-opened) inside BulkUpdate(Async)
            var columnName = sqlHelper.DelimitIdentifier("Id");
            var tableName = sqlHelper.DelimitIdentifier("#MyTempTable" + tableSufix);
            var createTableSql = $" TABLE {tableName} ({columnName} INTEGER);";

            createTableSql = sqlType switch
            {
                SqlType.Sqlite => $"CREATE TEMPORARY {createTableSql}",
                SqlType.SqlServer => $"CREATE {createTableSql}",
                _ => throw new ArgumentException($"Unknown database type: '{sqlType}'.", nameof(sqlType)),
            };
            await context.Database.ExecuteSqlRawAsync(createTableSql);

            await bulkOperation(context);

            await context.Database.ExecuteSqlRawAsync($"SELECT {columnName} FROM {tableName}");
        }
        finally
        {
            await context.Database.CloseConnectionAsync();
        }
    }

    private static async Task RunInsertAsync(bool isBulk)
    {
        using var context = new TestContext(ContextUtil.GetOptions());

        var entities = new List<Item>();
        var subEntities = new List<ItemHistory>();
        for (int i = 1; i < EntitiesNumber; i++)
        {
            var entity = new Item
            {
                ItemId = isBulk ? i : 0,
                Name = "name " + i,
                Description = string.Concat("info ", Guid.NewGuid().ToString().AsSpan(0, 3)),
                Quantity = i % 10,
                Price = i / (i % 5 + 1),
                TimeUpdated = DateTime.Now,
                ItemHistories = new List<ItemHistory>()
            };

            var subEntity1 = new ItemHistory
            {
                ItemHistoryId = SeqGuid.Create(),
                Remark = $"some more info {i}.1"
            };
            var subEntity2 = new ItemHistory
            {
                ItemHistoryId = SeqGuid.Create(),
                Remark = $"some more info {i}.2"
            };
            entity.ItemHistories.Add(subEntity1);
            entity.ItemHistories.Add(subEntity2);

            entities.Add(entity);
        }

        if (isBulk)
        {
            if (ContextUtil.DatabaseType == SqlType.SqlServer)
            {
                using var transaction = await context.Database.BeginTransactionAsync();
                var bulkConfig = new BulkConfig
                {
                    //PreserveInsertOrder = true, // true is default
                    SetOutputIdentity = true,
                    BatchSize = 4000,
                    CalculateStats = true
                };
                await context.BulkInsertAsync(entities, bulkConfig, (a) => WriteProgress(a));
                Assert.Equal(EntitiesNumber - 1, bulkConfig.StatsInfo?.StatsNumberInserted);
                Assert.Equal(0, bulkConfig.StatsInfo?.StatsNumberUpdated);
                Assert.Equal(0, bulkConfig.StatsInfo?.StatsNumberDeleted);

                foreach (var entity in entities)
                {
                    foreach (var subEntity in entity.ItemHistories)
                    {
                        subEntity.ItemId = entity.ItemId; // setting FK to match its linked PK that was generated in DB
                    }
                    subEntities.AddRange(entity.ItemHistories);
                }

                await context.BulkInsertAsync(subEntities);

                await transaction.CommitAsync();
            }
            else if (ContextUtil.DatabaseType == SqlType.Sqlite)
            {
                using var transaction = await context.Database.BeginTransactionAsync();

                var bulkConfig = new BulkConfig()
                {
                    SetOutputIdentity = true,
                };
                await context.BulkInsertAsync(entities, bulkConfig);

                foreach (var entity in entities)
                {
                    foreach (var subEntity in entity.ItemHistories)
                    {
                        subEntity.ItemId = entity.ItemId; // setting FK to match its linked PK that was generated in DB
                    }
                    subEntities.AddRange(entity.ItemHistories);
                }
                await context.BulkInsertAsync(subEntities, bulkConfig);

                await transaction.CommitAsync();
            }
        }
        else
        {
            await context.Items.AddRangeAsync(entities);
            await context.SaveChangesAsync();
        }

        // TEST
        int entitiesCount = await context.Items.CountAsync(); // = ItemsCountQuery(context);
        Item? lastEntity = context.Items.OrderByDescending(a => a.ItemId).FirstOrDefault(); // = LastItemQuery(context);

        Assert.Equal(EntitiesNumber - 1, entitiesCount);
        Assert.NotNull(lastEntity);
        Assert.Equal("name " + (EntitiesNumber - 1), lastEntity?.Name);
    }

    private static async Task RunInsertOrUpdateAsync(bool isBulk, SqlType sqlType)
    {
        using var context = new TestContext(ContextUtil.GetOptions());
        var entities = new List<Item>();
        var dateTimeNow = DateTime.Now;
        var dateTimeOffsetNow = DateTimeOffset.UtcNow;
        for (int i = 2; i <= EntitiesNumber; i += 2)
        {
            entities.Add(new Item
            {
                ItemId = isBulk ? i : 0,
                Name = "name InsertOrUpdate " + i,
                Description = "info",
                Quantity = i + 100,
                Price = i / (i % 5 + 1),
                TimeUpdated = dateTimeNow,
            });
        }
        if (isBulk)
        {
            var bulkConfig = new BulkConfig()
            {
                SetOutputIdentity = true,
                CalculateStats = true,
                SqlBulkCopyOptions = Microsoft.Data.SqlClient.SqlBulkCopyOptions.KeepIdentity
            };
            await context.BulkInsertOrUpdateAsync(entities, bulkConfig);
            if (sqlType == SqlType.SqlServer)
            {
                Assert.Equal(1, bulkConfig.StatsInfo?.StatsNumberInserted);
                Assert.Equal(EntitiesNumber / 2 - 1, bulkConfig.StatsInfo?.StatsNumberUpdated);
                Assert.Equal(0, bulkConfig.StatsInfo?.StatsNumberDeleted);
            }
        }
        else
        {
            await context.Items.AddRangeAsync(entities);
            await context.SaveChangesAsync();
        }

        // TEST
        int entitiesCount = await context.Items.CountAsync();
        Item? lastEntity = context.Items.OrderByDescending(a => a.ItemId).FirstOrDefault();

        Assert.Equal(EntitiesNumber, entitiesCount);
        Assert.NotNull(lastEntity);
        Assert.Equal("name InsertOrUpdate " + EntitiesNumber, lastEntity?.Name);
    }

    private static async Task RunInsertOrUpdateOrDeleteAsync(bool isBulk)
    {
        using var context = new TestContext(ContextUtil.GetOptions());

        var entities = new List<Item>();
        var dateTimeNow = DateTime.Now;
        var dateTimeOffsetNow = DateTimeOffset.UtcNow;
        for (int i = 2; i <= EntitiesNumber; i += 2)
        {
            entities.Add(new Item
            {
                ItemId = i,
                Name = "name InsertOrUpdateOrDelete " + i,
                Description = "info",
                Quantity = i,
                Price = i / (i % 5 + 1),
                TimeUpdated = dateTimeNow
            });
        }

        int? keepEntityItemId = null;
        if (isBulk)
        {
            var bulkConfig = new BulkConfig() { SetOutputIdentity = true, CalculateStats = true };
            keepEntityItemId = 3;
            bulkConfig.SetSynchronizeFilter<Item>(e => e.ItemId != keepEntityItemId.Value);
            bulkConfig.OnConflictUpdateWhereSql = (existing, inserted) => $"{inserted}.{nameof(Item.TimeUpdated)} > {existing}.{nameof(Item.TimeUpdated)}"; // can use nameof bacause in this case property name is same as column name 
            await context.BulkInsertOrUpdateOrDeleteAsync(entities, bulkConfig);
            Assert.Equal(0, bulkConfig.StatsInfo?.StatsNumberInserted);
            Assert.Equal(EntitiesNumber / 2, bulkConfig.StatsInfo?.StatsNumberUpdated);
            Assert.Equal((EntitiesNumber / 2) -1, bulkConfig.StatsInfo?.StatsNumberDeleted);
        }
        else
        {
            var existingItems = context.Items;
            var removedItems = existingItems.Where(x => !entities.Any(y => y.ItemId == x.ItemId));
            context.Items.RemoveRange(removedItems);
            await context.Items.AddRangeAsync(entities);
            await context.SaveChangesAsync();
        }

        // TEST
        using var contextRead = new TestContext(ContextUtil.GetOptions());
        int entitiesCount = await contextRead.Items.CountAsync(); // = ItemsCountQuery(context);
        Item? firstEntity = contextRead.Items.OrderBy(a => a.ItemId).FirstOrDefault(); // = LastItemQuery(context);
        Item? lastEntity = contextRead.Items.OrderByDescending(a => a.ItemId).FirstOrDefault();

        Assert.Equal(EntitiesNumber / 2 + (keepEntityItemId != null ? 1 : 0), entitiesCount);
        Assert.NotNull(firstEntity);
        Assert.Equal("name InsertOrUpdateOrDelete 2", firstEntity?.Name);
        Assert.NotNull(lastEntity);
        Assert.Equal("name InsertOrUpdateOrDelete " + EntitiesNumber, lastEntity?.Name);

        var bulkConfigSoftDel = new BulkConfig();
        bulkConfigSoftDel.SetSynchronizeSoftDelete<Item>(a => new Item { Quantity = 0 }); // Instead of Deleting from DB it updates Quantity to 0 (usual usecase would be: IsDeleted to True)
        context.BulkInsertOrUpdateOrDelete(new List<Item> { entities[1] }, bulkConfigSoftDel);

        var list = await context.Items.ToListAsync();
        Assert.True(list.Single(x => x.ItemId == entities[1].ItemId).Quantity != 0);
        Assert.True(list.Where(x => x.ItemId != entities[1].ItemId).All(x => x.Quantity == 0));
    }

    private static async Task RunUpdateAsync(bool isBulk, SqlType sqlType)
    {
        using var context = new TestContext(ContextUtil.GetOptions());

        int counter = 1;
        var entities = AllItemsQuery(context).ToList();
        foreach (var entity in entities)
        {
            entity.Description = "Desc Update " + counter++;
            entity.TimeUpdated = DateTime.Now;
        }
        if (isBulk)
        {
            var bulkConfig = new BulkConfig() { SetOutputIdentity = true, CalculateStats = true };
            await context.BulkUpdateAsync(entities, bulkConfig);
            if (sqlType == SqlType.SqlServer)
            {
                Assert.Equal(0, bulkConfig.StatsInfo?.StatsNumberInserted);
                Assert.Equal(EntitiesNumber, bulkConfig.StatsInfo?.StatsNumberUpdated);
                Assert.Equal(0, bulkConfig.StatsInfo?.StatsNumberDeleted);
            }
        }
        else
        {
            context.Items.UpdateRange(entities);
            await context.SaveChangesAsync();
        }

        // TEST
        int entitiesCount = await context.Items.CountAsync();
        Item? lastEntity = context.Items.OrderByDescending(a => a.ItemId).FirstOrDefault();

        Assert.Equal(EntitiesNumber, entitiesCount);
        Assert.NotNull(lastEntity);
        Assert.Equal("Desc Update " + EntitiesNumber, lastEntity?.Description);
    }

    private static async Task RunReadAsync()
    {
        using var context = new TestContext(ContextUtil.GetOptions());

        var entities = new List<Item>();
        for (int i = 1; i < EntitiesNumber; i++)
        {
            entities.Add(new Item { Name = "name " + i });
        }

        var bulkConfig = new BulkConfig { UpdateByProperties = new List<string> { nameof(Item.Name) }};
        await context.BulkReadAsync(entities, bulkConfig).ConfigureAwait(false);

        Assert.Equal(1, entities[0].ItemId);
        Assert.Equal(0, entities[1].ItemId);
        Assert.Equal(3, entities[2].ItemId);
        Assert.Equal(0, entities[3].ItemId);
    }

    private async Task RunDeleteAsync(bool isBulk, SqlType sqlType)
    {
        using var context = new TestContext(ContextUtil.GetOptions());

        var entities = AllItemsQuery(context).ToList();
        // ItemHistories will also be deleted because of Relationship - ItemId (Delete Rule: Cascade)
        if (isBulk)
        {
            var bulkConfig = new BulkConfig() { CalculateStats = true };
            await context.BulkDeleteAsync(entities, bulkConfig);
            if (sqlType == SqlType.SqlServer)
            {
                Assert.Equal(0, bulkConfig.StatsInfo?.StatsNumberInserted);
                Assert.Equal(0, bulkConfig.StatsInfo?.StatsNumberUpdated);
                Assert.Equal(EntitiesNumber / 2, bulkConfig.StatsInfo?.StatsNumberDeleted);
            }
        }
        else
        {
            context.Items.RemoveRange(entities);
            await context.SaveChangesAsync();
        }

        // TEST
        int entitiesCount = await context.Items.CountAsync();
        Item? lastEntity = context.Items.OrderByDescending(a => a.ItemId).FirstOrDefault();

        Assert.Equal(0, entitiesCount);
        Assert.Null(lastEntity);

        // RESET AutoIncrement
        string deleteTableSql = sqlType switch
        {
            SqlType.SqlServer => $"DBCC CHECKIDENT('[dbo].[{nameof(Item)}]', RESEED, 0);",
            SqlType.Sqlite => $"DELETE FROM sqlite_sequence WHERE name = '{nameof(Item)}';",
            _ => throw new ArgumentException($"Unknown database type: '{sqlType}'.", nameof(sqlType)),
        };
        await context.Database.ExecuteSqlRawAsync(deleteTableSql).ConfigureAwait(false);
    }
}
