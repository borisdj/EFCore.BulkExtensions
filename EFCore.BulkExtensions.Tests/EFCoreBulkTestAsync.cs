using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using EFCore.BulkExtensions.SqlAdapters;
using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Storage;
using Xunit;

namespace EFCore.BulkExtensions.Tests
{
    public class EFCoreBulkTestAsync
    {
        protected int EntitiesNumber => 10000;

        private static Func<TestContext, int> ItemsCountQuery = EF.CompileQuery<TestContext, int>(ctx => ctx.Items.Count());
        private static Func<TestContext, Item> LastItemQuery = EF.CompileQuery<TestContext, Item>(ctx => ctx.Items.LastOrDefault());
        private static Func<TestContext, IEnumerable<Item>> AllItemsQuery = EF.CompileQuery<TestContext, IEnumerable<Item>>(ctx => ctx.Items.AsNoTracking());

        [Theory]
        [InlineData(DbServer.SqlServer, true)]
        [InlineData(DbServer.Sqlite, true)]
        //[InlineData(DatabaseType.SqlServer, false)] // for speed comparison with Regular EF CUD operations
        public async Task OperationsTestAsync(DbServer dbServer, bool isBulk)
        {
            ContextUtil.DbServer = dbServer;

            await new EFCoreBatchTestAsync().RunDeleteAllAsync(dbServer); // TODO

            // Test can be run individually by commenting others and running each separately in order one after another
            await RunInsertAsync(isBulk);
            await RunInsertOrUpdateAsync(isBulk, dbServer);
            await RunUpdateAsync(isBulk, dbServer);
            if (dbServer == DbServer.SqlServer)
            {
                await RunReadAsync(isBulk); // Not Yet supported for Sqlite
                await RunInsertOrUpdateOrDeleteAsync(isBulk); // Not Yet supported for Sqlite
            }
            await RunDeleteAsync(isBulk, dbServer);
        }

        [Theory]
        [InlineData(DbServer.SqlServer)]
        [InlineData(DbServer.Sqlite)]
        public async Task SideEffectsTestAsync(DbServer dbServer)
        {
            await BulkOperationShouldNotCloseOpenConnectionAsync(dbServer, context => context.BulkInsertAsync(new[] { new Item() }));
            await BulkOperationShouldNotCloseOpenConnectionAsync(dbServer, context => context.BulkUpdateAsync(new[] { new Item() }));
        }

        private static async Task BulkOperationShouldNotCloseOpenConnectionAsync(
            DbServer dbServer,
            Func<TestContext, Task> bulkOperation)
        {
            ContextUtil.DbServer = dbServer;

            using (var context = new TestContext(ContextUtil.GetOptions()))
            {
                var sqlHelper = context.GetService<ISqlGenerationHelper>();
                await context.Database.OpenConnectionAsync();

                try
                {
                    // we use a temp table to verify whether the connection has been closed (and re-opened) inside BulkUpdate(Async)
                    var columnName = sqlHelper.DelimitIdentifier("Id");
                    var tableName = sqlHelper.DelimitIdentifier("#MyTempTable");
                    var createTableSql = $" TABLE {tableName} ({columnName} INTEGER);";

                    switch (dbServer)
                    {
                        case DbServer.Sqlite:
                            createTableSql = $"CREATE TEMPORARY {createTableSql}";
                            break;

                        case DbServer.SqlServer:
                            createTableSql = $"CREATE {createTableSql}";
                            break;

                        default:
                            throw new ArgumentException($"Unknown database type: '{dbServer}'.", nameof(dbServer));
                    }

                    await context.Database.ExecuteSqlRawAsync(createTableSql);

                    await bulkOperation(context);

                    await context.Database.ExecuteSqlRawAsync($"SELECT {columnName} FROM {tableName}");
                }
                finally
                {
                    await context.Database.CloseConnectionAsync();
                }
            }
        }

        private async Task RunInsertAsync(bool isBulk)
        {
            using (var context = new TestContext(ContextUtil.GetOptions()))
            {
                var entities = new List<Item>();
                var subEntities = new List<ItemHistory>();
                for (int i = 1; i < EntitiesNumber; i++)
                {
                    var entity = new Item
                    {
                        ItemId = isBulk ? i : 0,
                        Name = "name " + i,
                        Description = "info " + Guid.NewGuid().ToString().Substring(0, 3),
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
                    if (ContextUtil.DbServer == DbServer.SqlServer)
                    {
                        using (var transaction = await context.Database.BeginTransactionAsync())
                        {
                            var bulkConfig = new BulkConfig
                            {
                                //PreserveInsertOrder = true, // true is default
                                SetOutputIdentity = true,
                                BatchSize = 4000,
                                CalculateStats = true
                            };
                            await context.BulkInsertAsync(entities, bulkConfig);
                            Assert.Equal(EntitiesNumber - 1, bulkConfig.StatsInfo.StatsNumberInserted);
                            Assert.Equal(0, bulkConfig.StatsInfo.StatsNumberUpdated);
                            Assert.Equal(0, bulkConfig.StatsInfo.StatsNumberDeleted);

                            foreach (var entity in entities)
                            {
                                foreach (var subEntity in entity.ItemHistories)
                                {
                                    subEntity.ItemId = entity.ItemId; // setting FK to match its linked PK that was generated in DB
                                }
                                subEntities.AddRange(entity.ItemHistories);
                            }

                            await context.BulkInsertAsync(subEntities);

                            transaction.Commit();
                        }
                    }
                    else if (ContextUtil.DbServer == DbServer.Sqlite)
                    {
                        using (var transaction = context.Database.BeginTransaction())
                        {
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

                            transaction.Commit();
                        }
                    }
                }
                else
                {
                    await context.Items.AddRangeAsync(entities);
                    await context.SaveChangesAsync();
                }
            }
            using (var context = new TestContext(ContextUtil.GetOptions()))
            {
                //int entitiesCount = ItemsCountQuery(context);
                int entitiesCount = await context.Items.CountAsync();
                //Item lastEntity = LastItemQuery(context);
                Item lastEntity = context.Items.OrderByDescending(a => a.ItemId).FirstOrDefault();

                Assert.Equal(EntitiesNumber - 1, entitiesCount);
                Assert.NotNull(lastEntity);
                Assert.Equal("name " + (EntitiesNumber - 1), lastEntity.Name);
            }
        }

        private async Task RunInsertOrUpdateAsync(bool isBulk, DbServer dbServer)
        {
            using (var context = new TestContext(ContextUtil.GetOptions()))
            {
                var entities = new List<Item>();
                var dateTimeNow = DateTime.Now;
                for (int i = 2; i <= EntitiesNumber; i += 2)
                {
                    entities.Add(new Item
                    {
                        ItemId = i,
                        Name = "name InsertOrUpdate " + i,
                        Description = "info",
                        Quantity = i,
                        Price = i / (i % 5 + 1),
                        TimeUpdated = dateTimeNow
                    });
                }
                if (isBulk)
                {
                    var bulkConfig = new BulkConfig() { SetOutputIdentity = true, CalculateStats = true };
                    await context.BulkInsertOrUpdateAsync(entities, bulkConfig);
                    if (dbServer == DbServer.SqlServer)
                    {
                        Assert.Equal(1, bulkConfig.StatsInfo.StatsNumberInserted);
                        Assert.Equal(EntitiesNumber / 2 - 1, bulkConfig.StatsInfo.StatsNumberUpdated);
                        Assert.Equal(0, bulkConfig.StatsInfo.StatsNumberDeleted);
                    }
                }
                else
                {
                    await context.Items.AddRangeAsync(entities);
                    await context.SaveChangesAsync();
                }
            }
            using (var context = new TestContext(ContextUtil.GetOptions()))
            {
                //int entitiesCount = ItemsCountQuery(context);
                int entitiesCount = await context.Items.CountAsync();
                //Item lastEntity = LastItemQuery(context);
                Item lastEntity = context.Items.OrderByDescending(a => a.ItemId).FirstOrDefault();

                Assert.Equal(EntitiesNumber, entitiesCount);
                Assert.NotNull(lastEntity);
                Assert.Equal("name InsertOrUpdate " + EntitiesNumber, lastEntity.Name);
            }
        }

        private async Task RunInsertOrUpdateOrDeleteAsync(bool isBulk)
        {
            using (var context = new TestContext(ContextUtil.GetOptions()))
            {
                var entities = new List<Item>();
                var dateTimeNow = DateTime.Now;
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
                if (isBulk)
                {
                    var bulkConfig = new BulkConfig() { SetOutputIdentity = true, CalculateStats = true };
                    await context.BulkInsertOrUpdateOrDeleteAsync(entities, bulkConfig);
                    Assert.Equal(0, bulkConfig.StatsInfo.StatsNumberInserted);
                    Assert.Equal(EntitiesNumber / 2, bulkConfig.StatsInfo.StatsNumberUpdated);
                    Assert.Equal(EntitiesNumber / 2, bulkConfig.StatsInfo.StatsNumberDeleted);
                }
                else
                {
                    var existingItems = context.Items;
                    var removedItems = existingItems.Where(x => !entities.Any(y => y.ItemId == x.ItemId));
                    context.Items.RemoveRange(removedItems);
                    await context.Items.AddRangeAsync(entities);
                    await context.SaveChangesAsync();
                }
            }
            using (var context = new TestContext(ContextUtil.GetOptions()))
            {
                //int entitiesCount = ItemsCountQuery(context);
                int entitiesCount = await context.Items.CountAsync();
                //Item lastEntity = LastItemQuery(context);
                Item firstEntity = context.Items.OrderBy(a => a.ItemId).FirstOrDefault();
                Item lastEntity = context.Items.OrderByDescending(a => a.ItemId).FirstOrDefault();

                Assert.Equal(EntitiesNumber / 2, entitiesCount);
                Assert.NotNull(firstEntity);
                Assert.Equal("name InsertOrUpdateOrDelete 2", firstEntity.Name);
                Assert.NotNull(lastEntity);
                Assert.Equal("name InsertOrUpdateOrDelete " + EntitiesNumber, lastEntity.Name);
            }
        }

        private async Task RunUpdateAsync(bool isBulk, DbServer dbServer)
        {
            using (var context = new TestContext(ContextUtil.GetOptions()))
            {
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
                    if (dbServer == DbServer.SqlServer)
                    {
                        Assert.Equal(0, bulkConfig.StatsInfo.StatsNumberInserted);
                        Assert.Equal(EntitiesNumber, bulkConfig.StatsInfo.StatsNumberUpdated);
                        Assert.Equal(0, bulkConfig.StatsInfo.StatsNumberDeleted);
                    }
                }
                else
                {
                    context.Items.UpdateRange(entities);
                    await context.SaveChangesAsync();
                }
            }
            using (var context = new TestContext(ContextUtil.GetOptions()))
            {
                //int entitiesCount = ItemsCountQuery(context);
                int entitiesCount = await context.Items.CountAsync();
                //Item lastEntity = LastItemQuery(context);
                Item lastEntity = context.Items.OrderByDescending(a => a.ItemId).FirstOrDefault();

                Assert.Equal(EntitiesNumber, entitiesCount);
                Assert.NotNull(lastEntity);
                Assert.Equal("Desc Update " + EntitiesNumber, lastEntity.Description);
            }
        }

        private async Task RunReadAsync(bool isBulk)
        {
            using (var context = new TestContext(ContextUtil.GetOptions()))
            {
                var entities = new List<Item>();

                for (int i = 1; i < EntitiesNumber; i++)
                {
                    var entity = new Item
                    {
                        Name = "name " + i,
                    };
                    entities.Add(entity);
                }

                await context.BulkReadAsync(
                    entities,
                    new BulkConfig
                    {
                        UpdateByProperties = new List<string> { nameof(Item.Name) }
                    }
                );

                Assert.Equal(1, entities[0].ItemId);
                Assert.Equal(0, entities[1].ItemId);
                Assert.Equal(3, entities[2].ItemId);
                Assert.Equal(0, entities[3].ItemId);
            }
        }

        private async Task RunDeleteAsync(bool isBulk, DbServer dbServer)
        {
            using (var context = new TestContext(ContextUtil.GetOptions()))
            {
                var entities = AllItemsQuery(context).ToList();
                // ItemHistories will also be deleted because of Relationship - ItemId (Delete Rule: Cascade)
                if (isBulk)
                {
                    var bulkConfig = new BulkConfig() { CalculateStats = true };
                    await context.BulkDeleteAsync(entities, bulkConfig);
                    if (dbServer == DbServer.SqlServer)
                    {
                        Assert.Equal(0, bulkConfig.StatsInfo.StatsNumberInserted);
                        Assert.Equal(0, bulkConfig.StatsInfo.StatsNumberUpdated);
                        Assert.Equal(EntitiesNumber / 2, bulkConfig.StatsInfo.StatsNumberDeleted);
                    }
                }
                else
                {
                    context.Items.RemoveRange(entities);
                    await context.SaveChangesAsync();
                }
            }
            using (var context = new TestContext(ContextUtil.GetOptions()))
            {
                //int entitiesCount = ItemsCountQuery(context);
                int entitiesCount = await context.Items.CountAsync();
                //Item lastEntity = LastItemQuery(context);
                Item lastEntity = context.Items.OrderByDescending(a => a.ItemId).FirstOrDefault();

                Assert.Equal(0, entitiesCount);
                Assert.Null(lastEntity);
            }

            using (var context = new TestContext(ContextUtil.GetOptions()))
            {
                if (dbServer == DbServer.SqlServer)
                {
                    await context.Database.ExecuteSqlRawAsync("DBCC CHECKIDENT('[dbo].[Item]', RESEED, 0);").ConfigureAwait(false);
                }
                if (dbServer == DbServer.Sqlite)
                {
                    await context.Database.ExecuteSqlRawAsync("DELETE FROM sqlite_sequence WHERE name = 'Item';").ConfigureAwait(false);
                }
            }
        }
    }
}
