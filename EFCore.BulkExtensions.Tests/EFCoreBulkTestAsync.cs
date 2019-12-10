using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EFCore.BulkExtensions.Tests
{
    public class EFCoreBulkTestAsync
    {
        protected int EntitiesNumber => 100000;

        private static Func<TestContext, int> ItemsCountQuery = EF.CompileQuery<TestContext, int>(ctx => ctx.Items.Count());
        private static Func<TestContext, Item> LastItemQuery = EF.CompileQuery<TestContext, Item>(ctx => ctx.Items.LastOrDefault());
        private static Func<TestContext, IEnumerable<Item>> AllItemsQuery = EF.CompileQuery<TestContext, IEnumerable<Item>>(ctx => ctx.Items.AsNoTracking());

        [Theory]
        [InlineData(DbServer.SqlServer, true)]
        [InlineData(DbServer.Sqlite, true)]
        //[InlineData(DatabaseType.SqlServer, false)] // for speed comparison with Regular EF CUD operations
        public async Task OperationsTestAsync(DbServer databaseType, bool isBulkOperation)
        {
            ContextUtil.DbServer = databaseType;

            await new EFCoreBatchTestAsync().RunDeleteAllAsync(databaseType); // TODO

            // Test can be run individually by commenting others and running each separately in order one after another
            await RunInsertAsync(isBulkOperation);
            await RunInsertOrUpdateAsync(isBulkOperation);
            await RunUpdateAsync(isBulkOperation);
            if (databaseType == DbServer.SqlServer)
            {
                await RunReadAsync(isBulkOperation); // Not Yet supported for Sqlite
            }
            await RunDeleteAsync(isBulkOperation, databaseType);
        }

        private async Task RunInsertAsync(bool isBulkOperation)
        {
            using (var context = new TestContext(ContextUtil.GetOptions()))
            {
                var entities = new List<Item>();
                var subEntities = new List<ItemHistory>();
                for (int i = 1; i < EntitiesNumber; i++)
                {
                    var entity = new Item
                    {
                        ItemId = isBulkOperation ? i : 0,
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

                if (isBulkOperation)
                {
                    if (ContextUtil.DbServer == DbServer.SqlServer)
                    {
                        using (var transaction = await context.Database.BeginTransactionAsync())
                        {
                            await context.BulkInsertAsync(entities, new BulkConfig { PreserveInsertOrder = true, SetOutputIdentity = true, BatchSize = 4000 });

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
                        using (var connection = (SqliteConnection)context.Database.GetDbConnection())
                        {
                            connection.Open();
                            using (var transaction = connection.BeginTransaction())
                            {
                                var bulkConfig = new BulkConfig()
                                {
                                    SqliteConnection = connection,
                                    SqliteTransaction = transaction
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

        private async Task RunInsertOrUpdateAsync(bool isBulkOperation)
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
                if (isBulkOperation)
                {
                    var bulkConfig = new BulkConfig() { SetOutputIdentity = true, CalculateStats = true };
                    await context.BulkInsertOrUpdateAsync(entities, bulkConfig);
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

        private async Task RunUpdateAsync(bool isBulkOperation)
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
                if (isBulkOperation)
                {
                    var bulkConfig = new BulkConfig() { SetOutputIdentity = true, CalculateStats = true };
                    await context.BulkUpdateAsync(entities, bulkConfig);
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

        private async Task RunReadAsync(bool isBulkOperation)
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

        private async Task RunDeleteAsync(bool isBulkOperation, DbServer databaseType)
        {
            using (var context = new TestContext(ContextUtil.GetOptions()))
            {
                var entities = AllItemsQuery(context).ToList();
                // ItemHistories will also be deleted because of Relationship - ItemId (Delete Rule: Cascade)
                if (isBulkOperation)
                {
                    await context.BulkDeleteAsync(entities);
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
                if (databaseType == DbServer.SqlServer)
                {
                    await context.Database.ExecuteSqlRawAsync("DBCC CHECKIDENT('[dbo].[Item]', RESEED, 0);").ConfigureAwait(false);
                }
                if (databaseType == DbServer.Sqlite)
                {
                    await context.Database.ExecuteSqlRawAsync("DELETE FROM sqlite_sequence WHERE name = 'Item';").ConfigureAwait(false);
                }
            }
        }
    }
}
