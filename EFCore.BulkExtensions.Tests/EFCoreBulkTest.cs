using EFCore.BulkExtensions.SqlAdapters;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.Extensions.Caching.Memory;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using Xunit;

namespace EFCore.BulkExtensions.Tests
{
    public class EFCoreBulkTest
    {
        protected int EntitiesNumber => 10000;

        private static Func<TestContext, int> ItemsCountQuery = EF.CompileQuery<TestContext, int>(ctx => ctx.Items.Count());
        private static Func<TestContext, Item> LastItemQuery = EF.CompileQuery<TestContext, Item>(ctx => ctx.Items.LastOrDefault());
        private static Func<TestContext, IEnumerable<Item>> AllItemsQuery = EF.CompileQuery<TestContext, IEnumerable<Item>>(ctx => ctx.Items.AsNoTracking());

        [Theory]
        [InlineData(DbServer.SqlServer, true)]
        [InlineData(DbServer.Sqlite, true)]
        //[InlineData(DbServer.SqlServer, false)] // for speed comparison with Regular EF CUD operations
        public void OperationsTest(DbServer dbServer, bool isBulk)
        {
            ContextUtil.DbServer = dbServer;

            //DeletePreviousDatabase();
            new EFCoreBatchTest().RunDeleteAll(dbServer);

            RunInsert(isBulk);
            RunInsertOrUpdate(isBulk, dbServer);
            RunUpdate(isBulk, dbServer);

            RunRead(isBulk);

            if (dbServer == DbServer.SqlServer)
            {
                RunInsertOrUpdateOrDelete(isBulk); // Not supported for Sqlite (has only UPSERT), instead use BulkRead, then split list into sublists and call separately Bulk methods for Insert, Update, Delete.
            }
            //RunDelete(isBulk, dbServer);

            //CheckQueryCache();
        }

        [Theory]
        [InlineData(DbServer.SqlServer)]
        [InlineData(DbServer.Sqlite)]
        public void SideEffectsTest(DbServer dbServer)
        {
            BulkOperationShouldNotCloseOpenConnection(dbServer, context => context.BulkInsert(new[] { new Item() }));
            BulkOperationShouldNotCloseOpenConnection(dbServer, context => context.BulkUpdate(new[] { new Item() }));
        }

        private static void BulkOperationShouldNotCloseOpenConnection(DbServer dbServer, Action<TestContext> bulkOperation)
        {
            ContextUtil.DbServer = dbServer;
            using var context = new TestContext(ContextUtil.GetOptions());

            var sqlHelper = context.GetService<ISqlGenerationHelper>();
            context.Database.OpenConnection();

            try
            {
                // we use a temp table to verify whether the connection has been closed (and re-opened) inside BulkUpdate(Async)
                var columnName = sqlHelper.DelimitIdentifier("Id");
                var tableName = sqlHelper.DelimitIdentifier("#MyTempTable");
                var createTableSql = $" TABLE {tableName} ({columnName} INTEGER);";

                createTableSql = dbServer switch
                {
                    DbServer.Sqlite => $"CREATE TEMPORARY {createTableSql}",
                    DbServer.SqlServer => $"CREATE {createTableSql}",
                    _ => throw new ArgumentException($"Unknown database type: '{dbServer}'.", nameof(dbServer)),
                };

                context.Database.ExecuteSqlRaw(createTableSql);

                bulkOperation(context);

                context.Database.ExecuteSqlRaw($"SELECT {columnName} FROM {tableName}");
            }
            finally
            {
                context.Database.CloseConnection();
            }
        }

        private void DeletePreviousDatabase()
        {
            using var context = new TestContext(ContextUtil.GetOptions());
            context.Database.EnsureDeleted();
        }

        private void CheckQueryCache()
        {
            using var context = new TestContext(ContextUtil.GetOptions());
            var compiledQueryCache = ((MemoryCache)context.GetService<IMemoryCache>());

            Assert.Equal(0, compiledQueryCache.Count);
        }

        private void WriteProgress(decimal percentage)
        {
            Debug.WriteLine(percentage);
        }

        private void RunInsert(bool isBulk)
        {
            using var context = new TestContext(ContextUtil.GetOptions());

            var entities = new List<Item>();
            var subEntities = new List<ItemHistory>();
            for (int i = 1, j = -(EntitiesNumber - 1); i < EntitiesNumber; i++, j++)
            {
                var entity = new Item
                {
                    ItemId = 0, //isBulk ? j : 0, // no longer used since order(Identity temporary filled with negative values from -N to -1) is set automaticaly with default config PreserveInsertOrder=TRUE
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
                    using var transaction = context.Database.BeginTransaction();
                    var bulkConfig = new BulkConfig
                    {
                        //PreserveInsertOrder = true, // true is default
                        SetOutputIdentity = true,
                        BatchSize = 4000,
                        UseTempDB = true,
                        CalculateStats = true
                    };
                    context.BulkInsert(entities, bulkConfig, (a) => WriteProgress(a));
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
                    context.BulkInsert(subEntities);

                    transaction.Commit();
                }
                else if (ContextUtil.DbServer == DbServer.Sqlite)
                {
                    using var transaction = context.Database.BeginTransaction();
                    var bulkConfig = new BulkConfig() { SetOutputIdentity = true };
                    context.BulkInsert(entities, bulkConfig);

                    foreach (var entity in entities)
                    {
                        foreach (var subEntity in entity.ItemHistories)
                        {
                            subEntity.ItemId = entity.ItemId; // setting FK to match its linked PK that was generated in DB
                        }
                        subEntities.AddRange(entity.ItemHistories);
                    }
                    bulkConfig.SetOutputIdentity = false;
                    context.BulkInsert(subEntities, bulkConfig);

                    transaction.Commit();
                }
            }
            else
            {
                context.Items.AddRange(entities);
                context.SaveChanges();
            }

            // TEST
            int entitiesCount = ItemsCountQuery(context);
            Item lastEntity = context.Items.OrderByDescending(a => a.ItemId).FirstOrDefault();

            Assert.Equal(EntitiesNumber - 1, entitiesCount);
            Assert.NotNull(lastEntity);
            Assert.Equal("name " + (EntitiesNumber - 1), lastEntity.Name);
        }

        private void RunInsertOrUpdate(bool isBulk, DbServer dbServer)
        {
            using var context = new TestContext(ContextUtil.GetOptions());

            var entities = new List<Item>();
            var dateTimeNow = DateTime.Now;
            for (int i = 2; i <= EntitiesNumber; i += 2)
            {
                entities.Add(new Item
                {
                    ItemId = isBulk ? i : 0,
                    Name = "name InsertOrUpdate " + i,
                    Description = "info",
                    Quantity = i + 100,
                    Price = i / (i % 5 + 1),
                    TimeUpdated = dateTimeNow
                });
            }
            if (isBulk)
            {
                var bulkConfig = new BulkConfig() { SetOutputIdentity = true, CalculateStats = true };
                context.BulkInsertOrUpdate(entities, bulkConfig, (a) => WriteProgress(a));
                if (dbServer == DbServer.SqlServer)
                {
                    Assert.Equal(1, bulkConfig.StatsInfo.StatsNumberInserted);
                    Assert.Equal(EntitiesNumber / 2 - 1, bulkConfig.StatsInfo.StatsNumberUpdated);
                    Assert.Equal(0, bulkConfig.StatsInfo.StatsNumberDeleted);
                }
            }
            else
            {
                context.Items.Add(entities[entities.Count - 1]);
                context.SaveChanges();
            }

            // TEST
            int entitiesCount = context.Items.Count();
            Item lastEntity = context.Items.OrderByDescending(a => a.ItemId).FirstOrDefault();

            Assert.Equal(EntitiesNumber, entitiesCount);
            Assert.NotNull(lastEntity);
            Assert.Equal("name InsertOrUpdate " + EntitiesNumber, lastEntity.Name);
        }

        private void RunInsertOrUpdateOrDelete(bool isBulk)
        {
            using var context = new TestContext(ContextUtil.GetOptions());

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
                context.BulkInsertOrUpdateOrDelete(entities, bulkConfig, (a) => WriteProgress(a));
                Assert.Equal(0, bulkConfig.StatsInfo.StatsNumberInserted);
                Assert.Equal(EntitiesNumber / 2, bulkConfig.StatsInfo.StatsNumberUpdated);
                Assert.Equal(EntitiesNumber / 2, bulkConfig.StatsInfo.StatsNumberDeleted);
            }
            else
            {
                var existingItems = context.Items;
                var removedItems = existingItems.Where(x => !entities.Any(y => y.ItemId == x.ItemId));
                context.Items.RemoveRange(removedItems);
                context.Items.AddRange(entities);
                context.SaveChanges();
            }

            // TEST
            int entitiesCount = context.Items.Count();
            Item firstEntity = context.Items.OrderBy(a => a.ItemId).FirstOrDefault();
            Item lastEntity = context.Items.OrderByDescending(a => a.ItemId).FirstOrDefault();

            Assert.Equal(EntitiesNumber / 2, entitiesCount);
            Assert.NotNull(firstEntity);
            Assert.Equal("name InsertOrUpdateOrDelete 2", firstEntity.Name);
            Assert.NotNull(lastEntity);
            Assert.Equal("name InsertOrUpdateOrDelete " + EntitiesNumber, lastEntity.Name);
        }

        private void RunUpdate(bool isBulk, DbServer dbServer)
        {
            using var context = new TestContext(ContextUtil.GetOptions());

            int counter = 1;
            var entities = context.Items.AsNoTracking().ToList();
            foreach (var entity in entities)
            {
                entity.Description = "Desc Update " + counter++;
                entity.Quantity += 1000; // will not be changed since Quantity property is not in config PropertiesToInclude
            }
            if (isBulk)
            {
                var bulkConfig = new BulkConfig
                {
                    PropertiesToInclude = new List<string> { nameof(Item.Description) },
                    UpdateByProperties = dbServer == DbServer.SqlServer ? new List<string> { nameof(Item.Name) } : null,
                    CalculateStats = true
                };
                context.BulkUpdate(entities, bulkConfig);
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
                context.SaveChanges();
            }

            // TEST
            int entitiesCount = context.Items.Count();
            Item lastEntity = context.Items.OrderByDescending(a => a.ItemId).FirstOrDefault();

            Assert.Equal(EntitiesNumber, entitiesCount);
            Assert.NotNull(lastEntity);
            Assert.Equal("name InsertOrUpdate " + EntitiesNumber, lastEntity.Name);
        }

        private void RunRead(bool isBulk)
        {
            using var context = new TestContext(ContextUtil.GetOptions());

            var entities = new List<Item>();
            for (int i = 1; i < EntitiesNumber; i++)
            {
                var entity = new Item
                {
                    Name = "name " + i,
                };
                entities.Add(entity);
            }

            using var transaction = context.Database.BeginTransaction();
            context.BulkRead(
                entities,
                new BulkConfig
                {
                    UpdateByProperties = new List<string> { nameof(Item.Name) }
                }
            );
            transaction.Commit();

            Assert.Equal(1, entities[0].ItemId);
            Assert.Equal(0, entities[1].ItemId);
            Assert.Equal(3, entities[2].ItemId);
            Assert.Equal(0, entities[3].ItemId);
        }

        private void RunDelete(bool isBulk, DbServer dbServer)
        {
            using var context = new TestContext(ContextUtil.GetOptions());

            var entities = AllItemsQuery(context).ToList();
            // ItemHistories will also be deleted because of Relationship - ItemId (Delete Rule: Cascade)
            if (isBulk)
            {
                var bulkConfig = new BulkConfig() { CalculateStats = true };
                context.BulkDelete(entities, bulkConfig);
                if (dbServer == DbServer.SqlServer)
                {
                    Assert.Equal(0, bulkConfig.StatsInfo.StatsNumberInserted);
                    Assert.Equal(0, bulkConfig.StatsInfo.StatsNumberUpdated);
                    Assert.Equal(entities.Count, bulkConfig.StatsInfo.StatsNumberDeleted);
                }
            }
            else
            {
                context.Items.RemoveRange(entities);
                context.SaveChanges();
            }

            // TEST
            int entitiesCount = context.Items.Count();
            Item lastEntity = context.Items.OrderByDescending(a => a.ItemId).FirstOrDefault();

            Assert.Equal(0, entitiesCount);
            Assert.Null(lastEntity);

            // RESET AutoIncrement
            string deleteTableSql = dbServer switch
            {
                DbServer.SqlServer => $"DBCC CHECKIDENT('[dbo].[{nameof(Item)}]', RESEED, 0);",
                DbServer.Sqlite => $"DELETE FROM sqlite_sequence WHERE name = '{nameof(Item)}';",
                _ => throw new ArgumentException($"Unknown database type: '{dbServer}'.", nameof(dbServer)),
            };
            context.Database.ExecuteSqlRaw(deleteTableSql);
        }
    }
}
