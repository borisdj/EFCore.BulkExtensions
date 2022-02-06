using EFCore.BulkExtensions.SqlAdapters;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.Extensions.Caching.Memory;
using Npgsql;
using NpgsqlTypes;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text.Json;
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
        [InlineData(DbServer.PostgreSQL, true)]
        public void InsertTestPostgreSql(DbServer dbServer, bool isBulk)
        {
            ContextUtil.DbServer = dbServer;

            using var context = new TestContext(ContextUtil.GetOptions());

            context.Database.ExecuteSqlRaw($@"DELETE FROM ""{nameof(Item)}""");
            context.Database.ExecuteSqlRaw($@"ALTER SEQUENCE ""{nameof(Item)}_{nameof(Item.ItemId)}_seq"" RESTART WITH 1");

            context.Database.ExecuteSqlRaw($@"DELETE FROM ""{nameof(Box)}""");
            context.Database.ExecuteSqlRaw($@"ALTER SEQUENCE ""{nameof(Box)}_{nameof(Box.BoxId)}_seq"" RESTART WITH 1");

            context.Database.ExecuteSqlRaw($@"DELETE FROM ""{nameof(UserRole)}""");

            var currentTime = DateTime.UtcNow; // default DateTime type: "timestamp with time zone"; DateTime.Now goes with: "timestamp without time zone"

            var entities = new List<Item>();
            for (int i = 1; i <= 2; i++)
            {
                var entity = new Item
                {
                    //ItemId = i,
                    Name = "Name " + i,
                    Description = "info " + i,
                    Quantity = i,
                    Price = 0.1m * i,
                    TimeUpdated = currentTime,
                };
                entities.Add(entity);
            }

            var entities2 = new List<Item>();
            for (int i = 2; i <= 3; i++)
            {
                var entity = new Item
                {
                    ItemId = i,
                    Name = "Name " + i,
                    Description = "UPDATE " + i,
                    Quantity = i,
                    Price = 0.1m * i,
                    TimeUpdated = currentTime,
                };
                entities2.Add(entity);
            }

            var entities3 = new List<Item>();
            for (int i = 3; i <= 4; i++)
            {
                var entity = new Item
                {
                    //ItemId = i,
                    Name = "Name " + i,
                    Description = "CHANGE " + i,
                    Quantity = i,
                    Price = 0.1m * i,
                    TimeUpdated = currentTime,
                };
                entities3.Add(entity);
            }

            // INSERT
            context.BulkInsert(entities, new BulkConfig() { NotifyAfter = 1 }, (a) => WriteProgress(a));

            Assert.Equal("info 1", context.Items.Where(a => a.Name == "Name 1").AsNoTracking().FirstOrDefault().Description);
            Assert.Equal("info 2", context.Items.Where(a => a.Name == "Name 2").AsNoTracking().FirstOrDefault().Description);

            // UPDATE
            context.BulkInsertOrUpdate(entities2);

            Assert.Equal("UPDATE 2", context.Items.Where(a => a.Name == "Name 2").AsNoTracking().FirstOrDefault().Description);
            Assert.Equal("UPDATE 3", context.Items.Where(a => a.Name == "Name 3").AsNoTracking().FirstOrDefault().Description);

            var configUpdateBy = new BulkConfig { UpdateByProperties = new List<string> { nameof(Item.Name) } };

            configUpdateBy.SetOutputIdentity = true;
            context.BulkUpdate(entities3, configUpdateBy);

            Assert.Equal(3, entities3[0].ItemId); // to test Output
            Assert.Equal(4, entities3[1].ItemId);

            Assert.Equal("CHANGE 3", context.Items.Where(a => a.Name == "Name 3").AsNoTracking().FirstOrDefault().Description);
            Assert.Equal("CHANGE 4", context.Items.Where(a => a.Name == "Name 4").AsNoTracking().FirstOrDefault().Description);

            // Test Multiple KEYS
            var userRoles = new List<UserRole> { new UserRole { Description = "Info" } };
            context.BulkInsertOrUpdate(userRoles);

            // DELETE
            context.BulkDelete(new List<Item>() { entities2[1] }, configUpdateBy);


            // READ
            var secondEntity = new List<Item>() { new Item { Name = entities[1].Name } };
            context.BulkRead(secondEntity, configUpdateBy);
            Assert.Equal(2, secondEntity.FirstOrDefault().ItemId);
            Assert.Equal("UPDATE 2", secondEntity.FirstOrDefault().Description);


            // BATCH
            var query = context.Items.AsQueryable().Where(a => a.ItemId <= 1);
            query.BatchUpdate(new Item { Description = "UPDATE N", Price = 1.5m }/*, updateColumns*/);

            var queryJoin = context.ItemHistories.Where(p => p.Item.Description == "UPDATE 2");
            queryJoin.BatchUpdate(new ItemHistory { Remark = "Rx", });

            var query2 = context.Items.AsQueryable().Where(a => a.ItemId > 1 && a.ItemId < 3);
            query.BatchDelete();

            var descriptionsToDelete = new List<string> { "info" };
            var query3 = context.Items.Where(a => descriptionsToDelete.Contains(a.Description));
            query3.BatchDelete();

            // for type 'jsonb'
            JsonDocument jsonbDoc = JsonDocument.Parse(@"{ ""ModelEL"" : ""Square""}");
            var box = new Box { DocumentContent = jsonbDoc, ElementContent = jsonbDoc.RootElement };
            context.BulkInsert(new List<Box> { box });

            JsonDocument jsonbDoc2 = JsonDocument.Parse(@"{ ""ModelEL"" : ""Circle""}");
            var boxQuery = context.Boxes.AsQueryable().Where(a => a.BoxId <= 1);
            boxQuery.BatchUpdate(new Box { DocumentContent = jsonbDoc2, ElementContent = jsonbDoc2.RootElement });

            //var incrementStep = 100;
            //var suffix = " Concatenated";
            //query.BatchUpdate(a => new Item { Name = a.Name + suffix, Quantity = a.Quantity + incrementStep }); // example of BatchUpdate Increment/Decrement value in variable
        }

        [Theory]
        [InlineData(DbServer.SQLServer, true)]
        [InlineData(DbServer.SQLite, true)]
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

            if (dbServer == DbServer.SQLServer)
            {
                RunInsertOrUpdateOrDelete(isBulk); // Not supported for Sqlite (has only UPSERT), instead use BulkRead, then split list into sublists and call separately Bulk methods for Insert, Update, Delete.
            }
            RunDelete(isBulk, dbServer);

            //CheckQueryCache();
        }

        [Theory]
        [InlineData(DbServer.SQLServer)]
        [InlineData(DbServer.SQLite)]
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
                    DbServer.SQLite => $"CREATE TEMPORARY {createTableSql}",
                    DbServer.SQLServer => $"CREATE {createTableSql}",
                    _ => throw new ArgumentException($"Unknown database type: '{dbServer}'.", nameof(dbServer)),
                };

                context.Database.ExecuteSqlRaw(createTableSql);

                bulkOperation(context);

                context.Database.ExecuteSqlRaw($"SELECT {columnName} FROM {tableName}");
            }
            catch (Exception ex)
            {
                // Table already exist
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
                if (ContextUtil.DbServer == DbServer.SQLServer)
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
                else if (ContextUtil.DbServer == DbServer.SQLite)
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
                if (dbServer == DbServer.SQLServer)
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
                    UpdateByProperties = dbServer == DbServer.SQLServer ? new List<string> { nameof(Item.Name) } : null,
                    CalculateStats = true
                };
                context.BulkUpdate(entities, bulkConfig);
                if (dbServer == DbServer.SQLServer)
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

            context.BulkRead(
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

        private void RunDelete(bool isBulk, DbServer dbServer)
        {
            using var context = new TestContext(ContextUtil.GetOptions());

            var entities = AllItemsQuery(context).ToList();
            // ItemHistories will also be deleted because of Relationship - ItemId (Delete Rule: Cascade)
            if (isBulk)
            {
                var bulkConfig = new BulkConfig() { CalculateStats = true };
                context.BulkDelete(entities, bulkConfig);
                if (dbServer == DbServer.SQLServer)
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
                DbServer.SQLServer => $"DBCC CHECKIDENT('[dbo].[{nameof(Item)}]', RESEED, 0);",
                DbServer.SQLite => $"DELETE FROM sqlite_sequence WHERE name = '{nameof(Item)}';",
                _ => throw new ArgumentException($"Unknown database type: '{dbServer}'.", nameof(dbServer)),
            };
            context.Database.ExecuteSqlRaw(deleteTableSql);
        }
    }
}
