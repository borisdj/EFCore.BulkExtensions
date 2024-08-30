using EFCore.BulkExtensions.SqlAdapters;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.Extensions.Caching.Memory;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text.Json;
using Xunit;

namespace EFCore.BulkExtensions.Tests;

public class EFCoreBulkTest
{
    protected static int EntitiesNumber => 10000;

    private static readonly Func<TestContext, int> ItemsCountQuery = EF.CompileQuery<TestContext, int>(ctx => ctx.Items.Count());
    private static readonly Func<TestContext, Item?> LastItemQuery = EF.CompileQuery<TestContext, Item?>(ctx => ctx.Items.LastOrDefault());
    private static readonly Func<TestContext, IEnumerable<Item>> AllItemsQuery = EF.CompileQuery<TestContext, IEnumerable<Item>>(ctx => ctx.Items.AsNoTracking());

    [Theory]
    [InlineData(SqlType.PostgreSql)]
    public void InsertEnumStringValue(SqlType sqlType)
    {
        ContextUtil.DatabaseType = sqlType;

        using var context = new TestContext(ContextUtil.GetOptions());
        context.Database.ExecuteSqlRaw($@"DELETE FROM ""{nameof(Wall)}""");
        context.Database.ExecuteSqlRaw($@"DELETE FROM ""{nameof(TimeRecord)}""");

        var walls = new List<Wall>();
        for (int i = 1; i <= 10; i++)
        {
            walls.Add(new Wall
            {
                Id = i,
                WallTypeValue = WallType.Brick,
                WallCategory = WallCategory.High,
            });
        }

        //context.Walls.AddRange(walls);
        //context.SaveChanges();
        // INSERT
        context.BulkInsert(walls);

        var addedWall = context.Walls.AsNoTracking().First(x => x.Id == walls[0].Id);
         
        Assert.True(addedWall.WallTypeValue == walls[0].WallTypeValue);


        var timeRecord = new TimeRecord()
        {
            Source = new TimeRecordSource
            {
                Name = "Abcd",
                Type = TimeRecordSourceType.Operator // for PG required Converter explicitly configured in OnModelCreating
            },
        };

        context.BulkInsert(new List<TimeRecord> { timeRecord });
    }

    [Theory]
    [InlineData(SqlType.PostgreSql)]
    public void InsertTestPostgreSql(SqlType sqlType)
    {
        ContextUtil.DatabaseType = sqlType;

        using var context = new TestContext(ContextUtil.GetOptions());
        
        context.Database.ExecuteSqlRaw($@"DELETE FROM ""{nameof(Item)}""");
        context.Database.ExecuteSqlRaw($@"ALTER SEQUENCE ""{nameof(Item)}_{nameof(Item.ItemId)}_seq"" RESTART WITH 1");

        context.Database.ExecuteSqlRaw($@"DELETE FROM ""{nameof(ItemHistory)}""");

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
        for (int i = 3; i <= 4; i++)
        {
            var entity = new Item
            {
                //ItemId = i,
                Name = "Name " + i,
                Description = "UPDATE " + i,
                Quantity = i,
                Price = 0.1m * i,
                TimeUpdated = currentTime,
            };
            entities2.Add(entity);
        }

        var entities3 = new List<Item>();
        for (int i = 4; i <= 4; i++)
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
        
        var entities56 = new List<Item>();
        for (int i = 5; i <= 6; i++)
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
            entities56.Add(entity);
        }
        
        var entities78 = new List<Item>();
        for (int i = 7; i <= 8; i++)
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
            entities78.Add(entity);
        }

        // INSERT
        context.BulkInsert(entities);

        Assert.Equal("info 1", context.Items.Where(a => a.Name == "Name 1").AsNoTracking().FirstOrDefault()?.Description);
        Assert.Equal("info 2", context.Items.Where(a => a.Name == "Name 2").AsNoTracking().FirstOrDefault()?.Description);

        // UPDATE
        var config = new BulkConfig
        {
            UpdateByProperties = new List<string> { nameof(Item.Name) },
            NotifyAfter = 1,
            SetOutputIdentity = true,
            CalculateStats = true,
        };
        context.BulkInsertOrUpdate(entities2, config, (a) => WriteProgress(a));
        
        Assert.Equal("UPDATE 3", context.Items.Where(a => a.Name == "Name 3").AsNoTracking().FirstOrDefault()?.Description);
        Assert.Equal("UPDATE 4", context.Items.Where(a => a.Name == "Name 4").AsNoTracking().FirstOrDefault()?.Description);

        var configUpdateBy = new BulkConfig {
            SetOutputIdentity = true,
            UpdateByProperties = new List<string> { nameof(Item.Name) },
            PropertiesToInclude = new List<string> { nameof(Item.Name), nameof(Item.Description) }, // "Name" in list not necessary since is in UpdateBy
        };
        context.BulkUpdate(entities3, configUpdateBy);

        Assert.Equal(4, entities3[0].ItemId); // to test Output

        Assert.Equal("UPDATE 3", context.Items.Where(a => a.Name == "Name 3").AsNoTracking().FirstOrDefault()?.Description);
        Assert.Equal("CHANGE 4", context.Items.Where(a => a.Name == "Name 4").AsNoTracking().FirstOrDefault()?.Description);
        
        // Test Multiple KEYS
        var userRoles = new List<UserRole> { new UserRole { Description = "Info" } };
        context.BulkInsertOrUpdate(userRoles);

        // DELETE
        context.BulkDelete(new List<Item>() { entities2[0] }, configUpdateBy);

        // READ
        var secondEntity = new List<Item>() { new Item { Name = entities[1].Name } };
        context.BulkRead(secondEntity, configUpdateBy);
        Assert.Equal(2, secondEntity.FirstOrDefault()?.ItemId);
        Assert.Equal("info 2", secondEntity.FirstOrDefault()?.Description);

        // SAVE CHANGES
        context.AddRange(entities56);
        context.BulkSaveChanges();
        Assert.Equal(5, entities56[0].ItemId);

        // Test PropIncludeOnUpdate (supported with: 'applySubqueryLimit')
        var bulkConfig = new BulkConfig
        {
            UpdateByProperties = new List<string> { nameof(Item.Name) },
            PropertiesToIncludeOnUpdate = new List<string> { "" },
            SetOutputIdentity = true
        };
        context.BulkInsertOrUpdate(entities78, bulkConfig);

        context.BulkInsert(new List<ItemHistory> { new ItemHistory { ItemHistoryId = Guid.NewGuid(), Remark = "Rx", ItemId = 1 } });

        // BATCH
        var query = context.Items.AsQueryable().Where(a => a.ItemId <= 1);
#pragma warning disable
        query.BatchUpdate(new Item { Description = "UPDATE N", Price = 1.5m }); //, updateColumns);
#pragma warning disable
        var ids = new[] { Guid.Empty };
        context.ItemHistories.Where(o => ids.Contains(o.ItemHistoryId)).BatchDelete();

        var queryJoin = context.ItemHistories.Where(p => p.Item.Description == "UPDATE 2");
        queryJoin.BatchUpdate(new ItemHistory { Remark = "Rx", });

        var query2 = context.Items.AsQueryable().Where(a => a.ItemId > 1 && a.ItemId < 3);
        query.BatchDelete();

        var quants = new[] { 1, 2, 3 };
        int qu = 5;
        query.Where(a => quants.Contains(a.Quantity)).BatchUpdate(o => new Item { Quantity = qu });

        var descriptionsToDelete = new List<string> { "info" };
        var query3 = context.Items.Where(a => descriptionsToDelete.Contains(a.Description ?? ""));
        query3.BatchDelete();

        // for type 'jsonb'
        JsonDocument jsonbDoc = JsonDocument.Parse(@"{ ""ModelEL"" : ""Square""}");
        var box = new Box { DocumentContent = jsonbDoc, ElementContent = jsonbDoc.RootElement };
        context.BulkInsert(new List<Box> { box });

        JsonDocument jsonbDoc2 = JsonDocument.Parse(@"{ ""ModelEL"" : ""Circle""}");
        var boxQuery = context.Boxes.AsQueryable().Where(a => a.BoxId <= 1);
        boxQuery.BatchUpdate(new Box { DocumentContent = jsonbDoc2, ElementContent = jsonbDoc2.RootElement });


        var graphQLModels = new List<GraphQLModel>();
        for (int i = 1; i <= 2; i++)
        {
            var graphQLModel = new GraphQLModel
            {
                //ItemId = i,
                Name = "Name " + i,
            };
            graphQLModels.Add(graphQLModel);
        }

        var cnfg = new BulkConfig() { PropertiesToExclude = new List<string> { nameof(GraphQLModel.Id) } };
        context.BulkInsert(graphQLModels, cnfg);

        //var incrementStep = 100;
        //var suffix = " Concatenated";
        //query.BatchUpdate(a => new Item { Name = a.Name + suffix, Quantity = a.Quantity + incrementStep }); // example of BatchUpdate Increment/Decrement value in variable
    }
    
    [Theory]
    [InlineData(SqlType.MySql)]
    // -- Before first run following command should be executed on mysql server:
    //    SET GLOBAL local_infile = true;
    // -- otherwise exception: "Loading local data is disabled; this must be enabled on both the client and server sides"
    // -- For client side connection string is already set with: "AllowLoadLocalInfile=true"
    public void InsertTestMySql(SqlType sqlType)
    {
        ContextUtil.DatabaseType = sqlType;

        using var context = new TestContext(ContextUtil.GetOptions());

        var currentTime = DateTime.UtcNow; // default DateTime type: "timestamp with time zone"; DateTime.Now goes with: "timestamp without time zone"

        context.Items.RemoveRange(context.Items.ToList());
        context.SaveChanges();
        context.Database.ExecuteSqlRaw("ALTER TABLE " + nameof(Item) + " AUTO_INCREMENT = 1");
        context.SaveChanges();

        var entities1 = new List<Item>();
        for (int i = 1; i <= 10; i++)
        {
            var entity = new Item
            {
                ItemId = i,
                Name = "Name " + i,
                Description = "info " + i,
                Quantity = i,
                Price = 0.1m * i,
                TimeUpdated = currentTime,
            };
            entities1.Add(entity);
        }

        var entities2 = new List<Item>();
        
        for (int i = 6; i <= 15; i++)
        {
            var entity = new Item
            {
                ItemId = i,
                Name = "Name " + i,
                Description = "v2 info " + i,
                Quantity = i,
                Price = 0.1m * i,
                TimeUpdated = currentTime,
            };
            entities2.Add(entity);
        }
        var entities3 = new List<Item>();
        var entities4 = new List<Item>();
        var entities5 = new List<Item>();

        // INSERT

        context.BulkInsertOrUpdate(entities1, bc => bc.SetOutputIdentity = true);
        Assert.Equal(1, entities1[0].ItemId);
        Assert.Equal("info 1", context.Items.Where(a => a.Name == "Name 1").AsNoTracking().FirstOrDefault()?.Description);
        Assert.Equal("info 2", context.Items.Where(a => a.Name == "Name 2").AsNoTracking().FirstOrDefault()?.Description);

        var query = context.Items.AsQueryable().Where(a => a.ItemId <= 1);
        query.BatchUpdate(new Item { Description = "UPDATE N", Price = 1.5m }); //, updateColumns);

        // INSERT Or UPDATE
        //mysql automatically detects unique or primary key
        context.BulkInsertOrUpdate(entities2, new BulkConfig { UpdateByProperties  = new List<string> { nameof(Item.ItemId) } });
        Assert.Equal("info 5", context.Items.Where(a => a.Name == "Name 5").AsNoTracking().FirstOrDefault()?.Description);
        Assert.Equal("v2 info 6", context.Items.Where(a => a.Name == "Name 6").AsNoTracking().FirstOrDefault()?.Description);
        Assert.Equal("v2 info 15", context.Items.Where(a => a.Name == "Name 15").AsNoTracking().FirstOrDefault()?.Description);
        
        entities3.AddRange(context.Items.Where(a => a.ItemId <= 2).AsNoTracking());
        foreach (var entity in entities3)
        {
            entity.Description = "UPDATED";
        }
        context.BulkUpdate(entities3);
        Assert.Equal("UPDATED", context.Items.Where(a => a.Name == "Name 1").AsNoTracking().FirstOrDefault()?.Description);

        // TODO Custom UpdateBy column not working
        entities4.AddRange(context.Items.Where(a => a.ItemId >= 3 && a.ItemId <= 4).AsNoTracking());
        foreach (var entity in entities4)
        {
            entity.ItemId = 0; // should be matched by Name
            entity.Description = "UPDATED 2";
        }
        var configUpdateBy = new BulkConfig { UpdateByProperties = new List<string> { nameof(Item.Name) } }; // SetOutputIdentity = true;
        context.BulkUpdate(entities4, configUpdateBy);
        Assert.Equal("UPDATED 2", context.Items.Where(a => a.Name == "Name 3").AsNoTracking().FirstOrDefault()?.Description);

        context.BulkDelete(new List<Item> { new Item { ItemId = 11 } });
        Assert.False(context.Items.Where(a => a.Name == "Name 11").AsNoTracking().Any());

        entities5.Add(new Item { ItemId = 16, Name = "Name 16", Description = "info 16" });
        context.AddRange(entities5);
        context.BulkSaveChanges(); // can only be used for multiple Insert with one or more tables, can not combine it with Upsert, since 'ItemId' would had to be set for a subset only and no way to distinguish it.
        Assert.Equal("info 16", context.Items.Where(a => a.Name == "Name 16").AsNoTracking().FirstOrDefault()?.Description);

        var entities6 = new List<Item>();
        for (int i = 16; i <= 17; i++)
        {
            var entity = new Item
            {
                ItemId = i,
                Name = "Name " + i,
                Description = "info " + i,
                Quantity = i,
                Price = 0.1m * i,
                TimeUpdated = currentTime,
            };
            entities6.Add(entity);
        }
        var bulkConfig = new BulkConfig
        {
            UpdateByProperties = new List<string> { nameof(Item.Name) },
            PropertiesToIncludeOnUpdate = new List<string> { "" },
            SetOutputIdentity = true
        };
        context.BulkInsertOrUpdate(entities6, bulkConfig);
    }

    [Theory]
    [InlineData(SqlType.SqlServer, true)]
    [InlineData(SqlType.Sqlite, true)]
    //[InlineData(DbServer.SqlServer, false)] // for speed comparison with Regular EF CUD operations
    public void OperationsTest(SqlType sqlType, bool isBulk)
    {
        ContextUtil.DatabaseType = sqlType;

        //DeletePreviousDatabase();
        new EFCoreBatchTest().RunDeleteAll(sqlType);

        RunInsert(isBulk);
        RunInsertOrUpdate(isBulk, sqlType);
        RunUpdate(isBulk, sqlType);

        RunRead();

        if (sqlType == SqlType.SqlServer)
        {
            RunInsertOrUpdateOrDelete(isBulk); // Not supported for Sqlite (has only UPSERT), instead use BulkRead, then split list into sublists and call separately Bulk methods for Insert, Update, Delete.
        }
        RunDelete(isBulk, sqlType);

        //CheckQueryCache();
    }

    [Theory]
    [InlineData(SqlType.SqlServer)]
    [InlineData(SqlType.Sqlite)]
    public void SideEffectsTest(SqlType sqlType)
    {
        BulkOperationShouldNotCloseOpenConnection(sqlType, context => context.BulkInsert(new[] { new Item() }));
        BulkOperationShouldNotCloseOpenConnection(sqlType, context => context.BulkUpdate(new[] { new Item() }));
    }

    private static void BulkOperationShouldNotCloseOpenConnection(SqlType sqlType, Action<TestContext> bulkOperation)
    {
        ContextUtil.DatabaseType = sqlType;
        using var context = new TestContext(ContextUtil.GetOptions());

        var sqlHelper = context.GetService<ISqlGenerationHelper>();
        context.Database.OpenConnection();

        try
        {
            // we use a temp table to verify whether the connection has been closed (and re-opened) inside BulkUpdate(Async)
            var columnName = sqlHelper.DelimitIdentifier("Id");
            var tableName = sqlHelper.DelimitIdentifier("#MyTempTable");
            var createTableSql = $" TABLE {tableName} ({columnName} INTEGER);";

            createTableSql = sqlType switch
            {
                SqlType.Sqlite => $"CREATE TEMPORARY {createTableSql}",
                SqlType.SqlServer => $"CREATE {createTableSql}",
                _ => throw new ArgumentException($"Unknown database type: '{sqlType}'.", nameof(sqlType)),
            };

            context.Database.ExecuteSqlRaw(createTableSql);

            bulkOperation(context);

            var sql = $"SELECT {columnName} FROM {tableName}";
            context.Database.ExecuteSqlRaw(sql);
        }
        catch (Exception)
        {
            // Table already exist
        }
        finally
        {
            context.Database.CloseConnection();
        }
    }

    private static void DeletePreviousDatabase()
    {
        using var context = new TestContext(ContextUtil.GetOptions());
        context.Database.EnsureDeleted();
    }

    private static void CheckQueryCache()
    {
        using var context = new TestContext(ContextUtil.GetOptions());
        var compiledQueryCache = ((MemoryCache)context.GetService<IMemoryCache>());

        Assert.Equal(0, compiledQueryCache.Count);
    }

    private static void WriteProgress(decimal percentage, bool writeOnConsole = false)
    {
        if(writeOnConsole)
            Debug.WriteLine(percentage);
    }

    private static void RunInsert(bool isBulk)
    {
        using var context = new TestContext(ContextUtil.GetOptions());
        var categories  = new List<ItemCategory> { new ItemCategory { Id = 1, Name = "Some 1" }, new ItemCategory { Id = 2, Name = "Some 2" } };
        var entities = new List<Item>();
        var subEntities = new List<ItemHistory>();
        for (int i = 1, j = -(EntitiesNumber - 1); i < EntitiesNumber; i++, j++)
        {
            var entity = new Item
            {
                ItemId = 0, //isBulk ? j : 0, // no longer used since order(Identity temporary filled with negative values from -N to -1) is set automaticaly with default config PreserveInsertOrder=TRUE
                Name = "name " + i,
                Description = string.Concat("info ", Guid.NewGuid().ToString().AsSpan(0, 3)),
                Quantity = i % 10,
                Price = i / (i % 5 + 1),
                TimeUpdated = DateTime.Now,
                ItemHistories = new List<ItemHistory>()
            };

            entity.Category = categories[i%categories.Count];

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
            context.BulkInsertOrUpdate(categories);
            if (ContextUtil.DatabaseType == SqlType.SqlServer)
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
                context.BulkInsert(subEntities);

                transaction.Commit();
            }
            else if (ContextUtil.DatabaseType == SqlType.Sqlite)
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
        int entitiesCount = context.Items.Count();
        Item? lastEntity = context.Items.OrderByDescending(a => a.ItemId).FirstOrDefault();

        Assert.Equal(EntitiesNumber - 1, entitiesCount);
        Assert.NotNull(lastEntity);
        Assert.Equal("name " + (EntitiesNumber - 1), lastEntity?.Name);
    }

    private static void RunInsertOrUpdate(bool isBulk, SqlType sqlType)
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
            var bulkConfig = new BulkConfig() {
                SetOutputIdentity = true,
                CalculateStats = true,
                SqlBulkCopyOptions = SqlBulkCopyOptions.KeepIdentity
            };
            context.BulkInsertOrUpdate(entities, bulkConfig, (a) => WriteProgress(a));
            if (sqlType == SqlType.SqlServer)
            {
                Assert.Equal(1, bulkConfig.StatsInfo?.StatsNumberInserted);
                Assert.Equal(EntitiesNumber / 2 - 1, bulkConfig.StatsInfo?.StatsNumberUpdated);
                Assert.Equal(0, bulkConfig.StatsInfo?.StatsNumberDeleted);
            }
        }
        else
        {
            var lastEntity1 = entities[^1]; // Last
            context.Items.Add(lastEntity1);
            context.SaveChanges();
        }

        // TEST
        int entitiesCount = context.Items.Count();
        Item? lastEntity = context.Items.OrderByDescending(a => a.ItemId).FirstOrDefault();

        Assert.Equal(EntitiesNumber, entitiesCount);
        Assert.NotNull(lastEntity);
        Assert.Equal("name InsertOrUpdate " + EntitiesNumber, lastEntity?.Name);
    }

    private static void RunInsertOrUpdateOrDelete(bool isBulk)
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

        int? keepEntityItemId = null;
        if (isBulk)
        {
            var bulkConfig = new BulkConfig() { SetOutputIdentity = true, CalculateStats = true };
            keepEntityItemId = 3;
            bulkConfig.SetSynchronizeFilter<Item>(e => e.ItemId != keepEntityItemId.Value);
            bulkConfig.OnConflictUpdateWhereSql = (existing, inserted) => $"{inserted}.{nameof(Item.TimeUpdated)} > {existing}.{nameof(Item.TimeUpdated)}"; // can use nameof bacause in this case property name is same as column name 

            context.BulkInsertOrUpdateOrDelete(entities, bulkConfig, (a) => WriteProgress(a));

            Assert.Equal(0, bulkConfig.StatsInfo?.StatsNumberInserted);
            Assert.Equal(EntitiesNumber / 2, bulkConfig.StatsInfo?.StatsNumberUpdated);
            Assert.Equal(EntitiesNumber / 2 - 1, bulkConfig.StatsInfo?.StatsNumberDeleted);
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
        Item? firstEntity = context.Items.OrderBy(a => a.ItemId).FirstOrDefault();
        Item? lastEntity = context.Items.OrderByDescending(a => a.ItemId).FirstOrDefault();

        Assert.Equal(EntitiesNumber / 2 + (keepEntityItemId != null ? 1 : 0), entitiesCount);
        Assert.NotNull(firstEntity);
        Assert.Equal("name InsertOrUpdateOrDelete 2", firstEntity?.Name);
        Assert.NotNull(lastEntity);
        Assert.Equal("name InsertOrUpdateOrDelete " + EntitiesNumber, lastEntity?.Name);

        var bulkConfigSoftDel = new BulkConfig();
        bulkConfigSoftDel.SetSynchronizeSoftDelete<Item>(a => new Item { Quantity = 0 }); // Instead of Deleting from DB it updates Quantity to 0 (usual usecase would be: IsDeleted to True)
        context.BulkInsertOrUpdateOrDelete(new List<Item> { entities[1] }, bulkConfigSoftDel);

        var list = context.Items.Take(2).ToList();
        Assert.True(list[0].Quantity != 0);
        Assert.True(list[1].Quantity == 0);

        // TEST Alias
        context.Entries.Add(new Entry { Name = "Entry_InsertOrUpdateOrDelete" });
        context.SaveChanges();

        int entriesCount = context.Entries.Count();
        
        bulkConfigSoftDel.SetSynchronizeSoftDelete<Entry>(a => new Entry { Name = "Entry_InsertOrUpdateOrDelete_Deleted" });
        context.BulkInsertOrUpdateOrDelete(new List<Entry> { new Entry { Name = "Entry_InsertOrUpdateOrDelete_2" } }, bulkConfigSoftDel);

        Assert.Equal(entriesCount + 1,  context.Entries.Count());
        Assert.True(context.Entries.Any(e => e.Name == "Entry_InsertOrUpdateOrDelete_Deleted"));
    }

    private static void RunUpdate(bool isBulk, SqlType sqlType)
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
                UpdateByProperties = sqlType == SqlType.SqlServer ? new List<string> { nameof(Item.Name) } : null,
                CalculateStats = true
            };
            context.BulkUpdate(entities, bulkConfig);
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
            context.SaveChanges();
        }

        // TEST
        int entitiesCount = context.Items.Count();
        Item? lastEntity = context.Items.OrderByDescending(a => a.ItemId).FirstOrDefault();

        Assert.Equal(EntitiesNumber, entitiesCount);
        Assert.NotNull(lastEntity);
        Assert.Equal("name InsertOrUpdate " + EntitiesNumber, lastEntity?.Name);
    }

    private static void RunRead()
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

    private void RunDelete(bool isBulk, SqlType sqlType)
    {
        using var context = new TestContext(ContextUtil.GetOptions());

        var entities = context.Items.ToList();
        // ItemHistories will also be deleted because of Relationship - ItemId (Delete Rule: Cascade)
        if (isBulk)
        {
            var bulkConfig = new BulkConfig() { CalculateStats = true };
            context.BulkDelete(entities, bulkConfig);
            if (sqlType == SqlType.SqlServer)
            {
                Assert.Equal(0, bulkConfig.StatsInfo?.StatsNumberInserted);
                Assert.Equal(0, bulkConfig.StatsInfo?.StatsNumberUpdated);
                Assert.Equal(entities.Count, bulkConfig.StatsInfo?.StatsNumberDeleted);
            }
        }
        else
        {
            context.Items.RemoveRange(entities);
            context.SaveChanges();
        }

        // TEST
        int entitiesCount = context.Items.Count();
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
        context.Database.ExecuteSqlRaw(deleteTableSql);
    }
}
