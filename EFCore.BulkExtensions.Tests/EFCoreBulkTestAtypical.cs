using EFCore.BulkExtensions.SqlAdapters;
using FastMember;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using NetTopologySuite.Geometries;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using Xunit;

namespace EFCore.BulkExtensions.Tests;

public class EFCoreBulkTestAtypical
{
    protected static int EntitiesNumber => 1000;

    [Theory]
    [InlineData(SqlType.SqlServer)]
    //[InlineData(SqlType.PostgreSql)]
    //[InlineData(SqlType.Sqlite)]
    private void CustomSqlPostProcessTest(SqlType sqlType)
    {
        using var context = new TestContext(sqlType);

        var entries = new List<Entry> { new() { /*EntryId = 1,*/ Name = "Custom Info" } };
        BulkConfig bulkConfig = new() { CustomSqlPostProcess = "UPDATE Entry SET Name = Name + ' 2'" };
        context.BulkInsertOrUpdate(entries, bulkConfig);

        Assert.Equal("Custom Info 2", context.Entries.OrderBy(a => a.EntryId).LastOrDefault()?.Name);

        // Sample for Audit Json on Item:
        /*
        var customSqlPostProcessOnITEM = @"
            DECLARE @NowDt DATETIME = GETDATE();

            INSERT INTO [AuditData] (OriginalTableId, JsonData, TableName, PrimaryKeyName, AuditDate, AuditAction)
            SELECT mo.ItemId AS OriginalTableId,
            (SELECT mo.ItemId, mo.TimeUpdated
                FOR JSON PATH, ROOT('Item'))
            AS JsonData,
            'Item' AS TableName,
            'ItemId' AS PrimaryKeyName,
            @NowDt AS AuditDate,
            mo.SqlActionIUD AS AuditAction
            FROM ItemTemp3c851b08Output mo
            WHERE SqlActionIUD IN ('I', 'U');
        ";
        */

    }

    [Theory]
    [InlineData(SqlType.SqlServer)]
    //[InlineData(SqlType.PostgreSql)]
    //[InlineData(SqlType.Sqlite)]
    private void CalcStatsTest(SqlType sqlType)
    {
        using var context = new TestContext(sqlType);

        var entries = new List<Entry> { new() { /*EntryId = 1,*/ Name = "Some Info" } };
        BulkConfig bulkConfig = new() { CalculateStats = true, SetOutputIdentity = true, /*SetOutputNonIdentityColumns = false, SqlBulkCopyOptions = SqlBulkCopyOptions.KeepIdentity*/ };
        context.BulkInsert(entries, bulkConfig);
    }

    [Theory]
    [InlineData(SqlType.SqlServer)]
    [InlineData(SqlType.PostgreSql)]
    [InlineData(SqlType.Sqlite)]
    private void DefaultValuesTest(SqlType sqlType)
    {
        using var context = new TestContext(sqlType);
        context.Truncate<Document>();
#pragma warning disable
        context.Documents.BatchDelete();
#pragma warning disable
        SeqGuid.Create(sqlType);

        var entities = new List<Document>();
        for (int i = 1; i <= 5; i++)
        {
            entities.Add(new Document
            {
                DocumentId = SeqGuid.Create(sqlType),
                Content = "Info " + i
            });
        };
        context.BulkInsertOrUpdate(entities, bulkConfig => bulkConfig.SetOutputIdentity = true); // example of setting BulkConfig with Action argument

        var firstDocument = context.Documents.AsNoTracking().OrderBy(x => x.Content).FirstOrDefault();

        // TEST

        Assert.Equal(entities.Count, context.Documents.Count());

        Assert.Equal(entities[0].DocumentId, firstDocument?.DocumentId);

        if (sqlType == SqlType.SqlServer)
        {
            Assert.Equal(6, firstDocument?.ContentLength);
            Assert.NotEqual(0, firstDocument?.OrderNumber);
        }
    }

    [Theory]
    [InlineData(SqlType.SqlServer)]
    private void TemporalTableTest(SqlType sqlType)
    {
        using var context = new TestContext(sqlType);
        //context.Truncate<Storage>(); // Can not be used because table is Temporal, so BatchDelete used instead
        context.Storages.BatchDelete();
        context.Database.ExecuteSqlRaw($"DBCC CHECKIDENT('{nameof(Storage)}', RESEED, 0);");

        var entities = new List<Storage>()
        {
            new Storage { Data = "Info " + 1 },
            new Storage { Data = "Info " + 2 },
            new Storage { Data = "Info " + 3 },
        };
        context.BulkInsert(entities, new BulkConfig
        {
            SetOutputIdentity = true,
        });

        var en = context.Entry(entities[0]).Property("PeriodStart").CurrentValue;
        var en2 = context.Entry(entities[0]).Property("PeriodEnd").CurrentValue;

        var countDb = context.Storages.Count();
        var countEntities = entities.Count;

        Assert.Equal(countDb, countEntities);

        var entities2 = new List<Storage>()
        {
            new Storage { StorageId = 1 },
            new Storage { StorageId = 2 },
            new Storage { StorageId = 3 },
        };
        context.BulkRead(entities2);

        // TEST
        Assert.Equal(entities[0].Data, entities2[0].Data);
    }

    [Theory]
    [InlineData(SqlType.SqlServer)]
    private void RunDefaultPKInsertWithGraph(SqlType sqlType)
    {
        using var context = new TestContext(sqlType);
        var department = new Department
        {
            Name = "Software",
            Divisions = new List<Division>
                {
                    new Division{Name = "Student A"},
                    new Division{Name = "Student B"},
                    new Division{Name = "Student C"},
                }
        };

        context.BulkInsert(new List<Department> { department }, new BulkConfig { IncludeGraph = true });
    }

    [Theory]
    [InlineData(SqlType.SqlServer)]
    public void UpsertOrderTest(SqlType sqlType)
    {
        new EFCoreBatchTest().RunDeleteAll(sqlType);

        using var context = new TestContext(sqlType);
        context.Items.Add(new Item { Name = "name 1", Description = "info 1" });
        context.Items.Add(new Item { Name = "name 2", Description = "info 2" });
        context.SaveChanges();

        var entities = new List<Item>();
        for (int i = 1; i <= 4; i++)
        {
            int j = i;
            if (i == 1) j = 2;
            if (i == 2) j = 1;
            entities.Add(new Item
            {
                Name = "name " + j,
                Description = "info x " + j,
            });
        }
        entities.Add(new Item
        {
            Name = "name 2",
            Quantity = 1,
            Description = "info x 5",
        });

        context.BulkInsertOrUpdate(entities, new BulkConfig { SetOutputIdentity = true, UpdateByProperties = new List<string> { nameof(Item.Name), nameof(Item.Quantity) } });
        Assert.Equal(2, entities[0].ItemId);
    }

    [Theory]
    [InlineData(SqlType.SqlServer)]
    [InlineData(SqlType.Sqlite)] // Does NOT have Computed Columns
    private void ComputedAndDefaultValuesTest(SqlType sqlType)
    {
        using var context = new TestContext(sqlType);
        context.Truncate<Document>();
        bool isSqlite = sqlType == SqlType.Sqlite;

        var entities = new List<Document>();
        for (int i = 1; i <= EntitiesNumber; i++)
        {
            var entity = new Document
            {
                Content = "Info " + i
            };
            if (isSqlite)
            {
                entity.DocumentId = Guid.NewGuid();
                entity.ContentLength = entity.Content.Length;
            }
            entities.Add(entity);
        }
        context.BulkInsert(entities, bulkAction => bulkAction.SetOutputIdentity = true); // example of setting BulkConfig with Action argument

        var firstDocument = context.Documents.AsNoTracking().First();
        var count = context.Documents.Count();

        // TEST

        Assert.Equal("DefaultData", firstDocument.Tag);

        firstDocument.Tag = null;
        var upsertList = new List<Document> {
            //firstDocument, // GetPropertiesWithDefaultValue .SelectMany(
            new Document { Content = "Info " + (count + 1) }, // to test adding new with InsertOrUpdate (entity having Guid DbGenerated)
            new Document { Content = "Info " + (count + 2) }
        };
        if (isSqlite)
        {
            upsertList[0].DocumentId = Guid.NewGuid(); //[1]
            upsertList[1].DocumentId = Guid.NewGuid(); //[2]
        }
        count += 2;

        context.BulkInsertOrUpdate(upsertList);
        firstDocument = context.Documents.AsNoTracking().First();
        var entitiesCount = context.Documents.Count();

        //Assert.Null(firstDocument.Tag); // OnUpdate columns with Defaults not omitted, should change even to default value, in this case to 'null'

        Assert.NotEqual(Guid.Empty, firstDocument.DocumentId);
        Assert.Equal(true, firstDocument.IsActive);
        Assert.Equal(firstDocument.Content.Length, firstDocument.ContentLength);
        Assert.Equal(entitiesCount, count);
    }

    [Theory]
    [InlineData(SqlType.SqlServer)]
    [InlineData(SqlType.Sqlite)] // Does NOT have Computed Columns
    private void ParameterlessConstructorTest(SqlType sqlType)
    {
        using var context = new TestContext(sqlType);
        context.Truncate<Letter>();

        var entities = new List<Letter>();
        int counter = 10;
        for (int i = 1; i <= counter; i++)
        {
            var entity = new Letter("Note " + i);
            entities.Add(entity);
        }
        context.BulkInsert(entities);

        var count = context.Letters.Count();
        var firstDocumentNote = context.Letters.AsNoTracking().First();

        // TEST
        Assert.Equal(counter, count);
        Assert.Equal("Note 1", firstDocumentNote.Note);
    }

    [Theory]
    [InlineData(SqlType.PostgreSql)]
    private void ArrayPGTest(SqlType sqlType)
    {
        using var context = new TestContext(sqlType);

        context.Truncate<FilePG>();

        var entities = new List<FilePG>();
        for (int i = 1; i <= EntitiesNumber; i++)
        {
            var entity = new FilePG
            {
                Description = "Array data" + i,
                Formats = new string[] { "txt", "pdf" },
            };
            entities.Add(entity);
        }

        context.BulkInsert(entities);
    }

    [Theory]
    [InlineData(SqlType.PostgreSql)]
    private void TimeStampPGTest(SqlType sqlType)
    {
        using var context = new TestContext(sqlType);

        context.Truncate<FilePG>();

        var entities = new List<FilePG>();
        for (int i = 1; i <= EntitiesNumber; i++)
        {
            var entity = new FilePG
            {
                Description = "Some data " + i,
            };
            entities.Add(entity);
        }

        context.FilePGs.AddRange(entities);

        context.BulkSaveChanges();
    }

    [Theory]
    [InlineData(SqlType.SqlServer)]
    //[InlineData(DbServer.Sqlite)] // No TimeStamp column type but can be set with DefaultValueSql: "CURRENT_TIMESTAMP" as it is in OnModelCreating() method.
    private void TimeStampTest(SqlType sqlType)
    {
        using var context = new TestContext(sqlType);

        context.Truncate<File>();

        var entities = new List<File>();
        for (int i = 1; i <= EntitiesNumber; i++)
        {
            var entity = new File
            {
                Description = "Some data " + i
            };
            entities.Add(entity);
        }

        context.BulkInsert(entities, bc => bc.SetOutputIdentity = true); // example of setting BulkConfig with Action argument

        // Test BulkRead
        var entitiesRead = new List<File>
        {
            new File { Description = "Some data 1" },
            new File { Description = "Some data 2" }
        };
        context.BulkRead(entitiesRead, new BulkConfig
        {
            UpdateByProperties = new List<string> { nameof(File.Description) }
        });
        Assert.Equal(1, entitiesRead.First().FileId);
        Assert.NotNull(entitiesRead.First().VersionChange);

        // For testing concurrency conflict (UPDATE changes RowVersion which is TimeStamp column)
        context.Database.ExecuteSqlRaw("UPDATE dbo.[File] SET Description = 'Some data 1 PRE CHANGE' WHERE [Id] = 1;");

        var entitiesToUpdate = entities.Take(10).ToList();
        foreach (var entityToUpdate in entitiesToUpdate)
        {
            entityToUpdate.Description += " UPDATED";
        }

        using var transaction = context.Database.BeginTransaction();
        var bulkConfig = new BulkConfig { SetOutputIdentity = true, DoNotUpdateIfTimeStampChanged = true };
        context.BulkUpdate(entitiesToUpdate, bulkConfig);

        var list = bulkConfig.TimeStampInfo?.EntitiesOutput.Cast<File>().ToList();
        Assert.Equal(9, list?.Count);
        Assert.Equal(1, bulkConfig.TimeStampInfo?.NumberOfSkippedForUpdate);

        if (bulkConfig.TimeStampInfo?.NumberOfSkippedForUpdate > 0)
        {
            //Options, based on needs:

            // 1. rollback entire Update
            transaction.Rollback(); // 1. rollback entire Update

            // 2. throw Exception
            //throw new DbUpdateConcurrencyException()

            // 3. Update them again

            // 4. Skip them and leave it unchanged

        }
        else
        {
            transaction.Commit();
        }
    }

    [Theory]
    [InlineData(SqlType.SqlServer)]
    [InlineData(SqlType.Sqlite)]
    [InlineData(SqlType.PostgreSql)]
    private void CompositeKeyTest(SqlType sqlType)
    {
        using var context = new TestContext(sqlType);

        context.Truncate<UserRole>();

        // INSERT
        var entitiesToInsert = new List<UserRole>();
        for (int i = 0; i < EntitiesNumber; i++)
        {
            entitiesToInsert.Add(new UserRole(i / 10, i % 10, "desc"));
        }
        context.BulkInsert(entitiesToInsert);

        // UPDATE
        var entitiesToUpdate = context.UserRoles.ToList();
        int entitiesCount = entitiesToUpdate.Count;
        for (int i = 0; i < entitiesCount; i++)
        {
            entitiesToUpdate[i].Description = "desc updated " + i;
        }
        context.BulkUpdate(entitiesToUpdate);

        var entitiesToUpsert = new List<UserRole>()
        {
            new UserRole { UserId = 1, RoleId = 1 },
            new UserRole { UserId = 2, RoleId = 2 },
            new UserRole { UserId = 100, RoleId = 10 },
        };

        // TEST
        var entities = context.UserRoles.ToList();
        Assert.Equal(EntitiesNumber, entities.Count);

        context.BulkInsertOrUpdate(entitiesToUpsert, new BulkConfig { PropertiesToInclude = new List<string> { nameof(UserRole.UserId), nameof(UserRole.RoleId) } });
        var entitiesFinal = context.UserRoles.ToList();
        Assert.Equal(EntitiesNumber + 1, entitiesFinal.Count);
    }

    [Theory]
    [InlineData(SqlType.SqlServer)]
    [InlineData(SqlType.Sqlite)]
    [InlineData(SqlType.PostgreSql)]
    private void DiscriminatorShadowTest(SqlType sqlType)
    {
        using var context = new TestContext(sqlType);

        context.BulkDelete(context.Students.ToList());

        // INSERT
        var entitiesToInsert = new List<Student>();
        for (int i = 1; i <= EntitiesNumber; i++)
        {
            entitiesToInsert.Add(new Student
            {
                Name = "name " + i,
                Subject = "Math"
            });
        }
        context.Students.AddRange(entitiesToInsert); // adding to Context so that Shadow property 'Discriminator' gets set
        context.BulkInsert(entitiesToInsert);

        // UPDATE
        var entitiesToInsertOrUpdate = new List<Student>();
        for (int i = 1; i <= EntitiesNumber / 2; i += 2)
        {
            entitiesToInsertOrUpdate.Add(new Student
            {
                Name = "name " + i,
                Subject = "Math Upd"
            });
        }
        context.Students.AddRange(entitiesToInsertOrUpdate); // adding to Context so that Shadow property 'Discriminator' gets set
        context.BulkInsertOrUpdate(entitiesToInsertOrUpdate, new BulkConfig
        {
            UpdateByProperties = new List<string> { nameof(Student.Name) },
            PropertiesToExclude = new List<string> { nameof(Student.PersonId) },
        });

        // TEST
        var entities = context.Students.ToList();
        Assert.Equal(EntitiesNumber, entities.Count);
    }

    [Theory]
    [InlineData(SqlType.SqlServer)]
    [InlineData(SqlType.Sqlite)]
    private void ValueConversionTest(SqlType sqlType)
    {
        using var context = new TestContext(sqlType);

        context.BulkDelete(context.Infos.ToList());

        var dateTime = DateTime.Today;

        // INSERT
        var entitiesToInsert = new List<Info>();
        for (int i = 1; i <= EntitiesNumber; i++)
        {
            entitiesToInsert.Add(new Info
            {
                Message = "Msg " + i,
                ConvertedTime = dateTime,
                InfoType = InfoType.InfoTypeA
            });
        }
        context.BulkInsert(entitiesToInsert);

        if (sqlType == SqlType.SqlServer)
        {
            var entities = context.Infos.ToList();
            var entity = entities.First();

            Assert.Equal(entity.ConvertedTime, dateTime);
            Assert.Equal("logged", entity.GetLogData());
            Assert.Equal(DateTime.Today, entity.GetDateCreated());

            var conn = context.Database.GetDbConnection();
            if (conn.State != ConnectionState.Open)
                conn.Open();

            using var command = conn.CreateCommand();
            command.CommandText = $"SELECT TOP 1 * FROM {nameof(Info)} ORDER BY {nameof(Info.InfoId)} DESC";
            var reader = command.ExecuteReader();
            reader.Read();
            var row = new Info()
            {
                ConvertedTime = reader.Field<DateTime>(nameof(Info.ConvertedTime))
            };
            Assert.Equal(row.ConvertedTime, dateTime.AddDays(1));
        }
    }

    [Theory]
    [InlineData(SqlType.SqlServer)]
    [InlineData(SqlType.Sqlite)]
    [InlineData(SqlType.PostgreSql)]
    private void OwnedTypesTest(SqlType sqlType)
    {
        using var context = new TestContext(sqlType);

        if (sqlType == SqlType.SqlServer)
        {
            context.Truncate<ChangeLog>();
            context.Database.ExecuteSqlRaw("TRUNCATE TABLE [" + nameof(ChangeLog) + "]");
        }
        else if (sqlType == SqlType.PostgreSql)
        {
            context.Truncate<ChangeLog>();
        }
        else
        {
            //context.ChangeLogs.BatchDelete(); // TODO
            context.BulkDelete(context.ChangeLogs.ToList());
        }

        var entities = new List<ChangeLog>();
        for (int i = 1; i <= EntitiesNumber; i++)
        {
            entities.Add(new ChangeLog
            {
                Description = "Dsc " + i,
                Audit = new Audit
                {
                    ChangedBy = "User" + 1,
                    ChangedTime = DateTime.Now,
                    InfoType = InfoType.InfoTypeA
                },
                AuditExtended = new AuditExtended
                {
                    CreatedBy = "UserS" + 1,
                    Remark = "test",
                    CreatedTime = DateTime.Now
                },
                AuditExtendedSecond = new AuditExtended
                {
                    CreatedBy = "UserS" + 1,
                    Remark = "sec",
                    CreatedTime = DateTime.Now
                }
            });
        }
        context.BulkInsert(entities);

        if (sqlType == SqlType.SqlServer || sqlType == SqlType.PostgreSql)
        {
            context.BulkRead(
                entities,
                new BulkConfig
                {
                    UpdateByProperties = new List<string> { nameof(ChangeLog.Description) }
                }
            );
            Assert.Equal(2, entities[1].ChangeLogId);
        }

        // TEST
        entities[0].Description += " UPD";
        entities[0].Audit.InfoType = InfoType.InfoTypeB;
        context.BulkUpdate(entities);
        if (sqlType == SqlType.SqlServer || sqlType == SqlType.PostgreSql)
        {
            context.BulkRead(entities);
        }
        Assert.Equal("Dsc 1 UPD", entities[0].Description);
        Assert.Equal(InfoType.InfoTypeB, entities[0].Audit.InfoType);
    }

    [Theory]
    [InlineData(SqlType.SqlServer)]
    private void OwnedTypeSpatialDataTest(SqlType sqlType)
    {
        using var context = new TestContext(sqlType);

        if (sqlType == SqlType.SqlServer)
        {
            context.Truncate<Tracker>();
            context.Database.ExecuteSqlRaw("TRUNCATE TABLE [" + nameof(Tracker) + "]");
        }
        else if (sqlType == SqlType.PostgreSql)
        {
            context.Truncate<ChangeLog>();
        }
        else
        {
            //context.ChangeLogs.BatchDelete(); // TODO
            context.BulkDelete(context.ChangeLogs.ToList());
        }

        var entities = new List<Tracker>();
        for (int i = 1; i <= EntitiesNumber; i++)
        {
            entities.Add(new Tracker
            {
                Description = "Dsc " + i,
                Location = new TrackerLocation()
                {
                    LocationName = "Anywhere",
                    Location = new Point(0, 0) { SRID = 4326 }
                }
            });
        }
        context.BulkInsert(entities);

        if (sqlType == SqlType.SqlServer || sqlType == SqlType.PostgreSql)
        {
            context.BulkRead(
                entities,
                new BulkConfig
                {
                    UpdateByProperties = new List<string> { nameof(Tracker.Description) }
                }
            );
            Assert.Equal(2, entities[1].TrackerId);
        }

        // TEST
        entities[0].Description += " UPD";
        entities[0].Location.Location = new Point(1, 1) { SRID = 4326 };
        context.BulkUpdate(entities);
        if (sqlType == SqlType.SqlServer || sqlType == SqlType.PostgreSql)
        {
            context.BulkRead(entities);
        }
        Assert.Equal("Dsc 1 UPD", entities[0].Description);
        Assert.Equal(new Point(1, 1) { SRID = 4326 }, entities[0].Location.Location);
    }

    [Theory]
    [InlineData(SqlType.SqlServer)]
    [InlineData(SqlType.PostgreSql)]
    [InlineData(SqlType.Sqlite)] //Not supported
    private void ShadowFKPropertiesTest(SqlType sqlType) // with Foreign Key as Shadow Property
    {
        using var context = new TestContext(sqlType);

        if (sqlType == SqlType.SqlServer)
        {
            context.Truncate<ItemLink>();
            context.Database.ExecuteSqlRaw("TRUNCATE TABLE [" + nameof(ItemLink) + "]");
        }
        else
        {
            //context.ChangeLogs.BatchDelete(); // TODO
            context.BulkDelete(context.ItemLinks.ToList());
        }
        //context.BulkDelete(context.Items.ToList()); // On table with FK Truncate does not work

        if (!context.Items.Any())
        {
            for (int i = 1; i <= 10; ++i)
            {
                var entity = new Item
                {
                    ItemId = 0,
                    Name = "name " + i,
                    Description = string.Concat("info ", Guid.NewGuid().ToString().AsSpan(0, 3)),
                    Quantity = i % 10,
                    Price = i / (i % 5 + 1),
                    TimeUpdated = DateTime.Now,
                    ItemHistories = new List<ItemHistory>()
                };

                context.Items.Add(entity);
            }
            context.SaveChanges();
        }

        var items = context.Items.ToList();
        var entities = new List<ItemLink>();
        for (int i = 0; i < EntitiesNumber; i++)
        {
            var itemLink = new ItemLink
            {
                ItemLinkId = 0,
                Item = items[i % items.Count]
            };

            context.Entry(itemLink).Property("Data").CurrentValue = "aaa";
            context.Entry(itemLink).Property("ItemId").CurrentValue = itemLink.Item.ItemId;

            entities.Add(itemLink);
        }

        context.ItemLinks.AddRange(entities);
        context.SaveChanges();

        //context.BulkInsert(entities);

        List<ItemLink> links = context.ItemLinks.ToList();
        Assert.True(links.Count > 0, "ItemLink row count");

        foreach (var link in links)
        {
            Assert.NotNull(link.Item);
        }

        //context.Truncate<ItemLink>();
    }

    [Theory]
    [InlineData(SqlType.SqlServer)]
    private void UpsertWithOutputSortTest(SqlType sqlType)
    {
        using var context = new TestContext(sqlType);

        new EFCoreBatchTest().RunDeleteAll(sqlType);

        var entitiesInitial = new List<Item>();
        for (int i = 1; i <= 10; ++i)
        {
            var entity = new Item { Name = "name " + i };
            entitiesInitial.Add(entity);
        }
        context.Items.AddRange(entitiesInitial);
        context.SaveChanges();

        var entities = new List<Item>()
        {
            new Item { ItemId = 0, Name = "name " + 11 + " New" },
            new Item { ItemId = 6, Name = "name " + 6 + " Updated" },
            new Item { ItemId = 5, Name = "name " + 5 + " Updated" },
            new Item { ItemId = 0, Name = "name " + 12 + " New" }
        };
        context.BulkInsertOrUpdate(entities, new BulkConfig() { SetOutputIdentity = true });

        Assert.Equal(11, entities[0].ItemId);
        Assert.Equal(6, entities[1].ItemId);
        Assert.Equal(5, entities[2].ItemId);
        Assert.Equal(12, entities[3].ItemId);
        Assert.Equal("name " + 11 + " New", entities[0].Name);
        Assert.Equal("name " + 6 + " Updated", entities[1].Name);
        Assert.Equal("name " + 5 + " Updated", entities[2].Name);
        Assert.Equal("name " + 12 + " New", entities[3].Name);
    }

    [Theory]
    [InlineData(SqlType.SqlServer)]
    [InlineData(SqlType.Sqlite)]
    [InlineData(SqlType.PostgreSql)]
    private void NoPrimaryKeyTest(SqlType sqlType)
    {
        using var context = new TestContext(sqlType);

        var list = context.Moduls.ToList();
        var bulkConfig = new BulkConfig { UpdateByProperties = new List<string> { nameof(Modul.Code) } };
        context.BulkDelete(list, bulkConfig);

        var list1 = new List<Modul>();
        var list2 = new List<Modul>();
        for (int i = 1; i <= 20; i++)
        {
            if (i <= 10)
            {
                list1.Add(new Modul
                {
                    Code = i.ToString(),
                    Name = "Name " + i.ToString("00"),
                });
            }
            list2.Add(new Modul
            {
                Code = i.ToString(),
                Name = "Name " + i.ToString("00"),
            });
        }
        context.BulkInsert(list1);
        list2[0].Name = "UPD";
        context.BulkInsertOrUpdate(list2);

        // TEST
        Assert.Equal(20, context.Moduls.ToList().Count);
    }

    [Theory]
    [InlineData(SqlType.SqlServer)]
    private void EscapeBracketTest(SqlType sqlType)
    {
        using var context = new TestContext(sqlType);

        var list0 = context.Templates.ToList();
        context.BulkDelete(list0);

        var list = new List<Template>();
        for (int i = 1; i <= 10; i++)
        {
            list.Add(new Template
            {
                Name = "Name " + i.ToString("00")
            });
        }
        context.BulkInsert(list, bc => bc.SetOutputIdentity = true);
        context.BulkUpdate(list);

        // TEST
        Assert.Equal(10, context.Templates.ToList().Count);
    }

    [Theory]
    [InlineData(SqlType.SqlServer)]
    [InlineData(SqlType.Sqlite)]
    private void NonEntityChildTest(SqlType sqlType)
    {
        using var context = new TestContext(sqlType);
        var list = context.Animals.ToList();
        context.BulkDelete(list);

        var mammalList = new List<Mammal>()
            {
                new Mammal { Name = "Cat" },
                new Mammal { Name = "Dog" }
            };
        var bulkConfig = new BulkConfig { SetOutputIdentity = true };
        context.BulkInsert(mammalList, bulkConfig, type: typeof(Animal));

        // TEST
        Assert.Equal(2, context.Animals.ToList().Count);
    }

    [Theory]
    [InlineData(SqlType.SqlServer)]
    [InlineData(SqlType.PostgreSql)]
    [InlineData(SqlType.Sqlite)]
    private void GeometryColumnTest(SqlType sqlType)
    {
        using var context = new TestContext(sqlType);

        context.BulkDelete(context.Addresses.ToList());
        
        var entities = new List<Address> {
            new Address {
                Street = "Some Street nn",
                LocationGeography = new Point(52, 13),
                LocationGeometry = new Point(52, 13),
                GeoLine = new LineString(new List<Coordinate> { new Coordinate(52, 13), new Coordinate(50, 12) }.ToArray()) { SRID = 4326 },
                GeoPoint = new Point(52, 13) { SRID = 4326 }
            },
            new Address {
                Street = "Street 2",
                LocationGeography = new Point(55, 10),
                LocationGeometry = new Point(55, 10),
                GeoLine = new LineString(new List<Coordinate> { new Coordinate(58, 12), new Coordinate(49, 8) }.ToArray()) { SRID = 4326 },
                GeoPoint = new Point(23, 9) { SRID = 4326 }
            }
        };
        
        context.BulkInsert(entities);

        Assert.Equal(2, context.Addresses.Count());
        Assert.All(context.Set<Address>().ToList(), a => Assert.Equal(2, a.GeoLine.Count));
    }

    [Fact]
    private void GeographyAndGeometryArePersistedCorrectlyTest() // GEOJson
    {
        const SqlType sqlType = SqlType.SqlServer;
        using (var context = new TestContext(sqlType))
        {
            context.BulkDelete(context.Addresses.ToList());
        }

        var point = new Point(52, 13);

        using (var context = new TestContext(sqlType))
        {
            var entities = new List<Address> {
                new Address {
                    Street = "Some Street nn",
                    LocationGeography = point,
                    LocationGeometry = point
                }
            };

            context.BulkInsertOrUpdate(entities);
        }

        using (var context = new TestContext(sqlType))
        {
            var address = context.Addresses.Single();
            Assert.Equal(point.X, address.LocationGeography.Coordinate.X);
            Assert.Equal(point.Y, address.LocationGeography.Coordinate.Y);
            Assert.Equal(point.X, address.LocationGeometry.Coordinate.X);
            Assert.Equal(point.Y, address.LocationGeometry.Coordinate.Y);
        }
    }

    [Fact]
    private void HierarchyIdColumnTest()
    {
        const SqlType sqlType = SqlType.SqlServer;
        using (var context = new TestContext(sqlType))
        {
            context.BulkDelete(context.Categories.ToList());
        }

        using (var context = new TestContext(sqlType))
        {
            var nodeIdAsString = "/1/";
            var entities = new List<Category> {
                new Category
                {
                    Name = "Root Element",
                    HierarchyDescription = HierarchyId.Parse(nodeIdAsString)
                }
            };

            context.BulkInsertOrUpdate(entities);
        }
    }

    [Fact]
    private void HierarchyIdIsPersistedCorrectlySimpleTest()
    {
        const SqlType sqlType = SqlType.SqlServer;
        using (var context = new TestContext(sqlType))
        {
            context.BulkDelete(context.Categories.ToList());
        }

        var nodeIdAsString = "/1/";

        using (var context = new TestContext(sqlType))
        {
            var entities = new List<Category> {
                new Category
                {
                    Name = "Root Element",
                    HierarchyDescription = HierarchyId.Parse(nodeIdAsString)
                }
        };
            context.BulkInsertOrUpdate(entities);
        }

        using (var context = new TestContext(sqlType))
        {
            var category = context.Categories.Single();
            Assert.Equal(nodeIdAsString, category.HierarchyDescription.ToString());
        }
    }

    [Fact]
    private void HierarchyIdIsPersistedCorrectlyLargerHierarchyTest()
    {
        const SqlType sqlType = SqlType.SqlServer;
        using (var context = new TestContext(sqlType))
        {
            context.BulkDelete(context.Categories.ToList());
        }

        var nodeIdAsString = "/1.1/-2/3/4/5/";

        using (var context = new TestContext(sqlType))
        {
            var entities = new List<Category> {
                new Category
                {
                    Name = "Deep Element",
                    HierarchyDescription = HierarchyId.Parse(nodeIdAsString)
                }
        };
            context.BulkInsertOrUpdate(entities);
        }

        using (var context = new TestContext(sqlType))
        {
            var category = context.Categories.Single();
            Assert.Equal(nodeIdAsString, category.HierarchyDescription.ToString());
        }
    }

    [Theory]
    [InlineData(SqlType.SqlServer)]
    [InlineData(SqlType.PostgreSql)]
    [InlineData(SqlType.MySql)]
    private void DestinationAndSourceTableNameTest(SqlType sqlType)
    {
        using var context = new TestContext(sqlType);

        context.Truncate<Entry>();
        context.Truncate<EntryPrep>();
        context.Truncate<EntryArchive>();

        var entities = new List<Entry>();
        for (int i = 1; i <= 10; i++)
        {
            var entity = new Entry
            {
                Name = "Name " + i,
            };
            entities.Add(entity);
        }
        // [DEST]
        context.BulkInsert(entities, b => b.CustomDestinationTableName = nameof(EntryArchive)); // Insert into table 'EntryArchive'
        Assert.Equal(10, context.EntryArchives.Count());

        // [SOURCE] (With CustomSourceTableName list not used so can be empty)
        context.BulkInsert(new List<Entry>(), b => b.CustomSourceTableName = nameof(EntryArchive)); // InsertOrMERGE from table 'EntryArchive' into table 'Entry'
        Assert.Equal(10, context.Entries.Count());

        var entities2 = new List<EntryPrep>();
        for (int i = 1; i <= 20; i++)
        {
            var entity = new EntryPrep
            {
                NameInfo = "Name Info " + i,
            };
            entities2.Add(entity);
        }
        context.EntryPreps.AddRange(entities2);
        context.SaveChanges();

        var mappings = new Dictionary<string, string>
        {
            { nameof(EntryPrep.EntryPrepId), nameof(Entry.EntryId) }, // here used 'nameof(Prop)' since Columns have the same name as Props
            { nameof(EntryPrep.NameInfo), nameof(Entry.Name) }        // if columns they were different name then they would be set with string names, eg. "EntryPrepareId"
        };
        var bulkConfig = new BulkConfig
        {
            CustomSourceTableName = nameof(EntryPrep),
            CustomSourceDestinationMappingColumns = mappings,
            //UpdateByProperties = new List<string> { "Name" }        // with this all are insert since names are different
        };
        // [SOURCE]
        context.BulkInsertOrUpdate(new List<Entry>(), bulkConfig); // InsertOrMERGE from table 'EntryPrep' into table 'Entry'
        Assert.Equal(20, context.Entries.Count());
    }

    [Theory]
    [InlineData(SqlType.SqlServer)]
    private void TablePerTypeInsertTest(SqlType sqlType)
    {
        using var context = new TestContext(sqlType);

        context.LogPersonReports.Add(new LogPersonReport { }); // used for initial add so that after RESEED it starts from 1, not 0
        context.SaveChanges();
        context.Truncate<LogPersonReport>();
        context.Database.ExecuteSqlRaw($"DELETE FROM {nameof(Log)}");
        context.Database.ExecuteSqlRaw($"DBCC CHECKIDENT('{nameof(Log)}', RESEED, 0);");

        int nextLogId = GetLastRowId(context, tableName: nameof(Log));
        int numberOfNewToInsert = 1000;

        var entities = new List<LogPersonReport>();
        for (int i = 1; i <= numberOfNewToInsert; i++)
        {
            nextLogId++; // OPTION 1.
            var entity = new LogPersonReport
            {
                LogId = nextLogId, // OPTION 1.
                PersonId = (i % 22),
                RegBy = 15,
                CreatedDate = DateTime.Now,

                ReportId = (i % 22) * 10,
                LogPersonReportTypeId = 4,
            };
            entities.Add(entity);
        }

        var bulkConfigBase = new BulkConfig
        {
            SqlBulkCopyOptions = SqlBulkCopyOptions.KeepIdentity, // OPTION 1. - to ensure insert order is kept the same since SqlBulkCopy does not guarantee it.
            PropertiesToInclude = new List<string>
                {
                    nameof(LogPersonReport.LogId),
                    nameof(LogPersonReport.PersonId),
                    nameof(LogPersonReport.RegBy),
                    nameof(LogPersonReport.CreatedDate)
                }
        };
        var bulkConfig = new BulkConfig
        {
            PropertiesToInclude = new List<string> {
                    nameof(LogPersonReport.LogId),
                    nameof(LogPersonReport.ReportId),
                    nameof(LogPersonReport.LogPersonReportTypeId)
                }
        };
        context.BulkInsert(entities, bulkConfigBase, type: typeof(Log)); // to base 'Log' table

        //foreach(var entity in entities) { // OPTION 2. Could be set here if Id of base table Log was set by Db (when Op.2. used 'Option 1.' have to be commented out)
        //    entity.LogId = ++nextLogId;
        //}

        context.BulkInsert(entities, bulkConfig); // to 'LogPersonReport' table

        Assert.Equal(nextLogId, context.LogPersonReports.OrderByDescending(a => a.LogId).FirstOrDefault()?.LogId);
    }

    [Fact]
    private void TableWithSpecialRowVersion()
    {
        const SqlType sqlType = SqlType.SqlServer;
        using var context = new TestContext(sqlType);
        context.AtypicalRowVersionEntities.BatchDelete();
        context.AtypicalRowVersionConverterEntities.BatchDelete();

        var bulk = new List<AtypicalRowVersionEntity>();
        for (var i = 0; i < 100; i++)
            bulk.Add(new AtypicalRowVersionEntity { Id = Guid.NewGuid(), Name = $"Row {i}", RowVersion = i, SyncDevice = "Test" });

        //Assert.Throws<InvalidOperationException>(() => context.BulkInsertOrUpdate(bulk)); // commented since when running in Debug mode it pauses on Exception
        context.BulkInsertOrUpdate(bulk, new BulkConfig { IgnoreRowVersion = true });
        Assert.Equal(bulk.Count, context.AtypicalRowVersionEntities.Count());

        var bulk2 = new List<AtypicalRowVersionConverterEntity>();
        for (var i = 0; i < 100; i++)
            bulk2.Add(new AtypicalRowVersionConverterEntity { Id = Guid.NewGuid(), Name = $"Row {i}" });
        context.BulkInsertOrUpdate(bulk2);
        Assert.Equal(bulk2.Count, context.AtypicalRowVersionConverterEntities.Count());
    }

    private static int GetLastRowId(DbContext context, string tableName)
    {
        var sqlConnection = context.Database.GetDbConnection();
        sqlConnection.Open();
        using var command = sqlConnection.CreateCommand();
        command.CommandText = $"SELECT IDENT_CURRENT('{tableName}')";
        int lastRowIdScalar = Convert.ToInt32(command.ExecuteScalar());
        return lastRowIdScalar;
    }

    [Fact]
    private void TimeStamp2PGTest()
    {
        const SqlType sqlType = SqlType.PostgreSql;
        using var context = new TestContext(sqlType);

        context.BulkDelete(context.Events.ToList());

        var entities = new List<Event>();
        for (int i = 1; i <= 10; i++)
        {
            var entity = new Event
            {
                Name = "Event " + i,
                TimeCreated = DateTime.Now
            };
        }
        context.BulkInsert(entities);
    }

    [Theory]
    [InlineData(SqlType.SqlServer)]
    private void CustomPrecisionDateTimeTest(SqlType sqlType)
    {
        using var context = new TestContext(sqlType);

        context.BulkDelete(context.Events.ToList());

        var entities = new List<Event>();
        for (int i = 1; i <= 10; i++)
        {
            var entity = new Event
            {
                Name = "Event " + i,
                TimeCreated = DateTime.Now
            };
            var testTime = new DateTime(2020, 1, 1, 12, 45, 20, 324);
            if (i == 1)
            {
                entity.TimeCreated = testTime.AddTicks(6387); // Ticks will be 3256387 when rounded to 3 digits: 326 ms
            }
            if (i == 2)
            {
                entity.TimeCreated = testTime.AddTicks(5000); // Ticks will be 3255000 when rounded to 3 digits: 326 ms (middle .5zeros goes to Upper)
            }

            var fullDateTimeFormat = "yyyy-MM-dd HH:mm:ss.fffffff";
            entity.Description = entity.TimeCreated.ToString(fullDateTimeFormat);

            entities.Add(entity);
        }

        bool useBulk = true;
        if (useBulk)
        {
            context.BulkInsert(entities, b => b.DateTime2PrecisionForceRound = false);
        }
        else
        {
            context.AddRange(entities);
            context.SaveChanges();
        }

        // TEST
        Assert.Equal(3240000, context.Events.SingleOrDefault(a => a.Name == "Event 1")?.TimeCreated.Ticks % 10000000);
        Assert.Equal(3240000, context.Events.SingleOrDefault(a => a.Name == "Event 2")?.TimeCreated.Ticks % 10000000);
    }

    [Fact]
    private void ByteArrayPKBulkReadTest()
    {
        const SqlType sqlType = SqlType.Sqlite;
        using var context = new TestContext(sqlType);

        var list = context.Archives.ToList();
        if (list.Count > 0)
        {
            context.Archives.RemoveRange(list);
            context.SaveChanges();
        }

        var byte1 = new byte[] { 0x10, 0x10 };
        var byte2 = new byte[] { 0x20, 0x20 };
        var byte3 = new byte[] { 0x30, 0x30 };
        context.Archives.AddRange(
            new Archive { ArchiveId = byte1, Description = "Desc1" },
            new Archive { ArchiveId = byte2, Description = "Desc2" },
            new Archive { ArchiveId = byte3, Description = "Desc3" }
        );
        context.SaveChanges();

        var entities = new List<Archive>
        {
            new Archive { ArchiveId = byte1 },
            new Archive { ArchiveId = byte2 }
        };
        context.BulkRead(entities);

        Assert.Equal("Desc1", entities[0].Description);
        Assert.Equal("Desc2", entities[1].Description);
    }

    [Theory]
    [InlineData(SqlType.SqlServer)]
    [InlineData(SqlType.Sqlite)]
    [InlineData(SqlType.PostgreSql)]
    private void UpsertWithOnUpdateNonPK(SqlType sqlType)
    {
        using var context = new TestContext(sqlType);

        context.Truncate<Customer>();
        if (sqlType == SqlType.Sqlite)
        {
            context.Database.ExecuteSqlRaw($"UPDATE SQLITE_SEQUENCE SET SEQ=0 WHERE NAME='{nameof(Customer)}';");
            context.SaveChanges();
        }

        var cust = new Customer() { Name = "Kayle" };

        context.Customers.Add(cust);
        context.SaveChanges();

        var customers = new List<Customer>();
        customers.Add(new Customer() { Name = "John" });
        customers.Add(new Customer() { Name = "Smith" });
        customers.Add(new Customer() { Name = "Kayle" });

        /*customers[0].Id = 3; // Id set in Property
        customers[1].Id = 0;
        customers[2].Id = 0;*/

        using var context2 = new TestContext(sqlType);

        var bulkConfig = new BulkConfig
        {
            SetOutputIdentity = true,
            UpdateByProperties = new List<string> { nameof(Customer.Name) },
            //SqlBulkCopyOptions = Microsoft.Data.SqlClient.SqlBulkCopyOptions.KeepIdentity, // use it when Id is set in Property
        };
        context2.BulkInsertOrUpdate(customers, bulkConfig);

        if (sqlType == SqlType.Sqlite)
        {
            context2.BulkRead(customers, b =>
            {
                b.UpdateByProperties = new List<string> { nameof(Customer.Name) };
            });

            Assert.Equal(1, customers[2].Id);
            Assert.Equal(2, customers[0].Id);
            Assert.Equal(3, customers[1].Id);
        }
    }

    [Theory]
    [InlineData(SqlType.SqlServer)]
    [InlineData(SqlType.Sqlite)]
    [InlineData(SqlType.PostgreSql)]
    private void PrivateKeyTest(SqlType sqlType)
    {
        using (var context = new TestContext(sqlType))
        {
            context.BulkDelete(context.PrivateKeys.ToList());
        }

        using (var context = new TestContext(sqlType))
        {
            var entities = new List<PrivateKey> {
                new()
                {
                    Name = "foo"
                }
            };
            context.BulkInsertOrUpdate(entities);

            context.BulkUpdate(entities);
        }

        using (var context = new TestContext(sqlType))
        {
            var defaultValueTest = context.PrivateKeys.Single();
            Assert.Equal("foo", defaultValueTest.Name);
        }
    }

    [Theory]
    [InlineData(SqlType.SqlServer)]
    [InlineData(SqlType.Sqlite)]
    private void ReplaceReadEntitiesTest(SqlType sqlType)
    {
        using var context = new TestContext(sqlType);

        new EFCoreBatchTest().RunDeleteAll(sqlType);

        var list = new List<Item>
        {
            new Item { Name = "name 1" },
            new Item { Name = "name 2" },
            new Item { Name = "name 2" },
            new Item { Name = "name 3" }
        };
        context.Items.AddRange(list);
        context.SaveChanges();

        var names = new List<string> { "name 1", "name 2", "name 3", "name 4" };

        var items = names.Select(i => new Item { Name = i }).ToList();

        var config = new BulkConfig()
        {
            ReplaceReadEntities = true,
            UpdateByProperties = new List<string> { nameof(Item.Name) },
        };

        context.BulkRead(items, config);

        Assert.Equal(4, items.Count);
        Assert.Equal(2, items.Where(i => i.Name == "name 2").Count());
        Assert.Empty(items.Where(i => i.Name == "name 4"));
    }

    [Theory]
    [InlineData(SqlType.SqlServer)]
    [InlineData(SqlType.PostgreSql)]
    //[InlineData(SqlType.Sqlite)] // post v 8.0
    private void JsonTest(SqlType sqlType)
    {
        using var context = new TestContext(sqlType);

        context.Truncate<Author>();

        var list = new List<Author>
        {
            new Author
            {
                Name = "At",
                Contact = new ContactDetails
                {
                    Phone = "123-456",
                    Address = new AddressCD ( "Str1", "Ct", "10000", "" )
                }
            }
        };

        context.BulkInsert(list);

        list[0].Id = 1;
        list[0].Name = "At2";
        list[0].Contact.Address.Street = "Str2";
        context.BulkUpdate(list);

        var author = context.Authors.FirstOrDefault()!;

        Assert.Equal("123-456", author.Contact.Phone);

        Assert.Equal("At2", author.Name);
        Assert.Equal("Str2", author.Contact.Address.Street);

        var list2 = new List<Author>
        {
            new Author
            {
                Id = 1
            }
        };
        context.BulkRead(list2);
        Assert.Equal("At2", list2.FirstOrDefault()?.Name);

        var list3 = new List<Author>
        {
            new Author
            {
                Id = 1,
                Name = "At3",
            }
        };
        var bulkConfig = new BulkConfig { PropertiesToInclude = new List<string> { "Name" } };
        context.BulkUpdate(list3, bulkConfig);
        var authorReloaded = context.Authors.FirstOrDefault()!;
        Assert.NotNull(authorReloaded.Contact);
    }
    
    [Fact]
    public void PGArrayColumn()
    {
        // Test postgresql array column (int[])
        const SqlType sqlType = SqlType.PostgreSql;
        using var context = new TestContext(sqlType);

        context.Truncate<ArrayModel>();
        
        var list = new List<ArrayModel>
        {
            new()
            {
                Id = 1,
                Array = new [] { "1", "2", "3" },
                List = new List<int>(),
                EnumArray = Enum.GetValues<BindingFlags>(),
                Enum = BindingFlags.CreateInstance
            },
            new()
            {
                Id = 2,
                Array = null,
                List = new List<int>() { 1, 2},
                EnumArray = new [] { BindingFlags.Static },
                Enum = BindingFlags.DeclaredOnly
            },
        };

        context.BulkInsertOrUpdate(list);

        var result = context.Set<ArrayModel>().AsNoTracking().ToList();
        Assert.Equal(2, result.Count);
        Assert.Equivalent(list, result, true);
    }

    [Theory]
    [InlineData(SqlType.PostgreSql)]
    private void XminTest(SqlType sqlType)
    {
        using var context = new TestContext(sqlType);

        context.Truncate<Partner>();

        var list = new List<Partner>
        {
            new Partner { Name = "Aa1", FirstName = "Ab2" },
            new Partner { Name = "Ba1", FirstName = "Bb2" }
        };

        //context.BulkInsert(list);

        context.Partners.AddRange(list);
        context.SaveChanges();

        var first = context.Partners.FirstOrDefault();
        var list2 = new List<Partner>
        {
            new Partner
            {
                Id = first?.Id ?? Guid.Empty
            }
        };

        var bulkConfig = new BulkConfig
        {
            UpdateByProperties = new List<string> { nameof(Partner.Id) },
            PropertiesToInclude = new List<string> { nameof(Partner.Id), nameof(Partner.Name) }
        };
        //context.BulkRead(list2, bulkConfig); // Throws: 'The required column 'xmin' was not present in the results of a 'FromSql' operation.'
    }

    [Theory]
    [InlineData(SqlType.SqlServer)]
    private void DataReaderTest(SqlType sqlType)
    {
        using var context = new TestContext(sqlType);

        context.Truncate<Customer>();

        var entities = new List<Customer>
        {
            new Customer { Name = "Cust 1" },
            new Customer { Name = "Cust 2" },
        };

        using var reader = ObjectReader.Create(entities);
        context.BulkInsert(new List<Customer>(), new BulkConfig { DataReader = reader }); // , EnableStreaming = true
    }

    [Theory]
    [InlineData(SqlType.SqlServer)]
    private void ParallelsTestAsync(SqlType sqlType)
    {
        var entitiesLists = new List<List<Customer>>();

        for (int l = 0; l < 10; l++)
        {
            var entities = new List<Customer>();
            for (int i = 1; i <= 10000; i++)
            {
                int k = l * 10000 + i;
                var entity = new Customer
                {
                    Name = "name " + k,
                };
                entities.Add(entity);
            }
            entitiesLists.Add(entities);
        }

        using var context = new TestContext(sqlType);
        context.Truncate<Customer>();

        Parallel.ForEach(entitiesLists, chunk =>
            {
                context.BulkInsertAsync(chunk);
            }
        );
    }

    [Theory]
    [InlineData(SqlType.SqlServer)]
    public void UpdateOrderGraphTest(SqlType sqlType)
    {
        using var context = new TestContext(sqlType);

        context.Database.ExecuteSql($"delete from dbo.ParentType;");

        // Add the starting objects to the database
        List<ParentType> startingItems =
        [
            new ParentType
            {
                ParentTypeKey = 1,
                ParentLabel = "ParentLabel_1",
                Children =
                [
                    new ChildType() { ChildTypeKey = 1, ParentTypeKey = 1, ChildLabel = "Child_1_Label" }
                ]
            },
            new ParentType
            {
                ParentTypeKey = 2,
                ParentLabel = "ParentLabel_2",
                Children =
                [
                    new ChildType() { ChildTypeKey = 2, ParentTypeKey = 2, ChildLabel = "Child_2_Label" }
                ]
            }
        ];

        context.ParentTypes.AddRange(startingItems);
        context.SaveChangesAsync();
        // Display starting data
        foreach (var p in context.ParentTypes.Include(p => p.Children).AsNoTracking())
        {
            Console.WriteLine(p.ToString());
            foreach (var c in p.Children) { Console.WriteLine(c.ToString()); }
        }

        // Issue manifests only if all items of a type have edits
        // Only change to data from starting items is ParentLabel property edited to "..._Updated" for all ParentItems.
        // Note - Order of incoming (or edited) items is not ParentTypeKey ASC
        List<ParentType> editedItems =
        [
            new ParentType
            {
                ParentTypeKey = 2,
                ParentLabel = "ParentLabel_2_Updated",
                Children =
                [
                    new ChildType() { ChildTypeKey = 2, ParentTypeKey = 2, ChildLabel = "Child_2_Label" }
                ]
            },
            new ParentType
            {
                ParentTypeKey = 1,
                ParentLabel = "ParentLabel_1_Updated",
                Children =
                [
                    new ChildType() { ChildTypeKey = 1, ParentTypeKey = 1, ChildLabel = "Child_1_Label" }
                ]
            }
        ];
        context.BulkUpdate<ParentType>(editedItems, new BulkConfig { UseTempDB = true, IncludeGraph = true });

        // Show new results
        // Notice now the child items foreign key values are incorrect.
        Console.WriteLine("Results:");
        foreach (var p in context.ParentTypes.Include(p => p.Children).AsNoTracking())
        {
            Console.WriteLine(p.ToString());
            foreach (var c in p.Children) { Console.WriteLine(c.ToString()); }
        }
    }

    [Fact]
    public async static void ConverterStringPKTest() //for issue 1343
    {
        using var context = new TestContext(SqlType.PostgreSql);

        var playerId = PlayerId.Create("--wvkCLQSFiG9xpBbTTXBg");
        var modStat1 = ModStat.Create("000NeMAdR1Cx6FVye4UV6Q", playerId);
        var modStatLis = new List<ModStat>
        {
            modStat1
        };
        var mod = Mod.Create("000NeMAdR1Cx6FVye4UV6Q", playerId, modStatLis);

        await context.Mods.AddAsync(mod);
        await context.BulkSaveChangesAsync();
    }

    [Fact]
    public async static void GrphOwnedTest() //for issue 1337
    {
        using var context = new TestContext(SqlType.SqlServer);

        // CalculateStats = true throws: Cannot create a DbSet for 'Location' because it is configured as an owned entity type
        var bulkConfig = new BulkConfig { IncludeGraph = true, CalculateStats = false };

        Client[] entities = [
        new()
        {
            ClientId = "test1",
            ContactMethods = new()
            {
                HomePhone = "homephone1",
                LocationAdresses = [
                    new Location() { Address = "email1", Type = "emailtype1" },
                    new Location() { Address = "email2", Type = "emailtype2" },
                ]
            },
        }
            ];

        context.BulkInsertOrUpdate(entities, bulkConfig);
    }
}
