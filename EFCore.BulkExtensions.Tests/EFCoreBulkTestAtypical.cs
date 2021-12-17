using EFCore.BulkExtensions.SqlAdapters;
using Microsoft.EntityFrameworkCore;
using NetTopologySuite.Geometries;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using Xunit;

namespace EFCore.BulkExtensions.Tests
{
    public class EFCoreBulkTestAtypical
    {
        protected int EntitiesNumber => 1000;

        [Theory]
        [InlineData(DbServer.SQLServer)]
        [InlineData(DbServer.SQLite)]
        private void DefaultValuesTest(DbServer dbServer)
        {
            ContextUtil.DbServer = dbServer;
            using var context = new TestContext(ContextUtil.GetOptions());
            context.Truncate<Document>();
            context.Documents.BatchDelete();
            bool isSqlite = dbServer == DbServer.SQLite;

            var entities = new List<Document>() 
            {
                new Document { DocumentId = Guid.Parse("15E5936C-8021-45F4-A055-2BE89B065D9E"), Content = "Info " + 1 },
                new Document { DocumentId = Guid.Parse("00C69E47-A08F-49E0-97A6-56C62C9BB47E"), Content = "Info " + 2 },
                new Document { DocumentId = Guid.Parse("22CF94AE-20D3-49DE-83FA-90E79DD94706"), Content = "Info " + 3 },
                new Document { DocumentId = Guid.Parse("B3A2F9A5-4222-47C3-BEEA-BF50771665D3"), Content = "Info " + 4 },
                new Document { DocumentId = Guid.Parse("12AF6361-95BC-44F3-A487-C91C440018D8"), Content = "Info " + 5 },
            };
            var firstDocumentUp = entities.FirstOrDefault();
            context.BulkInsertOrUpdate(entities, bulkConfig => bulkConfig.SetOutputIdentity = true); // example of setting BulkConfig with Action argument

            var firstDocument = context.Documents.AsNoTracking().OrderBy(x => x.Content).FirstOrDefault();

            var countDb = context.Documents.Count();
            var countEntities = entities.Count();

            // TEST

            Assert.Equal(countDb, countEntities);

            Assert.Equal(firstDocument.DocumentId, firstDocumentUp.DocumentId);
        }

        [Theory]
        [InlineData(DbServer.SQLServer)]
        private void TemporalTableTest(DbServer dbServer)
        {
            ContextUtil.DbServer = dbServer;
            using var context = new TestContext(ContextUtil.GetOptions());
            //context.Truncate<Document>(); // Can not be used because table is Temporal, so BatchDelete used instead
            context.Storages.BatchDelete();

            var entities = new List<Storage>()
            {
                new Storage { StorageId = Guid.NewGuid(), Data = "Info " + 1 },
                new Storage { StorageId = Guid.NewGuid(), Data = "Info " + 2 },
                new Storage { StorageId = Guid.NewGuid(), Data = "Info " + 3 },
            };
            context.BulkInsert(entities);

            var countDb = context.Storages.Count();
            var countEntities = entities.Count();

            // TEST
            Assert.Equal(countDb, countEntities);
        }

        [Theory]
        [InlineData(DbServer.SQLServer)]
        private void RunDefaultPKInsertWithGraph(DbServer dbServer)
        {
            using (var context = new TestContext(ContextUtil.GetOptions()))
            {
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
            };
        }

        [Theory]
        [InlineData(DbServer.SQLServer)]
        public void UpsertOrderTest(DbServer dbServer)
        {
            ContextUtil.DbServer = dbServer;
            new EFCoreBatchTest().RunDeleteAll(dbServer);

            using var context = new TestContext(ContextUtil.GetOptions());
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

            context.BulkInsertOrUpdate(entities, new BulkConfig { SetOutputIdentity = true, UpdateByProperties = new List<string> { nameof(Item.Name) } });
            Assert.Equal(2, entities[0].ItemId);
        }

        [Theory]
        [InlineData(DbServer.SQLServer)]
        [InlineData(DbServer.SQLite)] // Does NOT have Computed Columns
        private void ComputedAndDefaultValuesTest(DbServer dbServer)
        {
            ContextUtil.DbServer = dbServer;
            using var context = new TestContext(ContextUtil.GetOptions());
            context.Truncate<Document>();
            bool isSqlite = dbServer == DbServer.SQLite;

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

            var firstDocument = context.Documents.AsNoTracking().FirstOrDefault();
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
            firstDocument = context.Documents.AsNoTracking().FirstOrDefault();
            var entitiesCount = context.Documents.Count();

            //Assert.Null(firstDocument.Tag); // OnUpdate columns with Defaults not omitted, should change even to default value, in this case to 'null'

            Assert.NotEqual(Guid.Empty, firstDocument.DocumentId);
            Assert.Equal(true, firstDocument.IsActive);
            Assert.Equal(firstDocument.Content.Length, firstDocument.ContentLength);
            Assert.Equal(entitiesCount, count);
        }

        [Theory]
        [InlineData(DbServer.SQLServer)]
        //[InlineData(DbServer.Sqlite)] // No TimeStamp column type but can be set with DefaultValueSql: "CURRENT_TIMESTAMP" as it is in OnModelCreating() method.
        private void TimeStampTest(DbServer dbServer)
        {
            ContextUtil.DbServer = dbServer;
            using var context = new TestContext(ContextUtil.GetOptions());

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
            context.BulkInsert(entities, bulkAction => bulkAction.SetOutputIdentity = true); // example of setting BulkConfig with Action argument

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
            Assert.Equal(9, list.Count());
            Assert.Equal(1, bulkConfig.TimeStampInfo.NumberOfSkippedForUpdate);

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
        [InlineData(DbServer.SQLServer)]
        [InlineData(DbServer.SQLite)]
        private void CompositeKeyTest(DbServer dbServer)
        {
            ContextUtil.DbServer = dbServer;
            using var context = new TestContext(ContextUtil.GetOptions());

            context.Truncate<UserRole>();

            // INSERT
            var entitiesToInsert = new List<UserRole>();
            for (int i = 0; i < EntitiesNumber; i++)
            {
                entitiesToInsert.Add(new UserRole
                {
                    UserId = i / 10,
                    RoleId = i % 10,
                    Description = "desc"
                });
            }
            context.BulkInsert(entitiesToInsert);

            // UPDATE
            var entitiesToUpdate = context.UserRoles.ToList();
            int entitiesCount = entitiesToUpdate.Count();
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
            Assert.Equal(EntitiesNumber, entities.Count());

            context.BulkInsertOrUpdate(entitiesToUpsert, new BulkConfig { PropertiesToInclude = new List<string> { nameof(UserRole.UserId), nameof(UserRole.RoleId) } });
            var entitiesFinal = context.UserRoles.ToList();
            Assert.Equal(EntitiesNumber + 1, entitiesFinal.Count());
        }

        [Theory]
        [InlineData(DbServer.SQLServer)]
        [InlineData(DbServer.SQLite)]
        private void DiscriminatorShadowTest(DbServer dbServer)
        {
            ContextUtil.DbServer = dbServer;
            using var context = new TestContext(ContextUtil.GetOptions());

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
            Assert.Equal(EntitiesNumber, entities.Count());
        }

        [Theory]
        [InlineData(DbServer.SQLServer)]
        [InlineData(DbServer.SQLite)]
        private void ValueConversionTest(DbServer dbServer)
        {
            ContextUtil.DbServer = dbServer;
            using var context = new TestContext(ContextUtil.GetOptions());

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

            if (dbServer == DbServer.SQLServer)
            {
                var entities = context.Infos.ToList();
                var entity = entities.FirstOrDefault();

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
        [InlineData(DbServer.SQLServer)]
        [InlineData(DbServer.SQLite)]
        private void OwnedTypesTest(DbServer dbServer)
        {
            ContextUtil.DbServer = dbServer;
            using var context = new TestContext(ContextUtil.GetOptions());

            if (dbServer == DbServer.SQLServer)
            {
                context.Truncate<ChangeLog>();
                context.Database.ExecuteSqlRaw("TRUNCATE TABLE [" + nameof(ChangeLog) + "]");
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

            if (dbServer == DbServer.SQLServer)
            {
                context.BulkRead(
                    entities,
                    new BulkConfig
                    {
                        UpdateByProperties = new List<string> { nameof(Item.Description) }
                    }
                );
                Assert.Equal(2, entities[1].ChangeLogId);
            }

            // TEST
            entities[0].Description += " UPD";
            entities[0].Audit.InfoType = InfoType.InfoTypeB;
            context.BulkUpdate(entities);
            if (dbServer == DbServer.SQLServer)
            {
                context.BulkRead(entities);
            }
            Assert.Equal("Dsc 1 UPD", entities[0].Description);
            Assert.Equal(InfoType.InfoTypeB, entities[0].Audit.InfoType);
        }

        [Theory]
        [InlineData(DbServer.SQLServer)]
        //[InlineData(DbServer.Sqlite)] Not supported
        private void ShadowFKPropertiesTest(DbServer dbServer) // with Foreign Key as Shadow Property
        {
            ContextUtil.DbServer = dbServer;
            using var context = new TestContext(ContextUtil.GetOptions());

            if (dbServer == DbServer.SQLServer)
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


            if (context.Items.Count() == 0)
            {
                for (int i = 1; i <= 10; ++i)
                {
                    var entity = new Item
                    {
                        ItemId = 0,
                        Name = "name " + i,
                        Description = "info " + Guid.NewGuid().ToString().Substring(0, 3),
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
                entities.Add(new ItemLink
                {
                    ItemLinkId = 0,
                    Item = items[i % items.Count]
                });
            }
            context.BulkInsert(entities);

            if (dbServer == DbServer.SQLServer)
            {
                List<ItemLink> links = context.ItemLinks.ToList();
                Assert.True(links.Count() > 0, "ItemLink row count");

                foreach (var link in links)
                {
                    Assert.NotNull(link.Item);
                }
            }
            context.Truncate<ItemLink>();
        }

        [Theory]
        [InlineData(DbServer.SQLServer)]
        private void UpsertWithOutputSortTest(DbServer dbServer)
        {
            ContextUtil.DbServer = dbServer;
            using var context = new TestContext(ContextUtil.GetOptions());

            new EFCoreBatchTest().RunDeleteAll(dbServer);

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
        [InlineData(DbServer.SQLServer)]
        [InlineData(DbServer.SQLite)]
        private void NoPrimaryKeyTest(DbServer dbServer)
        {
            ContextUtil.DbServer = dbServer;
            using var context = new TestContext(ContextUtil.GetOptions());

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
            Assert.Equal(20, context.Moduls.ToList().Count());
        }

        [Theory]
        [InlineData(DbServer.SQLServer)]
        [InlineData(DbServer.SQLite)]
        private void NonEntityChildTest(DbServer dbServer)
        {
            ContextUtil.DbServer = dbServer;

            using var context = new TestContext(ContextUtil.GetOptions());
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
            Assert.Equal(2, context.Animals.ToList().Count());
        }

        [Fact]
        private void GeometryColumnTest()
        {
            ContextUtil.DbServer = DbServer.SQLServer;
            using var context = new TestContext(ContextUtil.GetOptions());

            context.BulkDelete(context.Addresses.ToList());

            var entities = new List<Address> {
                    new Address {
                        Street = "Some Street nn",
                        LocationGeography = new Point(52, 13),
                        LocationGeometry = new Point(52, 13),
                    }
                };

            context.BulkInsertOrUpdate(entities);
        }

        [Fact]
        private void GeographyAndGeometryArePersistedCorrectlyTest()
        {
            ContextUtil.DbServer = DbServer.SQLServer;
            using (var context = new TestContext(ContextUtil.GetOptions()))
            {
                context.BulkDelete(context.Addresses.ToList());
            }

            var point = new Point(52, 13);

            using (var context = new TestContext(ContextUtil.GetOptions()))
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

            using (var context = new TestContext(ContextUtil.GetOptions()))
            {
                var address = context.Addresses.Single();
                Assert.Equal(point.X, address.LocationGeography.Coordinate.X);
                Assert.Equal(point.Y, address.LocationGeography.Coordinate.Y);
                Assert.Equal(point.X, address.LocationGeometry.Coordinate.X);
                Assert.Equal(point.Y, address.LocationGeometry.Coordinate.Y);
            }

        }

        [Fact]
        private void TablePerTypeInsertTest()
        {
            ContextUtil.DbServer = DbServer.SQLServer;
            using var context = new TestContext(ContextUtil.GetOptions());

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
                SqlBulkCopyOptions = Microsoft.Data.SqlClient.SqlBulkCopyOptions.KeepIdentity, // OPTION 1. - to ensure insert order is kept the same since SqlBulkCopy does not guarantee it.
                CustomDestinationTableName = nameof(Log),
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

            Assert.Equal(nextLogId, context.LogPersonReports.OrderByDescending(a => a.LogId).FirstOrDefault().LogId);
        }

        [Fact]
        private void TableWithSpecialRowVersion()
        {
            ContextUtil.DbServer = DbServer.SQLServer;
            using var context = new TestContext(ContextUtil.GetOptions());
            context.AtypicalRowVersionEntities.BatchDelete();
            context.AtypicalRowVersionConverterEntities.BatchDelete();

            var bulk = new List<AtypicalRowVersionEntity>();
            for (var i = 0; i < 100; i++)
                bulk.Add(new AtypicalRowVersionEntity { Id = Guid.NewGuid(), Name = $"Row {i}", RowVersion = i, SyncDevice = "Test" });

            //Assert.Throws<InvalidOperationException>(() => context.BulkInsertOrUpdate(bulk)); // commented since when running in Debug mode it pauses on Exception
            context.BulkInsertOrUpdate(bulk, new BulkConfig { IgnoreRowVersion = true });
            Assert.Equal(bulk.Count(), context.AtypicalRowVersionEntities.Count());

            var bulk2 = new List<AtypicalRowVersionConverterEntity>();
            for (var i = 0; i < 100; i++)
                bulk2.Add(new AtypicalRowVersionConverterEntity { Id = Guid.NewGuid(), Name = $"Row {i}" });
            context.BulkInsertOrUpdate(bulk2);
            Assert.Equal(bulk2.Count(), context.AtypicalRowVersionConverterEntities.Count());
        }

        private int GetLastRowId(DbContext context, string tableName)
        {
            var sqlConnection = context.Database.GetDbConnection();
            sqlConnection.Open();
            using var command = sqlConnection.CreateCommand();
            command.CommandText = $"SELECT IDENT_CURRENT('{tableName}')";
            int lastRowIdScalar = Convert.ToInt32(command.ExecuteScalar());
            return lastRowIdScalar;
        }

        [Fact]
        private void CustomPrecisionDateTimeTest()
        {
            ContextUtil.DbServer = DbServer.SQLServer;
            using var context = new TestContext(ContextUtil.GetOptions());

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
            Assert.Equal(3240000, context.Events.SingleOrDefault(a => a.Name == "Event 1").TimeCreated.Ticks % 10000000);
            Assert.Equal(3240000, context.Events.SingleOrDefault(a => a.Name == "Event 2").TimeCreated.Ticks % 10000000);
        }

        [Fact]
        private void ByteArrayPKBulkReadTest()
        {
            ContextUtil.DbServer = DbServer.SQLite;
            using var context = new TestContext(ContextUtil.GetOptions());

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

            var entities = new List<Archive>();
            entities.Add(new Archive { ArchiveId = byte1 });
            entities.Add(new Archive { ArchiveId = byte2 });
            context.BulkRead(entities);

            Assert.Equal("Desc1", entities[0].Description);
            Assert.Equal("Desc2", entities[1].Description);
        }
    }
}
