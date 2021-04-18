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
        [InlineData(DbServer.SqlServer)]
        [InlineData(DbServer.Sqlite)] // Does NOT have Computed Columns
        private void ComputedAndDefaultValuesTest(DbServer dbServer)
        {
            ContextUtil.DbServer = dbServer;
            using var context = new TestContext(ContextUtil.GetOptions());
            context.Truncate<Document>();

            var entities = new List<Document>();
            for (int i = 1; i <= EntitiesNumber; i++)
            {
                var entity = new Document
                {
                    Content = "Info " + i
                };
                if (dbServer == DbServer.Sqlite)
                {
                    entity.DocumentId = Guid.NewGuid();
                    entity.ContentLength = entity.Content.Length;
                }
                entities.Add(entity);
            }
            context.BulkInsert(entities, bulkAction => bulkAction.SetOutputIdentity = true); // example of setting BulkConfig with Action argument

            // TEST
            var documents = context.Documents.ToList();
            Assert.Equal(EntitiesNumber, documents.Count());
            var firstDocument = documents[0];
            Assert.NotEqual(Guid.Empty, firstDocument.DocumentId);
            Assert.Equal(firstDocument.Content.Length, firstDocument.ContentLength);
            Assert.Equal(true, firstDocument.IsActive);
        }

        [Theory]
        [InlineData(DbServer.SqlServer)]
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
                    Data = "Some data " + i
                };
                entities.Add(entity);
            }
            context.BulkInsert(entities, bulkAction => bulkAction.SetOutputIdentity = true); // example of setting BulkConfig with Action argument

            // For testing concurrency conflict (UPDATE changes RowVersion which is TimeStamp column)
            context.Database.ExecuteSqlRaw("UPDATE dbo.[File] SET Data = 'Some data 1 PRE CHANGE' WHERE [Id] = 1;");

            var entitiesToUpdate = entities.Take(10).ToList();
            foreach (var entityToUpdate in entitiesToUpdate)
            {
                entityToUpdate.Data += " UPDATED";
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
        [InlineData(DbServer.SqlServer)]
        [InlineData(DbServer.Sqlite)]
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

            // TEST
            var entities = context.UserRoles.ToList();
            Assert.Equal(EntitiesNumber, entities.Count());
        }

        [Theory]
        [InlineData(DbServer.SqlServer)]
        [InlineData(DbServer.Sqlite)]
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
        [InlineData(DbServer.SqlServer)]
        [InlineData(DbServer.Sqlite)]
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

            if (dbServer == DbServer.SqlServer)
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
        [InlineData(DbServer.SqlServer)]
        [InlineData(DbServer.Sqlite)]
        private void OwnedTypesTest(DbServer dbServer)
        {
            ContextUtil.DbServer = dbServer;
            using var context = new TestContext(ContextUtil.GetOptions());

            if (dbServer == DbServer.SqlServer)
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
                    }/*,
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
                        }*/
                });
            }
            context.BulkInsert(entities);

            if (dbServer == DbServer.SqlServer)
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
            if (dbServer == DbServer.SqlServer)
            {
                context.BulkRead(entities);
            }
            Assert.Equal("Dsc 1 UPD", entities[0].Description);
            Assert.Equal(InfoType.InfoTypeB, entities[0].Audit.InfoType);
        }

        [Theory]
        [InlineData(DbServer.SqlServer)]
        //[InlineData(DbServer.Sqlite)] Not supported
        private void ShadowFKPropertiesTest(DbServer dbServer) // with Foreign Key as Shadow Property
        {
            ContextUtil.DbServer = dbServer;
            using var context = new TestContext(ContextUtil.GetOptions());

            if (dbServer == DbServer.SqlServer)
            {
                context.Truncate<ItemLink>();
                context.Database.ExecuteSqlRaw("TRUNCATE TABLE [" + nameof(ItemLink) + "]");
            }
            else
            {
                //context.ChangeLogs.BatchDelete(); // TODO
                context.BulkDelete(context.ItemLinks.ToList());
            }
            context.BulkDelete(context.Items.ToList()); // On table with FK Truncate does not work


            for (int i = 1; i < 10; ++i)
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
            var items = context.Items.ToList();
            var entities = new List<ItemLink>();
            for (int i = 0; i <= EntitiesNumber - 1; i++)
            {
                entities.Add(new ItemLink
                {
                    ItemLinkId = 0,
                    Item = items[i % items.Count]
                });
            }
            context.BulkInsert(entities);

            if (dbServer == DbServer.SqlServer)
            {
                context.BulkRead(entities);
                foreach (var entity in entities)
                {
                    Assert.NotNull(entity.Item);
                }
            }

            context.BulkDelete(context.ItemLinks.ToList());
        }

        [Theory]
        [InlineData(DbServer.SqlServer)]
        [InlineData(DbServer.Sqlite)]
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
        [InlineData(DbServer.SqlServer)]
        [InlineData(DbServer.Sqlite)]
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
            ContextUtil.DbServer = DbServer.SqlServer;
            using var context = new TestContext(ContextUtil.GetOptions());

            context.BulkDelete(context.Addresses.ToList());

            var entities = new List<Address> {
                    new Address {
                        Street = "Some Street nn",
                        Location = new Point(52, 13)
                    }
                };

            context.BulkInsertOrUpdate(entities);
        }

        [Fact]
        private void TablePerTypeInsertTest()
        {
            ContextUtil.DbServer = DbServer.SqlServer;
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
            ContextUtil.DbServer = DbServer.SqlServer;
            using var context = new TestContext(ContextUtil.GetOptions());
            context.AtypicalRowVersionEntities.BatchDelete();

            var bulk = new List<AtypicalRowVersionEntity>();
            for (var i = 0; i < 100; i++)
                bulk.Add(new AtypicalRowVersionEntity { Id = Guid.NewGuid(), Name = $"Row {i}", RowVersion = i, SyncDevice = "Test" });

            //Assert.Throws<InvalidOperationException>(() => context.BulkInsertOrUpdate(bulk)); // commented since when running in Debug mode it pauses on Exception
            context.BulkInsertOrUpdate(bulk, new BulkConfig { IgnoreRowVersion = true });
            Assert.Equal(bulk.Count(), context.AtypicalRowVersionEntities.Count());
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
            ContextUtil.DbServer = DbServer.SqlServer;
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
                context.BulkInsert(entities, b => b.DateTime2PrecisionForceRound = true);
            }
            else
            {
                context.AddRange(entities);
                context.SaveChanges();
            }

            // TEST
            Assert.Equal(3250000, context.Events.SingleOrDefault(a => a.Name == "Event 1").TimeCreated.Ticks % 10000000);
            Assert.Equal(3250000, context.Events.SingleOrDefault(a => a.Name == "Event 2").TimeCreated.Ticks % 10000000);
        }
    }
}
