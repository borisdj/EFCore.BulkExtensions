using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using EFCore.BulkExtensions.SqlAdapters;
using Microsoft.EntityFrameworkCore;
using NetTopologySuite.Geometries;
using Xunit;

namespace EFCore.BulkExtensions.Tests
{
    public class EFCoreBulkTestAtypical
    {
        protected int EntitiesNumber => 1000;

        [Theory]
        [InlineData(DbServer.SqlServer)]
        [InlineData(DbServer.Sqlite)] // Does NOT have Computed Columns
        private void ComputedAndDefaultValuesTest(DbServer databaseType)
        {
            ContextUtil.DbServer = databaseType;
            using (var context = new TestContext(ContextUtil.GetOptions()))
            {
                context.Truncate<Document>();

                var entities = new List<Document>();
                for (int i = 1; i <= EntitiesNumber; i++)
                {
                    var entity = new Document
                    {
                        Content = "Info " + i
                    };
                    if (databaseType == DbServer.Sqlite)
                    {
                        entity.DocumentId = Guid.NewGuid();
                        entity.ContentLength = entity.Content.Length;
                        entity.IsActive = true;
                    }
                    entities.Add(entity);
                }
                context.BulkInsert(entities, bulkAction => bulkAction.SetOutputIdentity = true); // example of setting BulkConfig with Action argument
            }
            using (var context = new TestContext(ContextUtil.GetOptions()))
            {
                var entities = context.Documents.ToList();
                Assert.Equal(EntitiesNumber, entities.Count());
                var firstDocument = entities[0];
                Assert.NotEqual(Guid.Empty, firstDocument.DocumentId);
                Assert.Equal(firstDocument.Content.Length, firstDocument.ContentLength);
                Assert.Equal(true, firstDocument.IsActive);
            }
        }

        [Theory]
        [InlineData(DbServer.SqlServer)]
        //[InlineData(DbServer.Sqlite)] // No TimeStamp column type but can be set with DefaultValueSql: "CURRENT_TIMESTAMP" as it is in OnModelCreating() method.
        private void TimeStampTest(DbServer databaseType)
        {
            ContextUtil.DbServer = databaseType;
            using (var context = new TestContext(ContextUtil.GetOptions()))
            {
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

                using (var transaction = context.Database.BeginTransaction())
                {
                    var bulkConfig = new BulkConfig { SetOutputIdentity = true, DoNotUpdateIfTimeStampChanged = true };
                    context.BulkUpdate(entitiesToUpdate, bulkConfig);

                    var list = bulkConfig.TimeStampInfo?.EntitiesOutput.Cast<File>().ToList();
                    Assert.Equal(9, list.Count());
                    Assert.Equal(1, bulkConfig.TimeStampInfo.NumberOfSkippedForUpdate);

                    if (bulkConfig.TimeStampInfo?.NumberOfSkippedForUpdate > 0)
                    {
                        transaction.Rollback();
                    }
                    else
                    {
                        transaction.Commit();
                    }
                }
            }
        }

        [Theory]
        [InlineData(DbServer.SqlServer)]
        [InlineData(DbServer.Sqlite)]
        private void CompositeKeyTest(DbServer databaseType)
        {
            ContextUtil.DbServer = databaseType;
            using (var context = new TestContext(ContextUtil.GetOptions()))
            {
                context.Truncate<UserRole>();
                var entities = new List<UserRole>();
                for (int i = 0; i < EntitiesNumber; i++)
                {
                    entities.Add(new UserRole
                    {
                        UserId = i / 10,
                        RoleId = i % 10,
                        Description = "desc"
                    });
                }
                context.BulkInsert(entities);
            }
            using (var context = new TestContext(ContextUtil.GetOptions()))
            {
                var entities = context.UserRoles.ToList();
                int entitiesCount = entities.Count();
                for (int i = 0; i < entitiesCount; i++)
                {
                    entities[i].Description = "desc updated " + i;
                }
                context.BulkUpdate(entities);
            }
            using (var context = new TestContext(ContextUtil.GetOptions()))
            {
                var entities = context.UserRoles.ToList();
                Assert.Equal(EntitiesNumber, entities.Count());
                context.BulkDelete(entities);
            }
        }

        [Theory]
        [InlineData(DbServer.SqlServer)]
        [InlineData(DbServer.Sqlite)]
        private void DiscriminatorShadowTest(DbServer databaseType)
        {
            ContextUtil.DbServer = databaseType;
            using (var context = new TestContext(ContextUtil.GetOptions()))
            {
                context.BulkDelete(context.Students.ToList());
            }

            using (var context = new TestContext(ContextUtil.GetOptions()))
            {
                var entities = new List<Student>();
                for (int i = 1; i <= EntitiesNumber; i++)
                {
                    entities.Add(new Student
                    {
                        Name = "name " + i,
                        Subject = "Math"
                    });
                }
                context.Students.AddRange(entities); // adding to Context so that Shadow property 'Discriminator' gets set

                context.BulkInsert(entities);
            }

            using (var context = new TestContext(ContextUtil.GetOptions()))
            {
                var entities = new List<Student>();
                for (int i = 1; i <= EntitiesNumber / 2; i += 2)
                {
                    entities.Add(new Student
                    {
                        Name = "name " + i,
                        Subject = "Math Upd"
                    });
                }
                context.Students.AddRange(entities); // adding to Context so that Shadow property 'Discriminator' gets set

                context.BulkInsertOrUpdate(entities, new BulkConfig
                {
                    UpdateByProperties = new List<string> { nameof(Student.Name) },
                    PropertiesToExclude = new List<string> { nameof(Student.PersonId) },
                });
            }

            using (var context = new TestContext(ContextUtil.GetOptions()))
            {
                var entities = context.Students.ToList();
                Assert.Equal(EntitiesNumber, entities.Count());
            }
        }

        [Theory]
        [InlineData(DbServer.SqlServer)]
        [InlineData(DbServer.Sqlite)]
        private void ValueConversionTest(DbServer databaseType)
        {
            ContextUtil.DbServer = databaseType;
            var dateTime = DateTime.Today;

            using (var context = new TestContext(ContextUtil.GetOptions()))
            {
                context.BulkDelete(context.Infos.ToList());
            }

            using (var context = new TestContext(ContextUtil.GetOptions()))
            {
                var entities = new List<Info>();
                for (int i = 1; i <= EntitiesNumber; i++)
                {
                    entities.Add(new Info
                    {
                        Message = "Msg " + i,
                        ConvertedTime = dateTime,
                        InfoType = InfoType.InfoTypeA
                    });
                }
                context.BulkInsert(entities);
            }

            if (databaseType == DbServer.SqlServer)
            {
                using (var context = new TestContext(ContextUtil.GetOptions()))
                {
                    var entities = context.Infos.ToList();
                    var entity = entities.FirstOrDefault();

                    Assert.Equal(entity.ConvertedTime, dateTime);

                    var conn = context.Database.GetDbConnection();
                    if (conn.State != ConnectionState.Open)
                        conn.Open();
                    using (var command = conn.CreateCommand())
                    {
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
            }
        }

        [Theory]
        [InlineData(DbServer.SqlServer)]
        [InlineData(DbServer.Sqlite)]
        private void OwnedTypesTest(DbServer databaseType)
        {
            ContextUtil.DbServer = databaseType;
            using (var context = new TestContext(ContextUtil.GetOptions()))
            {
                if (databaseType == DbServer.SqlServer)
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

                if (databaseType == DbServer.SqlServer)
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
            }
        }

        [Theory]
        [InlineData(DbServer.SqlServer)]
        //[InlineData(DbServer.Sqlite)] Not supported
        private void ShadowFKPropertiesTest(DbServer databaseType) // with Foreign Key as Shadow Property
        {
            ContextUtil.DbServer = databaseType;
            using (var context = new TestContext(ContextUtil.GetOptions()))
            {
                if (databaseType == DbServer.SqlServer)
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

                if (databaseType == DbServer.SqlServer)
                {
                    context.BulkRead(entities);
                    foreach (var entity in entities)
                    {
                        Assert.NotNull(entity.Item);
                    }
                }

                context.BulkDelete(context.ItemLinks.ToList());
            }
        }

        [Fact]
        private void GeometryColumnTest()
        {
            ContextUtil.DbServer = DbServer.SqlServer;
            using (var context = new TestContext(ContextUtil.GetOptions()))
            {
                context.BulkDelete(context.Addresses.ToList());
            }

            using (var context = new TestContext(ContextUtil.GetOptions()))
            {
                var entities = new List<Address> {
                    new Address {
                        Street = "Some Street nn",
                        Location = new Point(52, 13)
                    }
                };

                context.BulkInsertOrUpdate(entities);
            }
        }
    }
}
