using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EFCore.BulkExtensions.Tests
{
    public class EFCoreBulkTestAtypical
    {
        protected int EntitiesNumber => 1000;

        [Fact]
        private void InsertWithDbComputedColumnsAndOutput()
        {
            using (var context = new TestContext(ContextUtil.GetOptions()))
            {
                var entities = new List<Document>();
                for (int i = 0; i < EntitiesNumber; i++)
                {
                    entities.Add(new Document
                    {
                        Content = "Some data " + i,
                    });
                }
                context.BulkInsert(
                    entities,
                        new BulkConfig
                        {
                            SetOutputIdentity = true
                        }
                    );
            }
            using (var context = new TestContext(ContextUtil.GetOptions()))
            {
                var entities = context.Documents.ToList();
                Assert.Equal(EntitiesNumber, entities.Count());
                context.BulkDelete(entities);
            }
        }

        [Fact]
        private void InsertAndUpdateWithCompositeKey()
        {
            using (var context = new TestContext(ContextUtil.GetOptions()))
            {
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

        [Fact]
        private void InsertWithDiscriminatorShadow()
        {
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
                var entities = context.Students.ToList();
                Assert.Equal(EntitiesNumber, entities.Count());
                context.BulkDelete(entities);
            }
        }

        [Fact]
        private void InsertWithValueConversion()
        {
            var dateTime = new DateTime(2018, 1, 1);

            using (var context = new TestContext(ContextUtil.GetOptions()))
            {
                var entities = new List<Info>();
                for (int i = 1; i <= EntitiesNumber; i++)
                {
                    entities.Add(new Info
                    {
                        Message = "Msg " + i,
                        ConvertedTime = dateTime
                    });
                }
                context.BulkInsert(entities);
            }

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

                context.BulkDelete(entities);
            }
        }

        [Fact]
        private void InsertWithOwnedTypes()
        {
            using (var context = new TestContext(ContextUtil.GetOptions()))
            {
                context.Database.ExecuteSqlCommand("TRUNCATE TABLE [" + nameof(ChangeLog) + "]");

                var entities = new List<ChangeLog>();
                for (int i = 1; i <= EntitiesNumber; i++)
                {
                    entities.Add(new ChangeLog
                    {
                        ChangeLogId = 1,
                        Description = "Dsc " + i,
                        Audit = new Audit
                        {
                            ChangedBy = "User" + 1,
                            ChangedTime = DateTime.Now
                        }
                    });
                }
                context.BulkInsertOrUpdate(entities);
            }
        }
    }
}
