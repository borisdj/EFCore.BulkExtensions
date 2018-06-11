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
        [Fact]
        private void InsertAndUpdateWithCompositeKey()
        {
            using (var context = new TestContext(ContextUtil.GetOptions()))
            {
                int entitiesNumber = 1000;
                var entities = new List<UserRole>();
                for (int i = 0; i < entitiesNumber; i++)
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
                int entitiesNumber = entities.Count();
                for (int i = 0; i < entitiesNumber; i++)
                {
                    entities[i].Description = "desc updated " + i;
                }
                context.BulkUpdate(entities);
            }
            using (var context = new TestContext(ContextUtil.GetOptions()))
            {
                var entities = context.UserRoles.ToList();
                Assert.Equal(1000, entities.Count());
                context.BulkDelete(entities);
            }
        }

        [Fact]
        private void InsertWithDiscriminatorShadow()
        {
            using (var context = new TestContext(ContextUtil.GetOptions()))
            {
                int entitiesNumber = 1000;
                var entities = new List<Student>();
                for (int i = 1; i <= entitiesNumber; i++)
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
                Assert.Equal(1000, entities.Count());
                context.BulkDelete(entities);
            }
        }

        [Fact]
        private void InsertWithValueConversion()
        {
            var dateTime = new DateTime(2018, 1, 1);

            using (var context = new TestContext(ContextUtil.GetOptions()))
            {
                int entitiesNumber = 1000;
                var entities = new List<InfoLog>();
                for (int i = 1; i <= entitiesNumber; i++)
                {
                    entities.Add(new InfoLog
                    {
                        Message = "Msg " + i,
                        ConvertedTime = dateTime
                    });
                }
                context.BulkInsert(entities);
            }

            using (var context = new TestContext(ContextUtil.GetOptions()))
            {
                var entities = context.InfoLogs.ToList();
                var entity = entities.FirstOrDefault();

                Assert.Equal(entity.ConvertedTime, dateTime);

                var conn = context.Database.GetDbConnection();
                if (conn.State != ConnectionState.Open)
                    conn.Open();
                using (var command = conn.CreateCommand())
                {
                    command.CommandText = $"SELECT TOP 1 * FROM {nameof(InfoLog)} ORDER BY {nameof(InfoLog.InfoLogId)} DESC";
                    var reader = command.ExecuteReader();
                    reader.Read();
                    var row = new InfoLog()
                    {
                        ConvertedTime = reader.Field<DateTime>(nameof(InfoLog.ConvertedTime))
                    };
                    Assert.Equal(row.ConvertedTime, dateTime.AddDays(1));
                }

                context.BulkDelete(entities);
            }
        }
    }
}
