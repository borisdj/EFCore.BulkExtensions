using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EFCore.BulkExtensions.Tests
{
    public class EFOperationsTestAtypical
    {
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
                context.Students.AddRange(entities); // adding to Context is required so that Shadow property 'Descriminator' gets set
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
    }
}
