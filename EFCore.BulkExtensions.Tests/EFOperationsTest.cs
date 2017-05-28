using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EFCore.BulkExtensions.Tests
{
    public class EFOperationsTest
    {
        private int entitiesNumber = 1000;

        private DbContextOptions GetContextOptions()
        {
            var builder = new DbContextOptionsBuilder<TestContext>();
            var databaseName = nameof(EFOperationsTest);
            var connectionString = $"Server=localhost;Database={databaseName};Trusted_Connection=True;MultipleActiveResultSets=true";
            builder.UseSqlServer(connectionString); // Can NOT Test with UseInMemoryDb (Exception: Relational-specific methods can only be used when the context is using a relational)
            return builder.Options;
        }
        
        [Theory]
        [InlineData(true)]
        //[InlineData(false)] // for speed comparison with Regular EF CUD operations
        public void OperationsTest(bool isBulkOperation)
        {
            // Test can be run individually by commenting 2 others and running each separately in order one after another
            //RunInsert(isBulkOperation); // 1.First comment RunUpdate(isBulkOperation); and RunDelete(isBulkOperation); which will insert rows into table
            //RunUpdate(isBulkOperation); // 2.Next comment RunInsert(isBulkOperation); RunDelete(isBulkOperation); for updating
            //RunDelete(isBulkOperation); // 3.Finally comment RunInsert(isBulkOperation); RunUpdate(isBulkOperation); which will delete rows

            RunInsertOrUpdate(isBulkOperation);
        }

        private void RunInsert(bool isBulkOperation)
        {
            using (var context = new TestContext(GetContextOptions()))
            {
                var entities = new List<Item>();
                for (int i = 1; i <= entitiesNumber; i++)
                {
                    entities.Add(new Item
                    {
                        //ItemId = Guid.NewGuid(),
                        Name = "name " + i,
                        Description = "Some info",
                        Quantity = i,
                        Price = i / (i % 5 + 1),
                        TimeUpdated = DateTime.Now
                    });
                }
                if (isBulkOperation)
                {
                    context.BulkInsert(entities);
                }
                else
                {
                    context.Item.AddRange(entities);
                    context.SaveChanges();
                }
            }
            using (var context = new TestContext(GetContextOptions()))
            {
                int entitiesCount = context.Item.Count();
                Item lastEntity = context.Item.LastOrDefault();

                Assert.Equal(entitiesNumber, entitiesCount);
                Assert.NotNull(lastEntity);
                Assert.Equal("name " + entitiesNumber, lastEntity.Name);
            }
        }

        private void RunInsertOrUpdate(bool isBulkOperation)
        {
            using (var context = new TestContext(GetContextOptions()))
            {
                var entities = new List<Item>();
                var dateTimeNow = DateTime.Now;
                for (int i = 1; i <= entitiesNumber; i++)
                {
                    entities.Add(new Item
                    {
                        ItemId = i - entitiesNumber,
                        //ItemId = Guid.NewGuid(),
                        Name = "name " + i,
                        Description = "Some info",
                        Quantity = i,
                        Price = i / (i % 5 + 1),
                        TimeUpdated = dateTimeNow
                    });
                }
                if (isBulkOperation)
                {
                    context.BulkInsertOrUpdate(entities, true);
                }
                else
                {
                    context.Item.AddRange(entities);
                    context.SaveChanges();
                }
            }
            using (var context = new TestContext(GetContextOptions()))
            {
                int entitiesCount = context.Item.Count();
                Item lastEntity = context.Item.LastOrDefault();

                Assert.Equal(entitiesNumber, entitiesCount);
                Assert.NotNull(lastEntity);
                Assert.Equal("name " + entitiesNumber, lastEntity.Name);
            }
        }

        private void RunUpdate(bool isBulkOperation)
        {
            using (var context = new TestContext(GetContextOptions()))
            {
                int counter = 1;
                var entities = context.Item.AsNoTracking().ToList();
                foreach (var entity in entities)
                {
                    entity.Name = "name Updated " + counter++;
                    entity.TimeUpdated = DateTime.Now;
                }
                if (isBulkOperation)
                {
                    context.BulkUpdate(entities);
                }
                else
                {
                    context.Item.UpdateRange(entities);
                    context.SaveChanges();
                }
            }
            using (var context = new TestContext(GetContextOptions()))
            {
                int entitiesCount = context.Item.Count();
                Item lastEntity = context.Item.LastOrDefault();

                Assert.Equal(entitiesNumber, entitiesCount);
                Assert.NotNull(lastEntity);
                Assert.Equal("name Updated " + entitiesNumber, lastEntity.Name);
            }
        }

        private void RunDelete(bool isBulkOperation)
        {
            using (var context = new TestContext(GetContextOptions()))
            {
                var entities = context.Item.AsNoTracking().ToList();
                if (isBulkOperation)
                {
                    context.BulkDelete(entities);
                }
                else
                {
                    context.Item.RemoveRange(entities);
                    context.SaveChanges();
                }
            }
            using (var context = new TestContext(GetContextOptions()))
            {
                int entitiesCount = context.Item.Count();
                Item lastEntity = context.Item.LastOrDefault();

                Assert.Equal(0, entitiesCount);
                Assert.Null(lastEntity);
            }
            
            using (var context = new TestContext(GetContextOptions()))
            {
                context.Database.ExecuteSqlCommand($"TRUNCATE TABLE {nameof(Item)};"); // Resets AutoIncrement
            }
        }
    }
}
