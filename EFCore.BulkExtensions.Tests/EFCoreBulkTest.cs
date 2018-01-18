using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EFCore.BulkExtensions.Tests
{
    public class EFCoreBulkTest
    {
        private int entitiesNumber = 100000;
        
        [Theory]
        //[InlineData(true)]
        [InlineData(true, true)]
        //[InlineData(false, true)] // for speed comparison with Regular EF CUD operations
        public void OperationsTest(bool isBulkOperation, bool insertTo2Tables = false)
        {
            // Test can be run individually by commenting others and running each separately in order one after another
            RunInsert(isBulkOperation, insertTo2Tables);
            RunInsertOrUpdate(isBulkOperation);
            RunUpdate(isBulkOperation);
            RunDelete(isBulkOperation);
        }

        private void WriteProgress(decimal percentage)
        {
            Debug.WriteLine(percentage);
        }

        private void RunInsert(bool isBulkOperation, bool insertTo2Tables = false)
        {
            using (var context = new TestContext(ContextUtil.GetOptions()))
            {
                var entities = new List<Item>();
                var subEntities = new List<ItemHistory>();
                for (int i = 1; i < entitiesNumber; i++)
                {
                    entities.Add(new Item
                    {
                        ItemId = isBulkOperation ? i : 0,
                        Name = "name " + i,
                        Description = "info " + Guid.NewGuid().ToString().Substring(0, 3),
                        Quantity = i % 10,
                        Price = i / (i % 5 + 1),
                        TimeUpdated = DateTime.Now
                    });
                }
                if (insertTo2Tables)
                {
                    foreach (var entity in entities)
                    {
                        subEntities.Add(new ItemHistory
                        {
                            ItemHistoryId = SeqGuid.Create(),
                            ItemId = entity.ItemId,
                            Remark = "some more info"
                        });
                    }
                }

                if (isBulkOperation)
                {
                    if (!insertTo2Tables)
                    {
                        context.BulkInsert(entities);
                    }
                    else
                    {
                        using (var transaction = context.Database.BeginTransaction())
                        {
                            context.BulkInsert(entities, new BulkConfig { PreserveInsertOrder = true, SetOutputIdentity = true, BatchSize = 4000/*, UseTempDB = true*/ }, (a) => WriteProgress(a));
                            context.BulkInsert(subEntities);
                            transaction.Commit();
                        }
                    }
                }
                else
                {
                    context.Items.AddRange(entities);

                    if (insertTo2Tables)
                    {
                        context.ItemHistories.AddRange(subEntities);
                    }

                    context.SaveChanges();
                }
            }
            using (var context = new TestContext(ContextUtil.GetOptions()))
            {
                int entitiesCount = context.Items.Count();
                Item lastEntity = context.Items.LastOrDefault();

                Assert.Equal(entitiesNumber - 1, entitiesCount);
                Assert.NotNull(lastEntity);
                Assert.Equal("name " + (entitiesNumber - 1), lastEntity.Name);
            }
        }

        private void RunInsertOrUpdate(bool isBulkOperation)
        {
            using (var context = new TestContext(ContextUtil.GetOptions()))
            {
                var entities = new List<Item>();
                var dateTimeNow = DateTime.Now;
                for (int i = 2; i <= entitiesNumber; i += 2)
                {
                    entities.Add(new Item
                    {
                        ItemId = isBulkOperation ? i : 0,
                        Name = "name InsertOrUpdate " + i,
                        Description = "info",
                        Quantity = i,
                        Price = i / (i % 5 + 1),
                        TimeUpdated = dateTimeNow
                    });
                }
                if (isBulkOperation)
                {
                    context.BulkInsertOrUpdate(entities, null, (a) => WriteProgress(a));
                }
                else
                {
                    context.Items.Add(entities[entities.Count - 1]);
                    context.SaveChanges();
                }
            }
            using (var context = new TestContext(ContextUtil.GetOptions()))
            {
                int entitiesCount = context.Items.Count();
                Item lastEntity = context.Items.LastOrDefault();

                Assert.Equal(entitiesNumber, entitiesCount);
                Assert.NotNull(lastEntity);
                Assert.Equal("name InsertOrUpdate " + entitiesNumber, lastEntity.Name);
            }
        }

        private void RunUpdate(bool isBulkOperation)
        {
            using (var context = new TestContext(ContextUtil.GetOptions()))
            {
                int counter = 1;
                var entities = context.Items.AsNoTracking().ToList();
                foreach (var entity in entities)
                {
                    entity.Name = "name Update " + counter++;
                    entity.TimeUpdated = DateTime.Now;
                }
                if (isBulkOperation)
                {
                    context.BulkUpdate(entities);
                }
                else
                {
                    context.Items.UpdateRange(entities);
                    context.SaveChanges();
                }
            }
            using (var context = new TestContext(ContextUtil.GetOptions()))
            {
                int entitiesCount = context.Items.Count();
                Item lastEntity = context.Items.LastOrDefault();

                Assert.Equal(entitiesNumber, entitiesCount);
                Assert.NotNull(lastEntity);
                Assert.Equal("name Update " + entitiesNumber, lastEntity.Name);
            }
        }

        private void RunDelete(bool isBulkOperation)
        {
            using (var context = new TestContext(ContextUtil.GetOptions()))
            {
                var entities = context.Items.AsNoTracking().ToList();
                // ItemHistories will also be deleted because of Relationship - ItemId (Delete Rule: Cascade)
                if (isBulkOperation)
                {
                    context.BulkDelete(entities);
                }
                else
                {
                    context.Items.RemoveRange(entities);
                    context.SaveChanges();
                }
            }
            using (var context = new TestContext(ContextUtil.GetOptions()))
            {
                int entitiesCount = context.Items.Count();
                Item lastEntity = context.Items.LastOrDefault();

                Assert.Equal(0, entitiesCount);
                Assert.Null(lastEntity);
            }

            using (var context = new TestContext(ContextUtil.GetOptions()))
            {
                // Resets AutoIncrement
                context.Database.ExecuteSqlCommand("DBCC CHECKIDENT ('dbo.[" + nameof(Item) + "]', RESEED, 0);"); // can NOT use $"...{nameof(Item)..." beacause it get parameterized
                //context.Database.ExecuteSqlCommand($"TRUNCATE TABLE {nameof(Item)};"); // can NOT work when there is ForeignKey - ItemHistoryId
            }
        }
    }
}
