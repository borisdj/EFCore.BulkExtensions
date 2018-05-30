using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EFCore.BulkExtensions.Tests
{
    public class EFCoreBulkTestAsync
    {
        private int entitiesNumber = 1000;

        [Theory]
        [InlineData(true, false)]
        //[InlineData(false)] // for speed comparison with Regular EF CUD operations
        public async Task OperationsTestAsync(bool isBulkOperation, bool insertTo2Tables = false)
        {
            // Test can be run individually by commenting others and running each separately in order one after another
            var dateTime = new DateTime(2018, 1, 1);
            await RunInsertAsync(isBulkOperation, dateTime, insertTo2Tables);
            await TestConversionAsync(dateTime.AddDays(1));

            dateTime = new DateTime(2018, 2, 1);
            await RunInsertOrUpdateAsync(isBulkOperation, dateTime);
            await TestConversionAsync(dateTime.AddDays(1));

            dateTime = new DateTime(2018, 3, 1);
            await RunUpdateAsync(isBulkOperation, dateTime);
            await TestConversionAsync(dateTime.AddDays(1));

            await RunDeleteAsync(isBulkOperation);
        }

        private async Task RunInsertAsync(bool isBulkOperation, DateTime dateTime, bool insertTo2Tables = false)
        {
            using (var context = new TestContext(ContextUtil.GetOptions()))
            {
                var entities = new List<Item>();
                var subEntities = new List<ItemHistory>();
                for (int i = 1; i < entitiesNumber; i++)
                {
                    entities.Add(new Item
                    {
                        ItemId = i,
                        Name = "name " + i,
                        Description = "info " + Guid.NewGuid().ToString().Substring(0, 3),
                        Quantity = i % 10,
                        Price = i / (i % 5 + 1),
                        TimeUpdated = dateTime,
                        ConvertedTime = dateTime,
                    });
                }
                if (isBulkOperation)
                {
                    if (insertTo2Tables)
                    {
                        using (var transaction = context.Database.BeginTransaction())
                        {
                            await context.BulkInsertAsync(entities, new BulkConfig { PreserveInsertOrder = true, SetOutputIdentity = true, BatchSize = 4000 });

                            foreach (var entity in entities)
                            {
                                subEntities.Add(new ItemHistory
                                {
                                    ItemHistoryId = SeqGuid.Create(),
                                    ItemId = entity.ItemId,
                                    Remark = "some more info"
                                });
                            }
                            await context.BulkInsertAsync(subEntities);

                            transaction.Commit();
                        }
                    }
                    else
                    {
                        await context.BulkInsertAsync(entities);
                    }
                }
                else
                {
                    await context.Items.AddRangeAsync(entities);
                    await context.SaveChangesAsync();
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

        private async Task RunInsertOrUpdateAsync(bool isBulkOperation, DateTime dateTime)
        {
            using (var context = new TestContext(ContextUtil.GetOptions()))
            {
                var entities = new List<Item>();
                for (int i = 2; i <= entitiesNumber; i += 2)
                {
                    entities.Add(new Item
                    {
                        ItemId = i,
                        Name = "name InsertOrUpdate " + i,
                        Description = "info",
                        Quantity = i,
                        Price = i / (i % 5 + 1),
                        TimeUpdated = dateTime,
                        ConvertedTime = dateTime
                    });
                }
                if (isBulkOperation)
                {
                    await context.BulkInsertOrUpdateAsync(entities);
                }
                else
                {
                    await context.Items.AddRangeAsync(entities);
                    await context.SaveChangesAsync();
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

        private async Task RunUpdateAsync(bool isBulkOperation, DateTime dateTime)
        {
            using (var context = new TestContext(ContextUtil.GetOptions()))
            {
                int counter = 1;
                var entities = context.Items.AsNoTracking().ToList();
                foreach (var entity in entities)
                {
                    entity.Name = "name Update " + counter++;
                    entity.TimeUpdated = DateTime.Now;
                    entity.ConvertedTime = dateTime;
                }
                if (isBulkOperation)
                {
                    await context.BulkUpdateAsync(entities);
                }
                else
                {
                    context.Items.UpdateRange(entities);
                    await context.SaveChangesAsync();
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

        private async Task RunDeleteAsync(bool isBulkOperation)
        {
            using (var context = new TestContext(ContextUtil.GetOptions()))
            {
                var entities = context.Items.AsNoTracking().ToList();
                // ItemHistories will also be deleted because of Relationship - ItemId (Delete Rule: Cascade)
                if (isBulkOperation)
                {
                    await context.BulkDeleteAsync(entities);
                }
                else
                {
                    context.Items.RemoveRange(entities);
                    await context.SaveChangesAsync();
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
                context.Database.ExecuteSqlCommand("DBCC CHECKIDENT ('dbo.[" + nameof(Item) + "]', RESEED, 0);");
                //context.Database.ExecuteSqlCommand($"TRUNCATE TABLE {nameof(Item)};"); // can NOT work when there is ForeignKey - ItemHistoryId
            }
        }

        private async Task TestConversionAsync(DateTime dateTime)
        {
            using (var context = new TestContext(ContextUtil.GetOptions()))
            {
                var conn = context.Database.GetDbConnection();
                if (conn.State != ConnectionState.Open)
                    await conn.OpenAsync();

                using (var command = conn.CreateCommand())
                {
                    command.CommandText = $"select top 1 * from {nameof(Item)} order by {nameof(Item.ItemId)} desc";
                    var reader = command.ExecuteReader();
                    await reader.ReadAsync();
                    var row = new Item()
                    {
                        ConvertedTime = reader.Field<DateTime>(nameof(Item.ConvertedTime)),
                    };
                    if (row.ConvertedTime != dateTime)
                        throw new Exception($"The '{nameof(Item.ConvertedTime)}' was not converted properly");
                }
            }
        }
    }
}
