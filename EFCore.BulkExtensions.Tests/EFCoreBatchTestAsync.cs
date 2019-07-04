using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EFCore.BulkExtensions.Tests
{
    public class EFCoreBatchTestAsync
    {
        protected int EntitiesNumber => 1000;

        [Fact]
        public async Task BatchTestAsync()
        {
            await RunBatchDeleteAll();
            await RunInsert();
            await RunBatchUpdate();
            await RunBatchDelete();

            using (var context = new TestContext(ContextUtil.GetOptions()))
            {
                var lastItem = context.Items.LastOrDefaultAsync().Result;
                Assert.Equal(500, lastItem.ItemId);
                Assert.Equal("Updated", lastItem.Description);
                Assert.Equal(100, lastItem.Quantity);
            }

            await RunBatchDeleteAll();
        }

        private async Task RunBatchDeleteAll()
        {
            using (var context = new TestContext(ContextUtil.GetOptions()))
            {
                await context.Items.BatchDeleteAsync();
            }
            using (var context = new TestContext(ContextUtil.GetOptions()))
            {
                await context.Database.ExecuteSqlCommandAsync("DBCC CHECKIDENT('[dbo].[Item]', RESEED, 0);");
            }
        }

        private async Task RunBatchUpdate()
        {
            using (var context = new TestContext(ContextUtil.GetOptions()))
            {
                //var updateColumns = new List<string> { nameof(Item.Quantity) }; // Adding explicitly PropertyName for update to its default value

                decimal price = 0;
                var query = context.Items.Where(a => a.ItemId <= 500 && a.Price >= price);
                await query.BatchUpdateAsync(new Item { Description = "Updated", Price = 1.5m }/*, updateColumns*/);

                await query.BatchUpdateAsync(a => new Item { Quantity = a.Quantity + 100 }); // example of BatchUpdate value Increment/Decrement
            }
        }

        private async Task RunBatchDelete()
        {
            using (var context = new TestContext(ContextUtil.GetOptions()))
            {
                await context.Items.Where(a => a.ItemId > 500).BatchDeleteAsync();
            }
        }

        private async Task RunInsert()
        {
            using (var context = new TestContext(ContextUtil.GetOptions()))
            {
                var entities = new List<Item>();
                for (int i = 1; i <= EntitiesNumber; i++)
                {
                    var entity = new Item
                    {
                        Name = "name " + i,
                        Description = "info " + Guid.NewGuid().ToString().Substring(0, 3),
                        Quantity = i % 10,
                        Price = i / (i % 5 + 1),
                        TimeUpdated = DateTime.Now,
                        ItemHistories = new List<ItemHistory>()
                    };
                    entities.Add(entity);
                }

                await context.Items.AddRangeAsync(entities);
                await context.SaveChangesAsync();
            }
        }
    }
}
