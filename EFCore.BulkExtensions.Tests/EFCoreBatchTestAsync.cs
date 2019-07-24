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

        [Theory]
        [InlineData(DbServer.SqlServer)]
        [InlineData(DbServer.Sqlite)]
        public async Task BatchTestAsync(DbServer databaseType)
        {
            ContextUtil.DbServer = databaseType;

            await RunBatchDeleteAllAsync(databaseType);
            await RunInsertAsync();
            await RunBatchUpdateAsync();
            await RunBatchDeleteAsync();

            using (var context = new TestContext(ContextUtil.GetOptions()))
            {
                var lastItem = context.Items.LastOrDefaultAsync().Result;
                Assert.Equal(500, lastItem.ItemId);
                Assert.Equal("Updated", lastItem.Description);
                Assert.Equal(100, lastItem.Quantity);
            }
        }

        public async Task RunBatchDeleteAllAsync(DbServer databaseType)
        {
            using (var context = new TestContext(ContextUtil.GetOptions()))
            {
                await context.Items.BatchDeleteAsync();

                if (databaseType == DbServer.SqlServer)
                {
                    await context.Database.ExecuteSqlCommandAsync("DBCC CHECKIDENT('[dbo].[Item]', RESEED, 0);").ConfigureAwait(false);
                }
                if (databaseType == DbServer.Sqlite)
                {
                    await context.Database.ExecuteSqlCommandAsync("DELETE FROM sqlite_sequence WHERE name = 'Item';").ConfigureAwait(false);
                }
            }
        }

        private async Task RunBatchUpdateAsync()
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

        private async Task RunBatchDeleteAsync()
        {
            using (var context = new TestContext(ContextUtil.GetOptions()))
            {
                await context.Items.Where(a => a.ItemId > 500).BatchDeleteAsync();
            }
        }

        private async Task RunInsertAsync()
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
