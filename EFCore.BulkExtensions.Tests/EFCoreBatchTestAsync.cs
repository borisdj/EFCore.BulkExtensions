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

            await RunDeleteAllAsync(databaseType);
            await RunInsertAsync();
            await RunBatchUpdateAsync();
            int deletedEntities = await RunTopBatchDeleteAsync();
            await RunBatchDeleteAsync();

            using (var context = new TestContext(ContextUtil.GetOptions()))
            {
                var firstItem = (await context.Items.ToListAsync()).First();
                var lastItem = (await context.Items.ToListAsync()).Last();
                Assert.Equal(1, deletedEntities);
                Assert.Equal(500, lastItem.ItemId);
                Assert.Equal("Updated", lastItem.Description);
                Assert.Null(lastItem.Price);
                Assert.StartsWith("name ", lastItem.Name);
                Assert.EndsWith(" Concatenated", lastItem.Name);
                Assert.EndsWith(" TOP(1)", firstItem.Name);
            }
        }

        internal async Task RunDeleteAllAsync(DbServer databaseType)
        {
            using (var context = new TestContext(ContextUtil.GetOptions()))
            {
                await context.Items.AddAsync(new Item { }); // used for initial add so that after RESEED it starts from 1, not 0
                await context.SaveChangesAsync();

                //await context.Items.BatchDeleteAsync(); // TODO: Use after BatchDelete gets implemented for v3.0 
                await context.BulkDeleteAsync(context.Items.ToList());

                if (databaseType == DbServer.SqlServer)
                {
                    await context.Database.ExecuteSqlRawAsync("DBCC CHECKIDENT('[dbo].[Item]', RESEED, 0);").ConfigureAwait(false);
                }
                if (databaseType == DbServer.Sqlite)
                {
                    await context.Database.ExecuteSqlRawAsync("DELETE FROM sqlite_sequence WHERE name = 'Item';").ConfigureAwait(false);
                }
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

        private async Task RunBatchUpdateAsync()
        {
            using (var context = new TestContext(ContextUtil.GetOptions()))
            {
                //var updateColumns = new List<string> { nameof(Item.Quantity) }; // Adding explicitly PropertyName for update to its default value

                decimal price = 0;
                var query = context.Items.Where(a => a.ItemId <= 500 && a.Price >= price);

                await query.BatchUpdateAsync(new Item { Description = "Updated" }/*, updateColumns*/);

                await query.BatchUpdateAsync(a => new Item { Name = a.Name + " Concatenated", Quantity = a.Quantity + 100, Price = null }); // example of BatchUpdate value Increment/Decrement

                query = context.Items.Where(a => a.ItemId <= 500 && a.Price == null);
                await query.Take(1).BatchUpdateAsync(a => new Item { Name = a.Name + " TOP(1)", Quantity = a.Quantity + 100 }); // example of BatchUpdate with TOP(1)
            }
        }

        private async Task<int> RunTopBatchDeleteAsync()
        {
            using (var context = new TestContext(ContextUtil.GetOptions()))
            {
                return await context.Items.Where(a => a.ItemId > 500).Take(1).BatchDeleteAsync();
            }
        }

        private async Task RunBatchDeleteAsync()
        {
            using (var context = new TestContext(ContextUtil.GetOptions()))
            {
                await context.Items.Where(a => a.ItemId > 500).BatchDeleteAsync();
            }
        }
    }
}
