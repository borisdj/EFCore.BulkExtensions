using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EFCore.BulkExtensions.Tests
{
    public class EFCoreBatchTest
    {
        protected int EntitiesNumber => 1000;

        [Fact]
        public void BatchTest()
        {
            RunBatchDeleteAll();
            RunInsert();
            RunBatchUpdate();
            RunBatchDelete();

            using (var context = new TestContext(ContextUtil.GetOptions()))
            {
                Assert.Equal(500, context.Items.LastOrDefault().ItemId);
                Assert.Equal("Updated", context.Items.LastOrDefault().Description);
            }

            RunBatchDeleteAll();
        }

        private void RunBatchDeleteAll()
        {
            using (var context = new TestContext(ContextUtil.GetOptions()))
            {
                context.Items.BatchDelete();
            }
            using (var context = new TestContext(ContextUtil.GetOptions()))
            {
                context.Database.ExecuteSqlCommand("DBCC CHECKIDENT('[dbo].[Item]', RESEED, 0);");
            }
        }

        private void RunBatchUpdate()
        {
            using (var context = new TestContext(ContextUtil.GetOptions()))
            {
                //var updateColumns = new List<string> { nameof(Item.Quantity) }; // Adding explicitly PropertyName for update to its default value
                context.Items.Where(ab => ab.ItemId <= 500 && ab.Price >= 0).BatchUpdate(new Item { Description = "Updated", Price = 1.5m }/*, updateColumns*/);
            }
        }

        private void RunBatchDelete()
        {
            using (var context = new TestContext(ContextUtil.GetOptions()))
            {
                context.Items.Where(a => a.ItemId > 500).BatchDelete();
            }
        }

        private void RunInsert()
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

                context.Items.AddRange(entities);
                context.SaveChanges();
            }
        }
    }
}
