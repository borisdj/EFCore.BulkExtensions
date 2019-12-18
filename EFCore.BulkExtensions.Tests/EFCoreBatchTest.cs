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

        [Theory]
        [InlineData(DbServer.SqlServer)]
        [InlineData(DbServer.Sqlite)]
        public void BatchTest(DbServer databaseType)
        {
            ContextUtil.DbServer = databaseType;

            RunDeleteAll(databaseType);
            RunInsert();
            RunBatchUpdate();
            int deletedEntities = RunTopBatchDelete();
            RunBatchDelete();
            RunBatchDelete2();
            //RunContainsBatchDelete(); // currently not supported for EFCore 3.0

            using (var context = new TestContext(ContextUtil.GetOptions()))
            {
                var firstItem = context.Items.ToList().FirstOrDefault();
                var lastItem = context.Items.ToList().LastOrDefault();
                Assert.Equal(1, deletedEntities);
                Assert.Equal(500, lastItem.ItemId);
                Assert.Equal("Updated", lastItem.Description);
                Assert.Equal(1.5m, lastItem.Price);
                Assert.StartsWith("name ", lastItem.Name);
                Assert.EndsWith(" Concatenated", lastItem.Name);
                Assert.EndsWith(" TOP(1)", firstItem.Name);
            }
        }

        internal void RunDeleteAll(DbServer databaseType)
        {
            using (var context = new TestContext(ContextUtil.GetOptions()))
            {
                context.Items.Add(new Item { }); // used for initial add so that after RESEED it starts from 1, not 0
                context.SaveChanges();

                context.Items.BatchDelete();
                context.BulkDelete(context.Items.ToList());

                if (databaseType == DbServer.SqlServer)
                {
                    context.Database.ExecuteSqlRaw("DBCC CHECKIDENT('[dbo].[Item]', RESEED, 0);");
                }
                if (databaseType == DbServer.Sqlite)
                {
                    context.Database.ExecuteSqlRaw("DELETE FROM sqlite_sequence WHERE name = 'Item';");
                }
            }
        }

        private void RunBatchUpdate()
        {
            using (var context = new TestContext(ContextUtil.GetOptions()))
            {
                //var updateColumns = new List<string> { nameof(Item.Quantity) }; // Adding explicitly PropertyName for update to its default value

                decimal price = 0;
                var query = context.Items.Where(a => a.ItemId <= 500 && a.Price >= price);

                query.BatchUpdate(new Item { Description = "Updated", Price = 1.5m }/*, updateColumns*/);

                var incrementStep = 100;
                var suffix = " Concatenated";
                query.BatchUpdate(a => new Item { Name = a.Name + suffix, Quantity = a.Quantity + incrementStep }); // example of BatchUpdate Increment/Decrement value in variable
                                                                                                                    //query.BatchUpdate(a => new Item { Quantity = a.Quantity + 100 }); // example direct value without variable

                query.Take(1).BatchUpdate(a => new Item { Name = a.Name + " TOP(1)", Quantity = a.Quantity + incrementStep }); // example of BatchUpdate with TOP(1)
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
                        Name = "name " + Guid.NewGuid().ToString().Substring(0, 3),
                        Description = "info",
                        Quantity = i % 10,
                        Price = i / (i % 5 + 1),
                        TimeUpdated = DateTime.Now,
                        ItemHistories = new List<ItemHistory>()
                    };
                    entities.Add(entity);
                }

                context.Items.AddRange(entities); // does not guarantee insert order for SqlServer
                context.SaveChanges();
            }
        }

        private int RunTopBatchDelete()
        {
            using (var context = new TestContext(ContextUtil.GetOptions()))
            {
                return context.Items.Where(a => a.ItemId > 500).Take(1).BatchDelete();
            }
        }

        private void RunBatchDelete()
        {
            using (var context = new TestContext(ContextUtil.GetOptions()))
            {
                context.Items.Where(a => a.ItemId > 500).BatchDelete();
            }
        }

        private void RunBatchDelete2()
        {
            using (var context = new TestContext(ContextUtil.GetOptions()))
            {
                var nameToDelete = "N4";
                context.Items.Where(a => a.Name == nameToDelete).BatchDelete();
            }
        }

        private void RunContainsBatchDelete() // currently not supported for EFCore 3.0
        {
            var descriptionsToDelete = new List<string> { "info" };
            using (var context = new TestContext(ContextUtil.GetOptions()))
            {
                context.Items.Where(a => descriptionsToDelete.Contains(a.Description)).BatchDelete();
            }
        }
    }
}
