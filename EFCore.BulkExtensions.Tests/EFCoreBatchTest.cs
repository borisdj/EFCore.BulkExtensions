using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.EntityFrameworkCore;
using System.Data;
using Microsoft.Data.SqlClient;
using Xunit;
using EFCore.BulkExtensions.SqlAdapters;

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
            RunContainsBatchDelete();
            RunContainsBatchDelete2();
            RunContainsBatchDelete3();
            RunAnyBatchDelete();

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

            if (databaseType == DbServer.SqlServer)
            {
                RunUdttBatch();
            }
        }

        // BATCH for Sqlite does Not work since switching to 3.0.0
        // Method ToParametrizedSql with Sqlite throws Exception on line:
        //   var enumerator = query.Provider.Execute<IEnumerable>(query.Expression).GetEnumerator();
        // Message:
        //   System.InvalidOperationException : The LINQ expression 'DbSet<Item>.Where(i => i.ItemId <= 500 && i.Price >= __price_0)' could not be translated.
        //   Either rewrite the query in a form that can be translated, or switch to client evaluation explicitly by inserting a call to either AsEnumerable(), AsAsyncEnumerable(), ToList(), or ToListAsync().
        //   See https://go.microsoft.com/fwlink/?linkid=2101038 for more information.
        //   QueryableMethodTranslatingExpressionVisitor.<VisitMethodCall>g__CheckTranslated|8_0(ShapedQueryExpression translated, <>c__DisplayClass8_0& )

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

        private void RunContainsBatchDelete()
        {
            var descriptionsToDelete = new List<string> { "info" };
            using (var context = new TestContext(ContextUtil.GetOptions()))
            {
                context.Items.Where(a => descriptionsToDelete.Contains(a.Description)).BatchDelete();
            }
        }

        private void RunContainsBatchDelete2()
        {
            var descriptionsToDelete = new List<string> { "info" };
            var nameToDelete = "N4";
            using (var context = new TestContext(ContextUtil.GetOptions()))
            {
                context.Items.Where(a => descriptionsToDelete.Contains(a.Description) || a.Name == nameToDelete).BatchDelete();
            }
        }

        private void RunContainsBatchDelete3()
        {
            var descriptionsToDelete = new List<string>();
            using (var context = new TestContext(ContextUtil.GetOptions()))
            {
                context.Items.Where(a => descriptionsToDelete.Contains(a.Description)).BatchDelete();
            }
        }

        private void RunAnyBatchDelete()
        {
            var descriptionsToDelete = new List<string> { "info" };
            using (var context = new TestContext(ContextUtil.GetOptions()))
            {
                context.Items.Where(a => descriptionsToDelete.Any(toDelete => toDelete == a.Description)).BatchDelete();
            }
        }

        private void RunUdttBatch()
        {
            var userRoles = (
                from userId in Enumerable.Range(1, 5)
                from roleId in Enumerable.Range(1, 5)
                select new UserRole { UserId = userId, RoleId = roleId, }
                )
                .ToList();
            var random = new Random();
            var keysToUpdate = userRoles
                .Where(x => random.Next() % 2 == 1)
                .Select(x => new UdttIntInt { C1 = x.UserId, C2 = x.RoleId, })
                .ToList();
            var keysToDelete = userRoles
                .Where(x => !keysToUpdate.Where(y => y.C1 == x.UserId && y.C2 == x.RoleId).Any())
                .Select(x => new UdttIntInt { C1 = x.UserId, C2 = x.RoleId, })
                .ToList();

            using (var context = new TestContext(ContextUtil.GetOptions()))
            {
                context.UserRoles.BatchDelete();

                context.UserRoles.AddRange(userRoles);
                context.SaveChanges();
            }

            // read with User Defined Table Type parameter
            using (var context = new TestContext(ContextUtil.GetOptions()))
            {
                var keysToUpdateQueryable = GetQueryableUdtt(context, keysToUpdate);
                var userRolesToUpdate = context.UserRoles
                    .Where(x => keysToUpdateQueryable.Where(y => y.C1 == x.UserId && y.C2 == x.RoleId).Any())
                    .ToList();

                var keysToDeleteQueryable = GetQueryableUdtt(context, keysToDelete);
                var userRolesToDelete = context.UserRoles
                    .Where(x => keysToDeleteQueryable.Where(y => y.C1 == x.UserId && y.C2 == x.RoleId).Any())
                    .ToList();

                Assert.Equal(keysToUpdate.Count, userRolesToUpdate.Count);
                Assert.Equal(keysToDelete.Count, userRolesToDelete.Count);
            }

            // batch update and batch delete with User Defined Table Type parameter
            using (var context = new TestContext(ContextUtil.GetOptions()))
            {
                var keysToUpdateQueryable = GetQueryableUdtt(context, keysToUpdate);
                var keysToDeleteQueryable = GetQueryableUdtt(context, keysToDelete);
                var userRolesToUpdate = context.UserRoles.Where(x => keysToUpdateQueryable.Where(y => y.C1 == x.UserId && y.C2 == x.RoleId).Any());
                var userRolesToDelete = context.UserRoles.Where(x => keysToDeleteQueryable.Where(y => y.C1 == x.UserId && y.C2 == x.RoleId).Any());

                // System.ArgumentException : No mapping exists from object type System.Object[] to a known managed provider native type.
                userRolesToUpdate.BatchUpdate(x => new UserRole { Description = "updated", });
                userRolesToDelete.BatchDelete();
            }

            using (var context = new TestContext(ContextUtil.GetOptions()))
            {
                Assert.Equal(keysToUpdate.Count, context.UserRoles.Count());
                Assert.True(!context.UserRoles.Where(x => x.Description == null || x.Description != "updated").Any());
            }
        }

        private IQueryable<UdttIntInt> GetQueryableUdtt(TestContext context, IReadOnlyList<UdttIntInt> list)
        {
            var parameterName = $"@p_{Guid.NewGuid():n}";
            var dt = new DataTable();
            dt.Columns.Add(nameof(UdttIntInt.C1), typeof(int));
            dt.Columns.Add(nameof(UdttIntInt.C2), typeof(int));
            foreach (var item in list)
            {
                dt.Rows.Add(item.C1, item.C2);
            }
            var parameter = new SqlParameter(parameterName, dt) { SqlDbType = SqlDbType.Structured, TypeName = "dbo.UdttIntInt", };
            return context.Set<UdttIntInt>().FromSqlRaw($@"select * from {parameterName}", parameter);
        }
    }
}
