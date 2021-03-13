using EFCore.BulkExtensions.SqlAdapters;
using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
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
            RunBatchUpdate(databaseType);

            int deletedEntities = 1;
            if (databaseType == DbServer.SqlServer)
            {
                RunBatchUpdate_UsingNavigationPropertiesThatTranslateToAnInnerQuery();
                deletedEntities = RunTopBatchDelete();
            }

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

                if (databaseType == DbServer.SqlServer)
                {
                    Assert.EndsWith(" TOP(1)", firstItem.Name);
                }
            }

            if (databaseType == DbServer.SqlServer)
            {
                RunUdttBatch();
            }

            if (databaseType == DbServer.SqlServer)
            {
                // Removing ORDER BY and CTE's are not implemented for SQLite.
                RunOrderByDeletes();
                RunIncludeDelete();
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

        private void RunBatchUpdate(DbServer databaseType)
        {
            using (var context = new TestContext(ContextUtil.GetOptions()))
            {
                //var updateColumns = new List<string> { nameof(Item.Quantity) }; // Adding explicitly PropertyName for update to its default value

                decimal price = 0;

                var query = context.Items.AsQueryable();
                if (databaseType == DbServer.SqlServer)
                {
                    query = query.Where(a => a.ItemId <= 500 && a.Price >= price);
                }
                if (databaseType == DbServer.Sqlite)
                {
                    query = query.Where(a => a.ItemId <= 500); // Sqlite currently does Not support multiple conditions
                }

                query.BatchUpdate(new Item { Description = "Updated", Price = 1.5m }/*, updateColumns*/);

                var incrementStep = 100;
                var suffix = " Concatenated";
                query.BatchUpdate(a => new Item { Name = a.Name + suffix, Quantity = a.Quantity + incrementStep }); // example of BatchUpdate Increment/Decrement value in variable

                if (databaseType == DbServer.SqlServer) // Sqlite currently does Not support Take(): LIMIT
                {
                    query.Take(1).BatchUpdate(a => new Item { Name = a.Name + " TOP(1)", Quantity = a.Quantity + incrementStep }); // example of BatchUpdate with TOP(1)
                }
            }
        }

        private void RunBatchUpdate_UsingNavigationPropertiesThatTranslateToAnInnerQuery()
        {
            var testDbCommandInterceptor = new TestDbCommandInterceptor();

            using (var context = new TestContext(ContextUtil.GetOptions(testDbCommandInterceptor)))
            {
                context.Parents.Where(parent => parent.ParentId < 5 && !string.IsNullOrEmpty(parent.Details.Notes))
                    .BatchUpdate(parent => new Parent { Description = parent.Details.Notes ?? "Fallback" });

                var actualSqlExecuted = testDbCommandInterceptor.ExecutedNonQueryCommands?.LastOrDefault();
                var expectedSql =
@"UPDATE p SET  [p].[Description] = (
    SELECT COALESCE([p1].[Notes], N'Fallback')
    FROM [ParentDetail] AS [p1]
    WHERE [p1].[ParentId] = [p].[ParentId]) 
FROM [Parent] AS [p]
LEFT JOIN [ParentDetail] AS [p0] ON [p].[ParentId] = [p0].[ParentId]
WHERE ([p].[ParentId] < 5) AND ([p0].[Notes] IS NOT NULL AND (([p0].[Notes] <> N'') OR [p0].[Notes] IS NULL))";

                Assert.Equal(expectedSql.Replace("\r\n", "\n"), actualSqlExecuted.Replace("\r\n", "\n"));

                context.Parents.Where(parent => parent.ParentId == 1)
                    .BatchUpdate(parent => new Parent { Value = parent.Children.Where(child => child.IsEnabled).Sum(child => child.Value) });

                actualSqlExecuted = testDbCommandInterceptor.ExecutedNonQueryCommands?.LastOrDefault();
                expectedSql =
@"UPDATE p SET  [p].[Value] = (
    SELECT SUM([c].[Value])
    FROM [Child] AS [c]
    WHERE ([p].[ParentId] = [c].[ParentId]) AND ([c].[IsEnabled] = CAST(1 AS bit))) 
FROM [Parent] AS [p]
WHERE [p].[ParentId] = 1";

                Assert.Equal(expectedSql.Replace("\r\n", "\n"), actualSqlExecuted.Replace("\r\n", "\n"));

                var newValue = 5;

                context.Parents.Where(parent => parent.ParentId == 1)
                    .BatchUpdate(parent => new Parent { 
                        Description = parent.Children.Where(child => child.IsEnabled && child.Value == newValue).Sum(child => child.Value).ToString(),
                        Value = newValue
                    });

                actualSqlExecuted = testDbCommandInterceptor.ExecutedNonQueryCommands?.LastOrDefault();
                expectedSql =
@"UPDATE p SET  [p].[Description] = (CONVERT(VARCHAR(100), (
    SELECT SUM([c].[Value])
    FROM [Child] AS [c]
    WHERE ([p].[ParentId] = [c].[ParentId]) AND (([c].[IsEnabled] = CAST(1 AS bit)) AND ([c].[Value] = @__p_0))))) , [p].[Value] = @param_1 
FROM [Parent] AS [p]
WHERE [p].[ParentId] = 1";

                Assert.Equal(expectedSql.Replace("\r\n", "\n"), actualSqlExecuted.Replace("\r\n", "\n"));
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

        private void RunOrderByDeletes()
        {
            using (var context = new TestContext(ContextUtil.GetOptions()))
            {
                context.Items.OrderBy(x => x.Name).Skip(2).Take(4).BatchDelete();
            }

            using (var context = new TestContext(ContextUtil.GetOptions()))
            {
                context.Items.OrderBy(x => x.Name).Take(2).BatchDelete();
            }

            using (var context = new TestContext(ContextUtil.GetOptions()))
            {
                context.Items.OrderBy(x => x.Name).BatchDelete();
            }
        }

        private void RunIncludeDelete()
        {
            using (var context = new TestContext(ContextUtil.GetOptions()))
            {
                context.Items.Include(x => x.ItemHistories).Where(x => !x.ItemHistories.Any()).OrderBy(x => x.ItemId).Skip(2).Take(4).BatchDelete();
            }

            using (var context = new TestContext(ContextUtil.GetOptions()))
            {
                context.Items.Include(x => x.ItemHistories).Where(x => !x.ItemHistories.Any()).OrderBy(x => x.ItemId).Take(4).BatchDelete();
            }
            using (var context = new TestContext(ContextUtil.GetOptions()))
            {
                context.Items.Include(x => x.ItemHistories).Where(x => !x.ItemHistories.Any()).BatchDelete();
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
