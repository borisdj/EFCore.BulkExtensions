using EFCore.BulkExtensions.SqlAdapters;
using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using Xunit;

namespace EFCore.BulkExtensions.Tests;

public class EFCoreBatchTest
{
    protected static int EntitiesNumber => 1000;

    [Theory]
    [InlineData(DbServer.SQLServer)]
    public void BatchConverterTest(DbServer dbServer)
    {
        ContextUtil.DbServer = dbServer;

        using var context = new TestContext(ContextUtil.GetOptions());
        context.Truncate<Info>();

        var currentDate = DateTime.Today;
        var entity = new Info { Message = "name A", DateTimeOff = currentDate };
        context.Infos.Add(entity);
        context.SaveChanges();

        var updateTo = new Info { Message = "name B Updated" };
        context.Infos.BatchUpdate(updateTo);

        using var contextRead = new TestContext(ContextUtil.GetOptions());
        Assert.Equal(currentDate, contextRead.Infos.First().DateTimeOff);
    }

    [Theory]
    [InlineData(DbServer.SQLServer)]
    [InlineData(DbServer.SQLite)]
    public void BatchTest(DbServer dbServer)
    {
        ContextUtil.DbServer = dbServer;

        RunDeleteAll(dbServer);
        RunInsert();
        RunBatchUpdate(dbServer);

        int deletedEntities = 1;
        if (dbServer == DbServer.SQLServer)
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

        RunBatchUpdateEnum();
        RunBatchUint();

        UpdateSetting(SettingsEnum.Sett1, "Val1UPDATE");
        UpdateByteArrayToDefault();

        using (var context = new TestContext(ContextUtil.GetOptions()))
        {
            var firstItem = context.Items.ToList().First();
            var lastItem = context.Items.ToList().Last();
            Assert.Equal(1, deletedEntities);
            Assert.Equal(500, lastItem.ItemId);
            Assert.Equal("Updated", lastItem.Description);
            Assert.Equal(1.5m, lastItem.Price);
            Assert.StartsWith("name ", lastItem.Name);
            Assert.EndsWith(" Concatenated", lastItem.Name);

            if (dbServer == DbServer.SQLServer)
            {
                Assert.EndsWith(" TOP(1)", firstItem.Name);
            }
        }

        if (dbServer == DbServer.SQLServer)
        {
            RunUdttBatch();
        }

        if (dbServer == DbServer.SQLServer)
        {
            // Removing ORDER BY and CTE's are not implemented for SQLite.
            RunOrderByDeletes();
            RunIncludeDelete();
        }
    }

    internal void RunDeleteAll(DbServer dbServer)
    {
        using var context = new TestContext(ContextUtil.GetOptions());

        context.Items.Add(new Item { }); // used for initial add so that after RESEED it starts from 1, not 0
        context.SaveChanges();

        context.Items.BatchDelete();
        context.BulkDelete(context.Items.ToList());

        // RESET AutoIncrement
        string deleteTableSql = dbServer switch
        {
            DbServer.SQLServer => $"DBCC CHECKIDENT('[dbo].[{nameof(Item)}]', RESEED, 0);",
            DbServer.SQLite => $"DELETE FROM sqlite_sequence WHERE name = '{nameof(Item)}';",
            DbServer.PostgreSQL => $@"ALTER SEQUENCE ""{nameof(Item)}_{nameof(Item.ItemId)}_seq"" RESTART WITH 1;",
            _ => throw new ArgumentException($"Unknown database type: '{dbServer}'.", nameof(dbServer)),
        };
        context.Database.ExecuteSqlRaw(deleteTableSql);
    }

    private static void RunBatchUpdate(DbServer dbServer)
    {
        using var context = new TestContext(ContextUtil.GetOptions());

        //var updateColumns = new List<string> { nameof(Item.Quantity) }; // Adding explicitly PropertyName for update to its default value

        decimal price = 0;

        var query = context.Items.AsQueryable();
        if (dbServer == DbServer.SQLServer)
        {
            query = query.Where(a => a.ItemId <= 500 && a.Price >= price);//.OrderBy(n => n.ItemId).Take(500);
        }
        if (dbServer == DbServer.SQLite)
        {
            query = query.Where(a => a.ItemId <= 500 && a.Price != null && a.Quantity >= 0);

            //query = query.Where(a => a.ItemId <= 500 && a.Price >= price);
            // -----
            // Sqlite currently (since switching to 3.0.0) does Not work for '&& a.Price >= price' neither for '&& a.Price >= 0', because of 'decimal' type
            // Method ToParametrizedSql with Sqlite throws Exception on line:
            //   var enumerator = query.Provider.Execute<IEnumerable>(query.Expression).GetEnumerator();
            // Message:
            //   System.InvalidOperationException : The LINQ expression 'DbSet<Item>.Where(i => i.ItemId <= 500 && i.Price >= __price_0)' could not be translated.
        }

        query.BatchUpdate(new Item { Description = "Updated", Price = 1.5m }/*, updateColumns*/);

        var incrementStep = 100;
        var suffix = " Concatenated";
        query.BatchUpdate(a => new Item { Name = a.Name + suffix, Quantity = a.Quantity + incrementStep }); // example of BatchUpdate Increment/Decrement value in variable

        if (dbServer == DbServer.SQLServer) // Sqlite currently does Not support Take(): LIMIT
        {
            query.Take(1).BatchUpdate(a => new Item { Name = a.Name + " TOP(1)", Quantity = a.Quantity + incrementStep }); // example of BatchUpdate with TOP(1)
        }
    }
    
    private static void RunBatchUpdateEnum()
    {
        using var context = new TestContext(ContextUtil.GetOptions());

        context.Truncate<Source>();

        context.Sources.AddRange(new Source[] {
            new Source { StatusId = Status.Init, TypeId = Type.Type2 },
            new Source { StatusId = Status.Changed, TypeId = Type.Type2 }
        });
        context.SaveChanges();

        var updateValues = new Source() { StatusId = Status.Changed };
        var updateColumns = new List<string>() { nameof(updateValues.StatusId) };
        context.Sources.Where(e => e.StatusId == Status.Init).BatchUpdate(updateValues, updateColumns);

        Assert.Equal(Type.Type2, context.Sources.FirstOrDefault()?.TypeId); // Should remain 'Type.Type2' and not be changed to default 'Type.Undefined'
    }

    private static void RunBatchUint()
    {
        using var context = new TestContext(ContextUtil.GetOptions());

        uint id = 0;
        context.Counters.Where(e => e.CounterId == id).BatchDelete();

    }

    private static void RunBatchUpdate_UsingNavigationPropertiesThatTranslateToAnInnerQuery()
    {
        var testDbCommandInterceptor = new TestDbCommandInterceptor();
        using var context = new TestContext(ContextUtil.GetOptions(testDbCommandInterceptor));

        context.Parents.Where(parent => parent.ParentId < 5 && !string.IsNullOrEmpty(parent.Details.Notes))
            .BatchUpdate(parent => new Parent { Description = parent.Details.Notes ?? "Fallback" });

        var actualSqlExecuted = testDbCommandInterceptor.ExecutedNonQueryCommands?.LastOrDefault()?.Sql;
        var expectedSql =
@"UPDATE p SET  [p].[Description] = (
    SELECT COALESCE([p1].[Notes], N'Fallback')
    FROM [ParentDetail] AS [p1]
    WHERE [p1].[ParentId] = [p].[ParentId]) 
FROM [Parent] AS [p]
LEFT JOIN [ParentDetail] AS [p0] ON [p].[ParentId] = [p0].[ParentId]
WHERE ([p].[ParentId] < 5) AND ([p0].[Notes] IS NOT NULL AND NOT ([p0].[Notes] LIKE N''))";

        Assert.Equal(expectedSql.Replace("\r\n", "\n"), actualSqlExecuted?.Replace("\r\n", "\n"));

        context.Parents.Where(parent => parent.ParentId == 1)
            .BatchUpdate(parent => new Parent { Value = parent.Children.Where(child => child.IsEnabled).Sum(child => child.Value) });

        actualSqlExecuted = testDbCommandInterceptor.ExecutedNonQueryCommands?.LastOrDefault()?.Sql;
        expectedSql =
@"UPDATE p SET  [p].[Value] = (
    SELECT COALESCE(SUM([c].[Value]), 0.0)
    FROM [Child] AS [c]
    WHERE ([p].[ParentId] = [c].[ParentId]) AND ([c].[IsEnabled] = CAST(1 AS bit))) 
FROM [Parent] AS [p]
WHERE [p].[ParentId] = 1";

        Assert.Equal(expectedSql.Replace("\r\n", "\n"), actualSqlExecuted?.Replace("\r\n", "\n"));

        var newValue = 5;

        context.Parents.Where(parent => parent.ParentId == 1)
            .BatchUpdate(parent => new Parent
            {
                Description = parent.Children.Where(child => child.IsEnabled && child.Value == newValue).Sum(child => child.Value).ToString(),
                Value = newValue
            });

        actualSqlExecuted = testDbCommandInterceptor.ExecutedNonQueryCommands?.LastOrDefault()?.Sql;
        expectedSql =
@"UPDATE p SET  [p].[Description] = (CONVERT(varchar(100), (
    SELECT COALESCE(SUM([c].[Value]), 0.0)
    FROM [Child] AS [c]
    WHERE ([p].[ParentId] = [c].[ParentId]) AND (([c].[IsEnabled] = CAST(1 AS bit)) AND ([c].[Value] = @__p_0))))) , [p].[Value] = @param_1 
FROM [Parent] AS [p]
WHERE [p].[ParentId] = 1";

        Assert.Equal(expectedSql.Replace("\r\n", "\n"), actualSqlExecuted?.Replace("\r\n", "\n"));
    }

    private static void RunInsert()
    {
        using var context = new TestContext(ContextUtil.GetOptions());

        var entities = new List<Item>();
        for (int i = 1; i <= EntitiesNumber; i++)
        {
            var entity = new Item
            {
                Name = string.Concat("name ", Guid.NewGuid().ToString().AsSpan(0, 3)),
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

    private static int RunTopBatchDelete()
    {
        using var context = new TestContext(ContextUtil.GetOptions());
        return context.Items.Where(a => a.ItemId > 500).Take(1).BatchDelete();
    }

    private static void RunBatchDelete()
    {
        using var context = new TestContext(ContextUtil.GetOptions());
        context.Items.Where(a => a.ItemId > 500).BatchDelete();
    }

    private static void RunBatchDelete2()
    {
        using var context = new TestContext(ContextUtil.GetOptions());
        var nameToDelete = "N4";
        context.Items.Where(a => a.Name == nameToDelete).BatchDelete();
    }

    private static void RunContainsBatchDelete()
    {
        var descriptionsToDelete = new List<string> { "info" };
        using var context = new TestContext(ContextUtil.GetOptions());
        context.Items.Where(a => descriptionsToDelete.Contains(a.Description ?? "")).BatchDelete();
    }

    private static void RunContainsBatchDelete2()
    {
        var descriptionsToDelete = new List<string> { "info" };
        var nameToDelete = "N4";
        using var context = new TestContext(ContextUtil.GetOptions());
        context.Items.Where(a => descriptionsToDelete.Contains(a.Description ?? "") || a.Name == nameToDelete).BatchDelete();
    }

    private static void RunContainsBatchDelete3()
    {
        var descriptionsToDelete = new List<string>();
        using var context = new TestContext(ContextUtil.GetOptions());
        context.Items.Where(a => descriptionsToDelete.Contains(a.Description ?? "")).BatchDelete();
    }

    private static void RunAnyBatchDelete()
    {
        var descriptionsToDelete = new List<string> { "info" };
        using var context = new TestContext(ContextUtil.GetOptions());
        context.Items.Where(a => descriptionsToDelete.Any(toDelete => toDelete == a.Description)).BatchDelete();
    }

    private static void RunOrderByDeletes()
    {
        using var context = new TestContext(ContextUtil.GetOptions());
        context.Items.OrderBy(x => x.Name).Skip(2).Take(4).BatchDelete();
        context.Items.OrderBy(x => x.Name).Take(2).BatchDelete();
        context.Items.OrderBy(x => x.Name).BatchDelete();
    }

    private static void RunIncludeDelete()
    {
        using var context = new TestContext(ContextUtil.GetOptions());
        context.Items.Include(x => x.ItemHistories).Where(x => !x.ItemHistories.Any()).OrderBy(x => x.ItemId).Skip(2).Take(4).BatchDelete();
        context.Items.Include(x => x.ItemHistories).Where(x => !x.ItemHistories.Any()).OrderBy(x => x.ItemId).Take(4).BatchDelete();
        context.Items.Include(x => x.ItemHistories).Where(x => !x.ItemHistories.Any()).BatchDelete();
    }

    private static void RunUdttBatch()
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
            var ll = context.UserRoles.ToList();

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

    private static IQueryable<UdttIntInt> GetQueryableUdtt(TestContext context, IReadOnlyList<UdttIntInt> list)
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

    private static void UpdateSetting(SettingsEnum settings, object value)
    {
        using var context = new TestContext(ContextUtil.GetOptions());
        context.Truncate<Setting>();

        context.Settings.Add(new Setting() { Settings = SettingsEnum.Sett1, Value = "Val1" });
        context.SaveChanges();

        // can work with explicit value: .Where(x => x.Settings == SettingsEnum.Sett1) or if named Parameter used then it has to be named (settings) same as Property (Settings) - Case not relevant, it is CaseInsensitive
        context.Settings.Where(x => x.Settings == settings).BatchUpdate(x => new Setting { Value = value.ToString() });

        context.Truncate<Setting>();
    }

    private static void UpdateByteArrayToDefault()
    {
        using var context = new TestContext(ContextUtil.GetOptions());

        context.Files.BatchUpdate(new File { DataBytes = null }, updateColumns: new List<string> { nameof(File.DataBytes) });
        context.Files.BatchUpdate(a => new File { DataBytes = null });
    }
}
