using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Threading.Tasks;
using EFCore.BulkExtensions.SqlAdapters;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EFCore.BulkExtensions.Tests.IncludeGraph;

public class GraphRoot
{
    public Guid Id { get; set; }
    public OwnedComponent Component { get; set; } = null!;
}

public class OwnedComponent
{
    public List<OwnedItem> Items { get; set; } = null!;
}

public class OwnedItem
{
    public string Value { get; set; } = null!;
}

public class OwnedGraphContext : DbContext
{
    public OwnedGraphContext([NotNull] DbContextOptions options) : base(options)
    {
    }

    public DbSet<GraphRoot> RootEntities { get; set; } = null!;

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        base.OnModelCreating(modelBuilder);

        modelBuilder.Entity<GraphRoot>(grb =>
        {
            grb.HasKey(r => r.Id);

            // Ids are generated externally and must not be overwritten by DB
            grb.Property(r => r.Id).ValueGeneratedNever();

            grb.OwnsOne(r => r.Component, cb =>
            {
                cb.OwnsMany(ad => ad.Items, acb =>
                {
                    acb.ToTable("GraphRoot_Items");
                });
            })

            // required to mitigate https://docs.microsoft.com/en-us/ef/core/what-is-new/ef-core-6.0/breaking-changes#nested-optionals
            .Navigation(r => r.Component).IsRequired();
        });
    }
}

public class OwnedGraph : IDisposable
{
    //[Theory]
    //[InlineData(DbServer.SQLServer)]
    public async Task Test(DatabaseType dbServer)
    {
        //ARRANGE
        ContextUtil.DatabaseType = dbServer;

        using var db = new OwnedGraphContext(ContextUtil.GetOptions<OwnedGraphContext>(databaseName: $"{nameof(EFCoreBulkTest)}_OwnedGraph"));
        await db.Database.EnsureCreatedAsync();

        var first = new GraphRoot
        {
            Id = Guid.NewGuid(),
            Component = new OwnedComponent
            {
                Items = new List<OwnedItem>() { new OwnedItem { Value = "a" }, new OwnedItem { Value = "b" } }
            }
        };

        var second = new GraphRoot
        {
            Id = Guid.NewGuid(),
            Component = new OwnedComponent
            {
                Items = new List<OwnedItem>() { new OwnedItem { Value = "c" }, new OwnedItem { Value = "d" } }
            }
        };

        //ACT
        /* normal EF core insert works
        db.RootEntities.AddRange(first, second);
        db.SaveChanges(); */

        // but this doesn't
        await db.BulkInsertAsync(new[] { first, second }, new BulkConfig { IncludeGraph = true });

        /* and BTW this doesn't work either
        db.RootEntities.AddRange(first, second);
        db.BulkSaveChanges(new BulkConfig { IncludeGraph = true }); */

        // ASSERT
        var reloaded = await db.RootEntities.Include(y => y.Component.Items).ToListAsync();
        Assert.Equal(2, reloaded.Count);

        var firstLoaded = reloaded.SingleOrDefault(x => x.Id == first.Id);
        var secondLoaded = reloaded.SingleOrDefault(x => x.Id == second.Id);

        Assert.NotNull(firstLoaded);
        Assert.NotNull(secondLoaded);

        Assert.Equal(first.Component.Items.Count, firstLoaded.Component.Items.Count);
        Assert.Equal(second.Component.Items.Count, secondLoaded.Component.Items.Count);
        Assert.Equal("a", firstLoaded.Component.Items[0].Value);
        Assert.Equal("b", firstLoaded.Component.Items[1].Value);
        Assert.Equal("c", secondLoaded.Component.Items[0].Value);
        Assert.Equal("d", secondLoaded.Component.Items[1].Value);
    }

    public void Dispose()
    {
        using var db = new OwnedGraphContext(ContextUtil.GetOptions<OwnedGraphContext>(databaseName: $"{nameof(EFCoreBulkTest)}_OwnedGraph"));
        db.Database.EnsureDeleted();
    }
}
