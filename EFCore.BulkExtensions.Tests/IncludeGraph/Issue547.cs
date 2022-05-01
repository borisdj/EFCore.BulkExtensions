using EFCore.BulkExtensions.SqlAdapters;
using Microsoft.EntityFrameworkCore;
using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace EFCore.BulkExtensions.Tests.IncludeGraph;

public class RootEntity
{
    public int Id { get; set; }
    public OwnedType Owned { get; set; } = null!;
    public OwnedInSeparateTable OwnedInSeparateTable { get; set; } = null!;
}

public class OwnedType
{
    public int? ChildId { get; set; }
    public ChildEntity Child { get; set; } = null!;
    public string Field1 { get; set; } = null!;
}

public class OwnedInSeparateTable
{
    public string Flowers { get; set; } = null!;
}

public class ChildEntity
{
    public int Id { get; set; }
    public string Name { get; set; } = null!;
}

public class Issue547DbContext : DbContext
{
    public Issue547DbContext([NotNullAttribute] DbContextOptions options) : base(options)
    {
    }

    public DbSet<RootEntity> RootEntities { get; set; } = null!;

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        base.OnModelCreating(modelBuilder);

        modelBuilder.Entity<RootEntity>(cfg =>
        {
            cfg.HasKey(y => y.Id);
            cfg.OwnsOne(y => y.Owned, own =>
            {
                own.HasOne(y => y.Child).WithMany().HasForeignKey(y => y.ChildId).IsRequired(false);
                own.Property(y => y.Field1).HasMaxLength(50);
            });

            cfg.OwnsOne(y => y.OwnedInSeparateTable, own =>
            {
                own.ToTable(nameof(OwnedInSeparateTable));
            });
        });

        modelBuilder.Entity<ChildEntity>(cfg =>
        {
            cfg.Property(y => y.Id).ValueGeneratedNever();
            cfg.Property(y => y.Name);
        });
    }
}

public class Issue547 : IDisposable
{
    [Theory]
    [InlineData(DbServer.SQLServer)]
    public async Task Test(DbServer dbServer)
    {
        ContextUtil.DbServer = dbServer;

        using var db = new Issue547DbContext(ContextUtil.GetOptions<Issue547DbContext>(databaseName: $"{nameof(EFCoreBulkTest)}_Issue547"));
        await db.Database.EnsureCreatedAsync();

        var tranches = new List<RootEntity>
        {
            new RootEntity {
                Id = 1,
                Owned = new OwnedType
                {
                    Field1 = "F1",
                    Child = new ChildEntity { Id = 1388, Name = "F1C1" }
                },
                OwnedInSeparateTable = new OwnedInSeparateTable
                {
                    Flowers = "Roses"
                }
            },
            new RootEntity {
                Id = 2,
                Owned = new OwnedType
                {
                    Field1 = "F2",
                    Child = new ChildEntity { Id = 1234, Name = "F2C2" }
                },
                OwnedInSeparateTable = new OwnedInSeparateTable
                {
                    Flowers = "Tulips"
                }
            }
        };

        await db.BulkInsertOrUpdateAsync(tranches, new BulkConfig
        {
            IncludeGraph = true
        });

        foreach (var a in tranches)
        {
            Assert.True(a.Owned.ChildId.HasValue);
        }

        var rootEntities = await db.RootEntities
            .Include(y => y.OwnedInSeparateTable)
            .Include(y => y.Owned.Child)
            .ToListAsync();

        foreach (var re in rootEntities)
        {
            Assert.NotNull(re.Owned);
            Assert.NotEmpty(re.Owned.Field1);

            Assert.NotNull(re.Owned.ChildId);
            Assert.NotNull(re.Owned.Child);
            Assert.NotEmpty(re.Owned.Child.Name);
            
            Assert.NotNull(re.OwnedInSeparateTable);
            Assert.NotEmpty(re.OwnedInSeparateTable.Flowers);
        }
    }

    public void Dispose()
    {
        using var db = new Issue547DbContext(ContextUtil.GetOptions<Issue547DbContext>(databaseName: $"{nameof(EFCoreBulkTest)}_Issue547"));
        db.Database.EnsureDeleted();
    }
}
