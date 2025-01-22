using System;
using System.Diagnostics.CodeAnalysis;
using System.Threading.Tasks;
using EFCore.BulkExtensions.SqlAdapters;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EFCore.BulkExtensions.Tests.Owned;

public class NestedOwnedTests
{
    [Theory]
    [InlineData(SqlType.SqlServer)]
    [InlineData(SqlType.PostgreSql)]
    public async Task NestedOwnedTest(SqlType sqlType)
    {
        ContextUtil.DatabaseType = sqlType;
        
        using var context = new NestedDbContext(ContextUtil.GetOptions<NestedDbContext>(databaseName: $"{nameof(EFCoreBulkTest)}_NestedOwned"));

        NestedRoot[] entities = [
            new()
            {
                NestedRootId = "nestedrootid",
                FirstNested = new()
                {
                    FirstNestedProperty = "firstnested",
                    Enum = WallType.Clay,
                    EnumArray = Enum.GetValues<WallType>(),
                    SecondNested = new()
                    {
                        SecondNestedProperty = "secondnested",
                        ThirdNested = new()
                        {
                            ThirdNestedProperty = "thirdnested",
                        },
                    },
                },
            }
        ];

        await context.BulkInsertOrUpdateAsync(entities);

        var nestedroot = await context.Set<NestedRoot>().SingleAsync();
        Assert.Equal("nestedrootid", nestedroot.NestedRootId);
        Assert.Equal("firstnested", nestedroot.FirstNested.FirstNestedProperty);
        Assert.Equal("secondnested", nestedroot.FirstNested.SecondNested.SecondNestedProperty);
        Assert.Equal("thirdnested", nestedroot.FirstNested.SecondNested.ThirdNested.ThirdNestedProperty);
        Assert.Equal(WallType.Clay, nestedroot.FirstNested.Enum);
        
        if (sqlType == SqlType.PostgreSql)
            Assert.Equal(Enum.GetValues<WallType>(), nestedroot.FirstNested.EnumArray);
    }
}

public class NestedRoot
{
    public string NestedRootId { get; set; } = default!;
    public FirstNested FirstNested { get; set; } = default!;
}

public class FirstNested
{
    public string? FirstNestedProperty { get; set; }
    public SecondNested SecondNested { get; set; } = default!;
    
    // Test value converter inside owned type
    public WallType Enum { get; set; }
    public WallType[] EnumArray { get; set; } = null!;
}

public class SecondNested
{
    public string? SecondNestedProperty { get; set; }
    public ThirdNested ThirdNested { get; set; } = default!;
}

public class ThirdNested
{
    public string? ThirdNestedProperty { get; set; }
}

public class NestedDbContext : DbContext
{
    [SuppressMessage("ReSharper", "VirtualMemberCallInConstructor")]
    public NestedDbContext(DbContextOptions opts) : base(opts)
    {
        Database.EnsureDeleted();
        Database.EnsureCreated();
    }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<NestedRoot>(r => r.OwnsOne(r => r.FirstNested, 
            f =>
            {
                f.OwnsOne(f => f.SecondNested,
                    s => s.OwnsOne(s => s.ThirdNested));
                
                if (!Database.IsNpgsql())
                    f.Ignore(x => x.EnumArray);
            }));
    }
}
