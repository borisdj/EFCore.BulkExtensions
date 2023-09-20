using Microsoft.EntityFrameworkCore;
using System;

namespace EFCore.BulkExtensions.Tests.BulkInsertOrUpdate;

public class SimpleBulkTestsContext : DbContext
{
    public DbSet<SimpleItem> Items { get; set; } = null!;

    public SimpleBulkTestsContext(DbContextOptions options)
        : base(options)
    {
    }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
    }
}

public class SimpleItem
{
    public int Id { get; set; }

    public string? Name { get; set; }

    public Guid BulkIdentifier { get; set; }

    public Guid GuidProperty { get; set; }

    public string? StringProperty { get; set; }
}
