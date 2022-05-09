using Microsoft.EntityFrameworkCore;
using System;
using System.Diagnostics.CodeAnalysis;

namespace EFCore.BulkExtensions.Tests.ShadowProperties;

public class SpDbContext : DbContext
{
    public SpDbContext([NotNull] DbContextOptions options) : base(options)
    {
        Database.EnsureCreated();
    }

    public DbSet<SpModel> SpModels { get; set; } = null!;

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        base.OnModelCreating(modelBuilder);

        modelBuilder.Entity<SpModel>(cfg =>
        {
            cfg.HasKey(y => y.Id);
            cfg.Property(y => y.Id).UseIdentityColumn();

            // Define the shadow properties
            cfg.Property<long>(SpModel.SpLong);
            cfg.Property<long?>(SpModel.SpNullableLong);

            cfg.Property<DateTime>(SpModel.SpDateTime);
        });
    }
}
