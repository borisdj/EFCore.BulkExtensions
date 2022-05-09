using EFCore.BulkExtensions.Tests.IncludeGraph.Model;
using Microsoft.EntityFrameworkCore;
using System.Diagnostics.CodeAnalysis;

namespace EFCore.BulkExtensions.Tests.ShadowProperties;

public class GraphDbContext : DbContext
{
    public GraphDbContext([NotNull] DbContextOptions options) : base(options)
    {
        this.Database.EnsureCreated();
    }

    public DbSet<WorkOrder> WorkOrders { get; set; } = null!;
    public DbSet<WorkOrderSpare> WorkOrderSpares { get; set; } = null!;
    public DbSet<Asset> Assets { get; set; } = null!;
    public DbSet<Spare> Spares { get; set; } = null!;

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        base.OnModelCreating(modelBuilder);

        modelBuilder.Entity<Asset>(cfg =>
        {
            cfg.HasKey(y => y.Id);

            cfg.HasOne(y => y.ParentAsset).WithMany(y => y.ChildAssets);
            cfg.HasMany(y => y.WorkOrders).WithOne(y => y.Asset).IsRequired();
        });

        modelBuilder.Entity<WorkOrder>(cfg =>
        {
            cfg.HasKey(y => y.Id);

            cfg.HasMany(y => y.WorkOrderSpares).WithOne(y => y.WorkOrder);
            cfg.HasOne(y => y.Asset).WithMany(y => y.WorkOrders).IsRequired();
        });

        modelBuilder.Entity<WorkOrderSpare>(cfg =>
        {
            cfg.HasKey(y => y.Id);

            cfg.HasOne(y => y.WorkOrder).WithMany(y => y.WorkOrderSpares).IsRequired();
            cfg.HasOne(y => y.Spare).WithMany().IsRequired();
        });

        modelBuilder.Entity<Spare>(cfg =>
        {
            cfg.HasKey(y => y.Id);
        });

    }
}
