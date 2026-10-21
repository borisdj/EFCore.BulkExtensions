using System.Diagnostics.CodeAnalysis;
using Microsoft.EntityFrameworkCore;

namespace EFCore.BulkExtensions.Tests.ShadowProperties;

internal class ShadowKeyDbContext : TestContextBase
{
    public ShadowKeyDbContext([NotNull] DbContextOptions options) : base(options)
    {
    }

    public DbSet<ShadowKeyModel> ShadowKeyModels { get; set; }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        base.OnModelCreating(modelBuilder);

        modelBuilder.Entity<ShadowKeyModel>(cfg =>
        {
            // Define the shadow PK
            cfg.Property<long>(ShadowKeyModel.IdProperty).UseIdentityColumn();
            cfg.HasKey(ShadowKeyModel.IdProperty);
        });
    }
}
