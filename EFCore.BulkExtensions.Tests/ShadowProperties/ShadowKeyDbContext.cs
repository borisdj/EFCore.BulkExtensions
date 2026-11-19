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
            NpgsqlPropertyBuilderExtensions.UseIdentityColumn(cfg.Property<long>(ShadowKeyModel.IdProperty));
            //cfg.Property<long>(ShadowKeyModel.IdProperty).UseIdentityColumn(); was prior but become Ambiguous call
            cfg.HasKey(ShadowKeyModel.IdProperty);
        });
    }
}
