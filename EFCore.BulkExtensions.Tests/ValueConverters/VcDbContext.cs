using Microsoft.EntityFrameworkCore;
using System;
using System.Diagnostics.CodeAnalysis;

namespace EFCore.BulkExtensions.Tests.ValueConverters
{
    public class VcDbContext : DbContext
    {
        public VcDbContext([NotNull] DbContextOptions options) : base(options)
        {
            this.Database.EnsureCreated();
        }

        public DbSet<VcModel> VcModels { get; set; }

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            base.OnModelCreating(modelBuilder);

            modelBuilder.Entity<VcModel>(cfg =>
            {
                cfg.HasKey(y => y.Id);
                cfg.Property(y => y.Id).UseIdentityColumn();

                cfg.Property(y => y.Enum).HasColumnType("nvarchar(4000)").HasConversion<string>();

            });
        }
    }
}
