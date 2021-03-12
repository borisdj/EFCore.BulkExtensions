using EFCore.BulkExtensions.Tests.IncludeGraph.Model;
using Microsoft.EntityFrameworkCore;
using System;
using System.Diagnostics.CodeAnalysis;

namespace EFCore.BulkExtensions.Tests.ShadowProperties
{
    public class GraphDbContext : DbContext
    {
        public GraphDbContext([NotNull] DbContextOptions options) : base(options)
        {
            this.Database.EnsureCreated();
        }

        public DbSet<WorkOrder> WorkOrders { get; set; }
        public DbSet<WorkOrderSpare> WorkOrderSpares { get; set; }
        public DbSet<Asset> Assets { get; set; }
        public DbSet<Spare> Spares { get; set; }

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            base.OnModelCreating(modelBuilder);

            modelBuilder.Entity<Asset>(cfg =>
            {
                cfg.HasKey(y => y.Id);
            });

            modelBuilder.Entity<WorkOrder>(cfg =>
            {
                cfg.HasKey(y => y.Id);

                cfg.HasMany(y => y.WorkOrderSpares).WithOne(y => y.WorkOrder);
                cfg.HasOne(y => y.Asset).WithMany();
            });

            modelBuilder.Entity<WorkOrderSpare>(cfg =>
            {
                cfg.HasKey(y => y.Id);

                cfg.HasOne(y => y.WorkOrder).WithMany(y => y.WorkOrderSpares);
                cfg.HasOne(y => y.Spare);
            });

            modelBuilder.Entity<Spare>(cfg =>
            {
                cfg.HasKey(y => y.Id);
            });

        }
    }
}
