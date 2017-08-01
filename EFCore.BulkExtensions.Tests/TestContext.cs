using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;

namespace EFCore.BulkExtensions.Tests
{
    public class TestContext : DbContext
    {
        public DbSet<Item> Items { get; set; }
        public DbSet<ItemHistory> ItemHistories { get; set; }

        public TestContext(DbContextOptions options) : base(options)
        {
            Database.EnsureCreated();
        }

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.RemovePluralizingTableNameConvention();
        }
    }

    public static class ContextUtil
    {
        public static DbContextOptions GetOptions()
        {
            var builder = new DbContextOptionsBuilder<TestContext>();
            var databaseName = nameof(EFOperationsTest);
            var connectionString = $"Server=localhost;Database={databaseName};Trusted_Connection=True;MultipleActiveResultSets=true";
            builder.UseSqlServer(connectionString); // Can NOT Test with UseInMemoryDb (Exception: Relational-specific methods can only be used when the context is using a relational)
            return builder.Options;
        }

    }

    public static class ModelBuilderExtensions
    {
        public static void RemovePluralizingTableNameConvention(this ModelBuilder modelBuilder)
        {
            foreach (IMutableEntityType entity in modelBuilder.Model.GetEntityTypes())
            {
                entity.Relational().TableName = entity.ClrType.Name;
            }
        }
    }

    public class Item
    {
        public int ItemId { get; set; }
        
        public string Name { get; set; }

        public string Description { get; set; }

        public int Quantity { get; set; }
        
        public decimal? Price { get; set; }

        public DateTime TimeUpdated { get; set; }

        public ICollection<ItemHistory> ItemHistories { get; set; }
    }

    // ItemHistory is used to test bulk Ops to mutiple tables(Item and ItemHistory), to test Guid as PK and to test other Schema(his)
    [Table(nameof(ItemHistory), Schema = "his")]
    public class ItemHistory
    {
        public Guid ItemHistoryId { get; set; }

        public int ItemId { get; set; }
        public virtual Item Item { get; set; }

        public string Remark { get; set; }
    }
}
