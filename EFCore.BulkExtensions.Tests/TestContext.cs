using System;
using Microsoft.EntityFrameworkCore;

namespace EFCore.BulkExtensions.Tests
{
    public class TestContext : DbContext
    {
        public DbSet<Item> Item { get; set; }

        public TestContext(DbContextOptions options) : base(options)
        {
            Database.EnsureCreated();
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
    }
}
