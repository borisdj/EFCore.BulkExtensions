using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;

namespace EFCore.BulkExtensions.Tests
{
    public class TestContext : DbContext
    {
        public DbSet<Item> Items { get; set; }
        public DbSet<ItemHistory> ItemHistories { get; set; }

        public DbSet<UserRole> UserRoles { get; set; }

        public DbSet<Document> Documents { get; set; }
        public DbSet<Person> Persons { get; set; }
        public DbSet<Instructor> Instructors { get; set; }
        public DbSet<Student> Students { get; set; }
        public DbSet<Info> Infos { get; set; }
        public DbSet<ChangeLog> ChangeLogs { get; set; }

        public TestContext(DbContextOptions options) : base(options)
        {
            Database.EnsureCreated();
        }

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.RemovePluralizingTableNameConvention();

            modelBuilder.Entity<UserRole>().HasKey(a => new { a.UserId, a.RoleId });

            modelBuilder.Entity<Info>(e => { e.Property(p => p.ConvertedTime).HasConversion((value) => value.AddDays(1), (value) => value.AddDays(-1)); });

            modelBuilder.Entity<Document>().Property(p => p.ContentLength).HasComputedColumnSql($"(CONVERT([int], len([{nameof(Document.Content)}])))");

            //modelBuilder.Entity<Item>().HasQueryFilter(p => p.Description != "1234"); // For testing Global Filter
        }
    }

    public static class ContextUtil
    {
        public static DbContextOptions GetOptions()
        {
            var builder = new DbContextOptionsBuilder<TestContext>();
            var databaseName = nameof(EFCoreBulkTest);
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
                if (!entity.IsOwned()) // without this exclusion OwnedType would not be by default in Owner Table
                {
                    entity.Relational().TableName = entity.ClrType.Name;
                }
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

    // UserRole is used to test tables with Composite PrimaryKey
    public class UserRole
    {
        [Key]
        public int UserId { get; set; }

        [Key]
        public int RoleId { get; set; }

        public string Description { get; set; }
    }

    // Person, Instructor nad Student are used to test Bulk with Shadow Property and Discriminator column
    public abstract class Person
    {
        public int PersonId { get; set; }

        public string Name { get; set; }
    }

    public class Instructor : Person
    {
        public string Class { get; set; }
    }

    public class Student : Person
    {
        public string Subject { get; set; }
    }

    // For testing Computed Columns
    public class Document
    {
        public int DocumentId { get; set; }

        [Required]
        public string Content { get; set; }

        [Timestamp]
        public byte[] VersionChange { get; set; }

        [DatabaseGenerated(DatabaseGeneratedOption.Computed)] // Computed columns have to be configured with Fluent API
        public int ContentLength { get; set; }
    }

    // For testring ValueConversion
    public class Info
    {
        public int InfoId { get; set; }

        public string Message { get; set; }

        public DateTime ConvertedTime { get; set; }
    }

    // For testing OwnedTypes
    public class ChangeLog
    {
        public int ChangeLogId { get; set; }

        public string Description { get; set; }

        public Audit Audit { get; set; }
    }

    [Owned]
    public class Audit
    {
        //[Column(nameof(ChangedBy))] // for setting custom column name, in this case prefix OwnedType_ ('Audit_') removed, so column would be only ('ChangedBy')
        public string ChangedBy { get; set; } // default Column name for Property of OwnedType is OwnedType_Property ('Audit_ChangedBy')

        public DateTime? ChangedTime { get; set; }
    }
}
